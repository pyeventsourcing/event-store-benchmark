pub mod proto {
    tonic::include_proto!("io.axoniq.axonserver.grpc");

    pub mod event {
        pub mod dcb {
            tonic::include_proto!("io.axoniq.axonserver.grpc.event.dcb");
        }
    }

    // Convenience re-export so downstream code can use `proto::dcb`.
    pub use event::dcb;
}

use anyhow::Result;
use proto::dcb::{
    dcb_event_store_client::DcbEventStoreClient, AppendEventsRequest, Event, GetHeadRequest,
    SourceEventsRequest, SourceEventsResponse, Tag, TaggedEvent,
};
use std::time::Duration;
use tokio_stream::once;
use tonic::transport::{Channel, Endpoint};

/// Minimal Axon Server DCB client.
#[derive(Clone)]
pub struct AxonServerClient {
    inner: DcbEventStoreClient<Channel>,
}

impl AxonServerClient {
    /// Connect to an Axon Server gRPC endpoint (e.g. `http://localhost:8124`).
    pub async fn connect(uri: String) -> Result<Self> {
        let endpoint = Endpoint::from_shared(uri)?
            .tcp_nodelay(true)
            .http2_keep_alive_interval(Duration::from_secs(5))
            .keep_alive_timeout(Duration::from_secs(10))
            .initial_stream_window_size(Some(4 * 1024 * 1024))
            .initial_connection_window_size(Some(8 * 1024 * 1024));

        let channel = endpoint.connect().await?;
        let inner = DcbEventStoreClient::new(channel);
        Ok(Self { inner })
    }

    /// Append a batch of tagged events unconditionally.
    pub async fn append(&mut self, events: Vec<TaggedEvent>) -> Result<i64> {
        let req = AppendEventsRequest {
            condition: None,
            event: events,
        };
        let response = self.inner.append(once(req)).await?.into_inner();
        Ok(response.sequence_of_the_first_event)
    }

    /// Convenience: append a single event with tags derived from string labels.
    pub async fn append_event(
        &mut self,
        name: &str,
        payload: Vec<u8>,
        tags: &[(&str, &str)],
    ) -> Result<i64> {
        let event = Event {
            identifier: uuid_string(),
            timestamp: now_millis(),
            name: name.to_string(),
            version: String::new(),
            payload: payload.into(),
            metadata: Default::default(),
        };
        let proto_tags: Vec<Tag> = tags
            .iter()
            .map(|(k, v)| Tag {
                key: k.as_bytes().to_vec().into(),
                value: v.as_bytes().to_vec().into(),
            })
            .collect();
        let tagged = TaggedEvent {
            event: Some(event),
            tag: proto_tags,
        };
        self.append(vec![tagged]).await
    }

    /// Source (read) events matching criteria from a given sequence.
    pub async fn source(
        &mut self,
        from_sequence: i64,
        criteria: Vec<proto::dcb::Criterion>,
    ) -> Result<Vec<SourceEventsResponse>> {
        let req = SourceEventsRequest {
            from_sequence,
            criterion: criteria,
        };
        let mut stream = self.inner.source(req).await?.into_inner();
        let mut results = Vec::new();
        while let Some(resp) = stream.message().await? {
            results.push(resp);
        }
        Ok(results)
    }

    /// Get the current head sequence of the event store.
    pub async fn get_head(&mut self) -> Result<i64> {
        let resp = self.inner.get_head(GetHeadRequest {}).await?.into_inner();
        Ok(resp.sequence)
    }
}

fn uuid_string() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    format!("{:x}-{:x}", d.as_secs(), d.subsec_nanos())
}

fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
