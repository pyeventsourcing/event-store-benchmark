use tonic::transport::Channel;
use crate::event_store::client::streams::streams_client::StreamsClient;
use crate::event_store::client::streams::{AppendReq, ReadReq, append_req, read_req};
use crate::event_store::generated::common::StreamIdentifier;
use crate::{ClientSettings, EventData, StreamName, AppendToStreamOptions, ReadStreamOptions, WriteResult, ReadStream, StreamPosition, ReadDirection};
use crate::commands::new_request;

/// A minimal KurrentDB client that works with a single node and performs direct gRPC calls.
/// This client does not use a background state machine or MPSC channels for request coordination.
pub struct KurrentDbClient {
    settings: ClientSettings,
    channel: Channel,
}

impl KurrentDbClient {
    /// Creates a new KurrentDbClient from ClientSettings.
    /// It connects to the first host defined in the settings.
    pub async fn new(uri: String) -> crate::Result<Self> {
        let settings: ClientSettings = uri.parse().map_err(|e: crate::ClientSettingsParseError| crate::Error::InitializationError(e.to_string()))?;

        let endpoint = settings.hosts().first()
            .ok_or_else(|| crate::Error::InitializationError("No hosts provided in settings".to_string()))?;

        let uri = settings.to_uri(endpoint);
        let channel = Channel::builder(uri)
            .connect()
            .await
            .map_err(|e| crate::Error::InitializationError(e.to_string()))?;

        Ok(Self {
            settings,
            channel,
        })
    }

    /// Appends events to a stream.
    pub async fn append_to_stream(
        &self,
        stream: impl StreamName,
        options: &AppendToStreamOptions,
        events: impl IntoIterator<Item = EventData>,
    ) -> crate::Result<WriteResult> {
        let mut client = StreamsClient::new(self.channel.clone());

        let stream_identifier = Some(StreamIdentifier {
            stream_name: stream.into_stream_name(),
        });

        let header = AppendReq {
            content: Some(append_req::Content::Options(append_req::Options {
                stream_identifier,
                expected_stream_revision: Some(options.version),
            })),
        };

        let events = events.into_iter().map(|e| e.into()).collect::<Vec<_>>();

        let payload = async_stream::stream! {
            yield header;
            for event in events {
                yield event;
            }
        };

        let req = new_request(&self.settings, options, payload);
        let resp = client.append(req).await
            .map_err(crate::Error::from_grpc)?
            .into_inner();

        match resp.result.unwrap() {
            crate::event_store::client::streams::append_resp::Result::Success(success) => Ok(success.into()),
            crate::event_store::client::streams::append_resp::Result::WrongExpectedVersion(error) => Err(error.into()),
        }
    }

    /// Reads events from a stream.
    pub async fn read_stream(
        &self,
        stream: impl StreamName,
        options: &ReadStreamOptions,
    ) -> crate::Result<ReadStream> {
        let mut client = StreamsClient::new(self.channel.clone());

        let stream_identifier = Some(StreamIdentifier {
            stream_name: stream.into_stream_name(),
        });

        let read_direction = match options.direction {
            ReadDirection::Forward => 0,
            ReadDirection::Backward => 1,
        };

        let revision_option = match options.position {
            StreamPosition::Position(rev) => read_req::options::stream_options::RevisionOption::Revision(rev),
            StreamPosition::Start => read_req::options::stream_options::RevisionOption::Start(()),
            StreamPosition::End => read_req::options::stream_options::RevisionOption::End(()),
        };

        let stream_options = read_req::options::StreamOptions {
            stream_identifier,
            revision_option: Some(revision_option),
        };

        let req_options = read_req::Options {
            stream_option: Some(read_req::options::StreamOption::Stream(stream_options)),
            count_option: Some(read_req::options::CountOption::Count(options.max_count as u64)),
            filter_option: Some(read_req::options::FilterOption::NoFilter(())),
            resolve_links: options.resolve_link_tos,
            read_direction,
            uuid_option: Some(read_req::options::UuidOption {
                content: Some(read_req::options::uuid_option::Content::Structured(())),
            }),
            control_option: None,
        };

        let message = ReadReq {
            options: Some(req_options),
        };

        let req = new_request(&self.settings, options, message);
        let resp = client.read(req).await
            .map_err(crate::Error::from_grpc)?
            .into_inner();

        let (sender, _) = tokio::sync::mpsc::unbounded_channel();
        Ok(ReadStream::new(sender, uuid::Uuid::new_v4(), resp))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EventData;
    use uuid::Uuid;

    #[tokio::test]
    // #[ignore] // Requires a running KurrentDB server
    async fn test_minimal_client_append_and_read() -> crate::Result<()> {
        let uri = "esdb://127.0.0.1:2113?tls=false";
        let client = KurrentDbClient::new(uri.to_string()).await?;

        let stream_name = format!("test-stream-{}", Uuid::new_v4());
        let event_data = EventData::binary("test-event", "test-payload".into());

        // Test Append
        let append_options = AppendToStreamOptions::default();
        let write_result = client.append_to_stream(stream_name.clone(), &append_options, vec![event_data]).await?;
        println!("Write result: {:?}", write_result);

        // Test Read
        let read_options = ReadStreamOptions::default().max_count(1);
        let mut read_stream = client.read_stream(stream_name, &read_options).await?;

        if let Some(event) = read_stream.next().await? {
            let recorded = event.get_original_event();
            assert_eq!(recorded.event_type, "test-event");
            assert_eq!(recorded.data, "test-payload".as_bytes());
        } else {
            panic!("Expected one event to be read");
        }

        Ok(())
    }
}
