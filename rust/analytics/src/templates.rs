use crate::aggregation::{SessionDetail, SessionIndex};
use anyhow::Result;

/// Generate the main index HTML page
pub fn generate_index_html(index: &SessionIndex) -> Result<String> {
    let sessions_json = serde_json::to_string_pretty(&index.sessions)?;

    let html = format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>ES-BENCH - Event Store Benchmark Suite</title>
  <style>
    {styles}
  </style>
</head>
<body>
  <div class="container">
    <header>
      <h1>Event Store Benchmark Suite</h1>
      <p class="subtitle">Performance analytics dashboard</p>
    </header>

    <div class="stats-grid">
      <div class="stat-card">
        <div class="stat-value">{total_sessions}</div>
        <div class="stat-label">Total Sessions</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">{workload_count}</div>
        <div class="stat-label">Workloads</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">{store_count}</div>
        <div class="stat-label">Event Stores</div>
      </div>
    </div>

    <div class="filters">
      <input type="text" id="search" placeholder="Search sessions..." class="search-input">
      <select id="workload-filter" class="filter-select">
        <option value="">All Workloads</option>
        {workload_options}
      </select>
      <select id="store-filter" class="filter-select">
        <option value="">All Stores</option>
        {store_options}
      </select>
    </div>

    <div id="sessions-container"></div>
  </div>

  <script>
    const sessions = {sessions_json};

    {javascript}
  </script>
</body>
</html>"#,
        styles = get_base_styles(),
        total_sessions = index.total_sessions,
        workload_count = index.workloads.len(),
        store_count = index.stores.len(),
        workload_options = index
            .workloads
            .iter()
            .map(|w| format!(r#"<option value="{}">{}</option>"#, w, w))
            .collect::<Vec<_>>()
            .join("\n        "),
        store_options = index
            .stores
            .iter()
            .map(|s| format!(r#"<option value="{}">{}</option>"#, s, s))
            .collect::<Vec<_>>()
            .join("\n        "),
        sessions_json = sessions_json,
        javascript = get_index_javascript(),
    );

    Ok(html)
}

/// Generate HTML for a session detail page
pub fn generate_session_html(detail: &SessionDetail) -> Result<String> {
    let detail_json = serde_json::to_string_pretty(detail)?;

    let html = format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{session_id} - ES-BENCH</title>
  <script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
  <script src="https://cdn.jsdelivr.net/npm/@observablehq/plot@0.6"></script>
  <style>
    {styles}
  </style>
</head>
<body>
  <div class="container">
    <nav class="breadcrumb">
      <a href="../../index.html">← All Sessions</a>
    </nav>

    <header>
      <h1>{workload_name}</h1>
      <p class="subtitle">Session: {session_id}</p>
    </header>

    <div class="metadata-grid">
      <div class="metadata-card">
        <h3>Benchmark Info</h3>
        <dl>
          <dt>Version</dt><dd>{version}</dd>
          <dt>Workload Type</dt><dd>{workload_type}</dd>
          <dt>Seed</dt><dd>{seed}</dd>
        </dl>
      </div>
      <div class="metadata-card">
        <h3>Environment</h3>
        <dl>
          <dt>OS</dt><dd>{os}</dd>
          <dt>CPU</dt><dd>{cpu}</dd>
          <dt>Memory</dt><dd>{memory_gb} GB</dd>
        </dl>
      </div>
    </div>

    <div class="chart-section">
      <h2>Performance Comparison</h2>
      <div id="throughput-chart"></div>
      <div id="latency-chart"></div>
    </div>

    <div class="stores-section">
      <h2>Store Details</h2>
      <div id="stores-container"></div>
    </div>

    <div class="config-section">
      <h2>Configuration</h2>
      <pre><code>{config}</code></pre>
    </div>
  </div>

  <script>
    const sessionData = {detail_json};

    {javascript}
  </script>
</body>
</html>"#,
        session_id = detail.metadata.session_id,
        workload_name = detail.metadata.workload_name,
        version = detail.metadata.benchmark_version,
        workload_type = detail.metadata.workload_type,
        seed = detail.metadata.seed,
        os = format!("{} {}", detail.environment.os, detail.environment.kernel),
        cpu = format!("{} ({} cores)", detail.environment.cpu_model, detail.environment.cpu_cores),
        memory_gb = format!("{:.1}", detail.environment.memory_gb),
        config = html_escape(&detail.config_yaml),
        styles = get_base_styles(),
        detail_json = detail_json,
        javascript = get_session_javascript(),
    );

    Ok(html)
}

/// Get base CSS styles
fn get_base_styles() -> &'static str {
    r#"
* {
  box-sizing: border-box;
  margin: 0;
  padding: 0;
}

body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
  font-size: 16px;
  line-height: 1.6;
  color: #1a1a1a;
  background: #f8f9fa;
  padding: 24px;
}

.container {
  max-width: 1400px;
  margin: 0 auto;
}

header {
  margin-bottom: 32px;
}

h1 {
  font-size: 32px;
  font-weight: 700;
  margin-bottom: 8px;
}

h2 {
  font-size: 24px;
  font-weight: 600;
  margin: 32px 0 16px;
}

h3 {
  font-size: 18px;
  font-weight: 600;
  margin-bottom: 12px;
}

.subtitle {
  font-size: 18px;
  color: #666;
}

.breadcrumb {
  margin-bottom: 16px;
}

.breadcrumb a {
  color: #3b82f6;
  text-decoration: none;
}

.breadcrumb a:hover {
  text-decoration: underline;
}

.stats-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 16px;
  margin-bottom: 32px;
}

.stat-card {
  background: white;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  padding: 24px;
  text-align: center;
}

.stat-value {
  font-size: 48px;
  font-weight: 700;
  color: #3b82f6;
}

.stat-label {
  font-size: 14px;
  color: #666;
  margin-top: 8px;
}

.filters {
  display: flex;
  gap: 12px;
  margin-bottom: 24px;
}

.search-input, .filter-select {
  padding: 10px 16px;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
  font-size: 14px;
}

.search-input {
  flex: 1;
  min-width: 300px;
}

.filter-select {
  min-width: 200px;
}

.session-card {
  background: white;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  padding: 20px;
  margin-bottom: 16px;
  transition: box-shadow 0.2s;
}

.session-card:hover {
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
}

.session-header {
  display: flex;
  justify-content: space-between;
  align-items: start;
  margin-bottom: 12px;
}

.session-title {
  font-size: 20px;
  font-weight: 600;
  color: #3b82f6;
  text-decoration: none;
}

.session-title:hover {
  text-decoration: underline;
}

.session-timestamp {
  color: #666;
  font-size: 14px;
}

.session-meta {
  display: flex;
  gap: 24px;
  flex-wrap: wrap;
  color: #666;
  font-size: 14px;
}

.session-stores {
  display: flex;
  gap: 8px;
  margin-top: 12px;
}

.store-badge {
  padding: 4px 12px;
  background: #e5e7eb;
  border-radius: 4px;
  font-size: 12px;
  font-weight: 500;
}

.metadata-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
  gap: 16px;
  margin-bottom: 32px;
}

.metadata-card {
  background: white;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  padding: 20px;
}

.metadata-card dl {
  display: grid;
  grid-template-columns: 120px 1fr;
  gap: 8px 16px;
}

.metadata-card dt {
  font-weight: 600;
  color: #666;
}

.metadata-card dd {
  color: #1a1a1a;
}

.chart-section {
  margin: 32px 0;
}

#throughput-chart, #latency-chart {
  background: white;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  padding: 24px;
  margin-bottom: 16px;
}

.stores-section {
  margin: 32px 0;
}

.store-detail {
  background: white;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  padding: 24px;
  margin-bottom: 16px;
}

.store-detail h3 {
  color: #3b82f6;
  margin-bottom: 16px;
}

.metrics-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 16px;
  margin-bottom: 24px;
}

.metric {
  text-align: center;
}

.metric-value {
  font-size: 24px;
  font-weight: 700;
  color: #1a1a1a;
}

.metric-label {
  font-size: 12px;
  color: #666;
  margin-top: 4px;
}

.config-section pre {
  background: white;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  padding: 20px;
  overflow-x: auto;
}

.config-section code {
  font-family: 'SF Mono', 'Monaco', 'Courier New', monospace;
  font-size: 13px;
  line-height: 1.5;
}

.chart-container {
  background: white;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  padding: 24px;
  margin-bottom: 16px;
}
"#
}

/// Get JavaScript for index page
fn get_index_javascript() -> &'static str {
    r#"
function renderSessions(filteredSessions) {
  const container = document.getElementById('sessions-container');

  if (filteredSessions.length === 0) {
    container.innerHTML = '<p style="text-align: center; color: #666; padding: 48px;">No sessions found</p>';
    return;
  }

  container.innerHTML = filteredSessions.map(session => `
    <div class="session-card">
      <div class="session-header">
        <a href="sessions/${session.session_id}/index.html" class="session-title">
          ${session.workload_name}
        </a>
        <span class="session-timestamp">${session.timestamp}</span>
      </div>
      <div class="session-meta">
        <span><strong>Type:</strong> ${session.workload_type}</span>
        <span><strong>Version:</strong> ${session.benchmark_version}</span>
        <span><strong>Events:</strong> ${session.total_events.toLocaleString()}</span>
        <span><strong>Duration:</strong> ${session.duration_s.toFixed(1)}s</span>
      </div>
      <div class="session-stores">
        ${session.stores_run.map(store => `<span class="store-badge">${store}</span>`).join('')}
      </div>
    </div>
  `).join('');
}

function filterSessions() {
  const searchTerm = document.getElementById('search').value.toLowerCase();
  const workloadFilter = document.getElementById('workload-filter').value;
  const storeFilter = document.getElementById('store-filter').value;

  const filtered = sessions.filter(session => {
    const matchesSearch = session.workload_name.toLowerCase().includes(searchTerm) ||
                          session.session_id.toLowerCase().includes(searchTerm);
    const matchesWorkload = !workloadFilter || session.workload_name === workloadFilter;
    const matchesStore = !storeFilter || session.stores_run.includes(storeFilter);

    return matchesSearch && matchesWorkload && matchesStore;
  });

  renderSessions(filtered);
}

document.getElementById('search').addEventListener('input', filterSessions);
document.getElementById('workload-filter').addEventListener('change', filterSessions);
document.getElementById('store-filter').addEventListener('change', filterSessions);

// Initial render
renderSessions(sessions);
"#
}

/// Get JavaScript for session detail page
fn get_session_javascript() -> &'static str {
    r##"
// Render throughput comparison chart
function renderThroughputChart() {
  const data = sessionData.stores.map(store => ({
    store: store.name,
    throughput: store.throughput_eps
  }));

  const chart = Plot.plot({
    marginLeft: 60,
    marginBottom: 60,
    height: 300,
    x: {label: "Event Store"},
    y: {label: "Throughput (events/sec)", grid: true},
    marks: [
      Plot.barY(data, {x: "store", y: "throughput", fill: "#3b82f6"}),
      Plot.ruleY([0])
    ]
  });

  document.getElementById('throughput-chart').appendChild(chart);
}

// Render latency comparison chart
function renderLatencyChart() {
  const container = document.getElementById('latency-chart');

  // Create a simple table-based visualization
  let html = '<div style="overflow-x: auto;"><table style="width: 100%; border-collapse: collapse;">';
  html += '<thead><tr><th style="text-align: left; padding: 12px; border-bottom: 2px solid #e5e7eb;">Store</th>';
  html += '<th style="text-align: right; padding: 12px; border-bottom: 2px solid #e5e7eb;">p50 (ms)</th>';
  html += '<th style="text-align: right; padding: 12px; border-bottom: 2px solid #e5e7eb;">p95 (ms)</th>';
  html += '<th style="text-align: right; padding: 12px; border-bottom: 2px solid #e5e7eb;">p99 (ms)</th>';
  html += '<th style="text-align: right; padding: 12px; border-bottom: 2px solid #e5e7eb;">p999 (ms)</th></tr></thead>';
  html += '<tbody>';

  sessionData.stores.forEach((store, idx) => {
    const bgColor = idx % 2 === 0 ? '#ffffff' : '#f9fafb';
    html += `<tr style="background: ${bgColor};">`;
    html += `<td style="padding: 12px; font-weight: 600;">${store.name}</td>`;
    html += `<td style="padding: 12px; text-align: right;">${store.latency_p50_ms.toFixed(3)}</td>`;
    html += `<td style="padding: 12px; text-align: right;">${store.latency_p95_ms.toFixed(3)}</td>`;
    html += `<td style="padding: 12px; text-align: right;">${store.latency_p99_ms.toFixed(3)}</td>`;
    html += `<td style="padding: 12px; text-align: right;">${store.latency_p999_ms.toFixed(3)}</td>`;
    html += '</tr>';
  });

  html += '</tbody></table></div>';
  container.innerHTML = html;
}

// Render store details
function renderStores() {
  const container = document.getElementById('stores-container');

  container.innerHTML = sessionData.stores.map(store => `
    <div class="store-detail">
      <h3>${store.name}</h3>
      <div class="metrics-grid">
        <div class="metric">
          <div class="metric-value">${store.throughput_eps.toFixed(0)}</div>
          <div class="metric-label">Events/sec</div>
        </div>
        <div class="metric">
          <div class="metric-value">${store.latency_p50_ms.toFixed(2)}</div>
          <div class="metric-label">p50 Latency (ms)</div>
        </div>
        <div class="metric">
          <div class="metric-value">${store.latency_p99_ms.toFixed(2)}</div>
          <div class="metric-label">p99 Latency (ms)</div>
        </div>
        <div class="metric">
          <div class="metric-value">${store.events_written.toLocaleString()}</div>
          <div class="metric-label">Events Written</div>
        </div>
      </div>
    </div>
  `).join('');
}

// Initialize
renderThroughputChart();
renderLatencyChart();
renderStores();
"##
}

/// HTML-escape a string
fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
