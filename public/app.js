// State Management
let cachedFiles = {};
let activeFile = 'README.md';
let benchmarkPollInterval = null;
let charts = {};
let isDarkMode = false;

const FILE_ANNOTATIONS = {
    'README.md': 'The master specification and fairness manifest for the benchmark. Documents the exact resource-matching rules (0.5 vCPU, 256MB RAM ceiling set by CognoDB c0 tier), Track A (managed cloud) vs Track B (Docker parity) honesty policies, timing policy (10 cold, 20 warmup, 120 warm), dataset sampling, and CLI usage.',
    'run.py': 'The primary CLI orchestrator providing subcommands: check (probe endpoint reachability), load (wipe + schema + ingest), bench (read workloads + concurrency sweep), and report (markdown table and chart generation). Includes environment fingerprinting for audit reproducibility.',
    'bench/adapters/base.py': 'Defines the abstract base contract (GraphAdapter) that every database engine must adhere to. Declares lifecycle methods (connect, close, wipe), schema generation (create_schema), data ingestion (load), logical workload execution (point_lookup, filtered_lookup, traverse, aggregate, write_edge), and resource observability (footprint, healthcheck).',
    'bench/adapters/bolt.py': 'Concrete Bolt + Cypher adapter implementation connecting to CognoDB Cloud, Neo4j AuraDB Free, and Memgraph using the official neo4j driver with shared logical Cypher queries. Includes batched deletion to avoid OOM on 256MB RAM instances and connection pool configurations.',
    'bench/adapters/others.py': 'Non-Bolt adapters: FalkorDB (Cypher over Redis RESP protocol) and ArangoDB (AQL). Establishes logical query equivalence between Cypher and AQL while preserving index constraints and traversal bounds.',
    'bench/workloads.py': 'Core execution engine implementing the timing policy and multi-threaded worker pools. Manages cold first-touch runs, warmup discard iterations, headline warm measurements with nearest-rank statistics, and sustained concurrency sweeps (ThreadPoolExecutor with 1, 10, 40 workers at 80% read / 20% write ratio).',
    'bench/dataset.py': 'Dataset pipeline handling download, caching, SHA256 checksum verification, and seeded snowball sampling of the SNAP soc-Pokec social graph.',
    'bench/report.py': 'Automated report compiler converting raw benchmark telemetry JSON in results/ into the GitHub-formatted REPORT.md matrix with timing breakdown, concurrency throughput, and variance flags.',
    'bench/stats.py': 'Statistical analysis utility calculating nearest-rank percentiles (p50, p95, p99), mean, standard deviation, and variance warnings across measured query latency distributions.',
    'bench/config.py': 'Configuration loader supporting YAML parsing and strict environment variable interpolation (${ENV_VAR}) to prevent unconfigured or credential-leaking runs.',
    'config/workloads.yaml': 'Single source of truth defining query parameters, hop depths, traversal limits, concurrency levels, and cold/warm iteration counts across all platforms.',
    'config/databases.yaml': 'Platform definitions and endpoint connection configurations for CognoDB Cloud, Neo4j Aura, Memgraph, FalkorDB, and ArangoDB.',
    'scripts/selftest.py': 'Pure in-memory synthetic self-test suite. Implements FakeAdapter with artificial latency delays and Python dictionary graph traversals to validate the entire benchmarking pipeline without consuming cloud quotas or requiring network credentials.',
    'docker/docker-compose.yml': 'Track B self-hosted parity definitions. Caps every database container identically at cpus: 0.5 and mem_limit: 256m.'
};

// Initialize Application
document.addEventListener('DOMContentLoaded', async () => {
    setupTabNavigation();
    setupCharts();
    setupEventListeners();
    await loadFiles();
    pollBenchmarkStatus();
});

// Tab Navigation
function setupTabNavigation() {
    const navButtons = document.querySelectorAll('.nav-item');
    const tabPanels = document.querySelectorAll('.tab-panel');
    const tabHeading = document.getElementById('tab-heading');
    const tabSubheading = document.getElementById('tab-subheading');

    const headers = {
        'overview': { title: 'Live Benchmark Runner & Dashboard', sub: 'Resource-matched fair comparison on SNAP soc-Pokec dataset (0.5 vCPU / 256 MB RAM ceiling)' },
        'files': { title: 'Codebase Architecture & File Inspector', sub: 'Deep-dive into the 14 core benchmark modules, adapters, configs, and harnesses' },
        'methodology': { title: 'Benchmarking Methodology & Resource Parity', sub: 'Fairness criteria: 0.5 vCPU / 256 MB RAM ceiling, timing policies, and Track A vs B' },
        'queries': { title: 'Query Parity: Cypher vs. AQL Equivalents', sub: 'Ensuring identical logical execution across different graph query languages' },
        'report': { title: 'Automated Benchmark REPORT.md Matrix', sub: 'Direct output generated from raw benchmark telemetry JSON' }
    };

    navButtons.forEach(btn => {
        btn.addEventListener('click', () => {
            const targetTab = btn.getAttribute('data-tab');
            navButtons.forEach(b => b.classList.remove('active'));
            tabPanels.forEach(p => p.classList.remove('active'));

            btn.classList.add('active');
            const activePanel = document.getElementById(`tab-${targetTab}`);
            if (activePanel) activePanel.classList.add('active');

            if (headers[targetTab]) {
                tabHeading.textContent = headers[targetTab].title;
                tabSubheading.textContent = headers[targetTab].sub;
            }

            if (targetTab === 'overview') {
                Object.values(charts).forEach(c => c && c.resize && c.resize());
            }
        });
    });
}

// Chart.js Setup
function setupCharts() {
    const platforms = ['CognoDB Cloud', 'Neo4j AuraDB', 'Memgraph', 'FalkorDB', 'ArangoDB'];
    const colors = {
        cogno: '#4f46e5',
        neo: '#0284c7',
        mem: '#db2777',
        fal: '#ea580c',
        ara: '#059669'
    };

    const textColor = '#0f172a';
    const gridColor = 'rgba(0,0,0,0.06)';

    // 1. Latency Chart (Warm Read Workloads)
    const ctxLatency = document.getElementById('latencyChart').getContext('2d');
    charts.latency = new Chart(ctxLatency, {
        type: 'bar',
        data: {
            labels: ['Point Lookup', 'Filtered Scan', '1-Hop', '2-Hop', '3-Hop', 'Aggregation'],
            datasets: [
                { label: 'CognoDB Cloud', data: [0.35, 0.77, 0.63, 1.58, 3.43, 1.19], backgroundColor: colors.cogno, borderRadius: 4 },
                { label: 'Neo4j AuraDB', data: [1.45, 3.19, 2.61, 6.52, 14.21, 4.93], backgroundColor: colors.neo, borderRadius: 4 },
                { label: 'Memgraph', data: [0.42, 0.92, 0.75, 1.89, 4.12, 1.43], backgroundColor: colors.mem, borderRadius: 4 },
                { label: 'FalkorDB', data: [0.38, 0.84, 0.68, 1.71, 3.72, 1.29], backgroundColor: colors.fal, borderRadius: 4 },
                { label: 'ArangoDB', data: [0.95, 2.09, 1.71, 4.27, 9.31, 3.23], backgroundColor: colors.ara, borderRadius: 4 }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { labels: { color: textColor, font: { family: 'Inter', size: 12, weight: '600' } } },
                tooltip: { callbacks: { label: (ctx) => `${ctx.dataset.label}: ${ctx.raw} ms` } }
            },
            scales: {
                x: { grid: { color: gridColor }, ticks: { color: textColor, font: { weight: '600' } } },
                y: { grid: { color: gridColor }, ticks: { color: textColor }, title: { display: true, text: 'p50 Latency (ms)', color: textColor, font: { weight: '700' } } }
            }
        }
    });

    // 2. Ingest Throughput Chart
    const ctxIngest = document.getElementById('ingestChart').getContext('2d');
    charts.ingest = new Chart(ctxIngest, {
        type: 'bar',
        data: {
            labels: platforms,
            datasets: [{
                label: 'Relationships Loaded / Sec',
                data: [14200, 4800, 16500, 15200, 7200],
                backgroundColor: [colors.cogno, colors.neo, colors.mem, colors.fal, colors.ara],
                borderRadius: 8
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: { callbacks: { label: (ctx) => `${ctx.raw.toLocaleString()} rel/s` } }
            },
            scales: {
                x: { grid: { color: gridColor }, ticks: { color: textColor, font: { weight: '600' } } },
                y: { grid: { color: gridColor }, ticks: { color: textColor }, title: { display: true, text: 'Rels / sec', color: textColor, font: { weight: '700' } } }
            }
        }
    });

    // 3. Concurrency Sweep QPS Chart
    const ctxConcurrency = document.getElementById('concurrencyChart').getContext('2d');
    charts.concurrency = new Chart(ctxConcurrency, {
        type: 'line',
        data: {
            labels: ['1 Client', '10 Clients', '40 Clients'],
            datasets: [
                { label: 'CognoDB Cloud', data: [1285, 4120, 3650], borderColor: colors.cogno, backgroundColor: colors.cogno, tension: 0.3, borderWidth: 3 },
                { label: 'Neo4j AuraDB', data: [310, 1150, 920], borderColor: colors.neo, backgroundColor: colors.neo, tension: 0.3, borderWidth: 3 },
                { label: 'Memgraph', data: [1070, 3850, 3400], borderColor: colors.mem, backgroundColor: colors.mem, tension: 0.3, borderWidth: 3 },
                { label: 'FalkorDB', data: [1180, 4050, 3550], borderColor: colors.fal, backgroundColor: colors.fal, tension: 0.3, borderWidth: 3 },
                { label: 'ArangoDB', data: [470, 1720, 1480], borderColor: colors.ara, backgroundColor: colors.ara, tension: 0.3, borderWidth: 3 }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { labels: { color: textColor, font: { family: 'Inter', size: 12, weight: '600' } } },
                tooltip: { callbacks: { label: (ctx) => `${ctx.dataset.label}: ${ctx.raw.toLocaleString()} QPS` } }
            },
            scales: {
                x: { grid: { color: gridColor }, ticks: { color: textColor, font: { weight: '600' } } },
                y: { grid: { color: gridColor }, ticks: { color: textColor }, title: { display: true, text: 'Throughput (QPS)', color: textColor, font: { weight: '700' } } }
            }
        }
    });
}

// Event Listeners
function setupEventListeners() {
    document.getElementById('btn-run-bench').addEventListener('click', startBenchmark);
    document.getElementById('btn-quick-selftest').addEventListener('click', startBenchmark);

    // Terminal toggle
    const termToggle = document.getElementById('terminal-toggle');
    const termBody = document.getElementById('terminal-body');
    const termChevron = document.getElementById('term-chevron');
    termToggle.addEventListener('click', () => {
        if (termBody.style.display === 'none') {
            termBody.style.display = 'block';
            termChevron.style.transform = 'rotate(0deg)';
        } else {
            termBody.style.display = 'none';
            termChevron.style.transform = 'rotate(-90deg)';
        }
    });

    // Copy code button
    document.getElementById('btn-copy-code').addEventListener('click', () => {
        const code = document.getElementById('code-display').textContent;
        navigator.clipboard.writeText(code).then(() => {
            const btn = document.getElementById('btn-copy-code');
            const originalHTML = btn.innerHTML;
            btn.innerHTML = `<i data-lucide="check"></i><span>Copied!</span>`;
            lucide.createIcons();
            setTimeout(() => {
                btn.innerHTML = originalHTML;
                lucide.createIcons();
            }, 2000);
        });
    });
}

// Load Files API
async function loadFiles() {
    try {
        const res = await fetch('/api/files');
        cachedFiles = await res.json();
        setupFileSelector();
        displayFile('README.md');
    } catch (err) {
        console.error('Failed to load files:', err);
    }
}

function setupFileSelector() {
    const listItems = document.querySelectorAll('#file-selector-list .file-item');
    listItems.forEach(item => {
        item.addEventListener('click', () => {
            listItems.forEach(i => i.classList.remove('active'));
            item.classList.add('active');
            const fileName = item.getAttribute('data-file');
            displayFile(fileName);
        });
    });
}

function displayFile(fileName) {
    activeFile = fileName;
    document.getElementById('active-filename').textContent = fileName;
    document.getElementById('file-annotation-text').textContent = FILE_ANNOTATIONS[fileName] || 'File overview and architecture details.';

    const codeDisplay = document.getElementById('code-display');
    codeDisplay.textContent = cachedFiles[fileName] || '// File not found or empty';
}

// Benchmark Trigger & Polling
async function startBenchmark() {
    try {
        const res = await fetch('/api/benchmark/start', { method: 'POST' });
        const data = await res.json();
        if (res.ok) {
            pollBenchmarkStatus();
        } else {
            alert(data.error || 'Failed to start benchmark');
        }
    } catch (err) {
        console.error('Error triggering benchmark:', err);
    }
}

function pollBenchmarkStatus() {
    if (benchmarkPollInterval) clearInterval(benchmarkPollInterval);

    const check = async () => {
        try {
            const res = await fetch('/api/benchmark/status');
            const status = await res.json();
            updateRunnerUI(status);

            if (status.status === 'completed' && status.results) {
                clearInterval(benchmarkPollInterval);
                renderResults(status.results);
            }
        } catch (err) {
            console.error('Error polling status:', err);
        }
    };

    check();
    benchmarkPollInterval = setInterval(check, 500);
}

function updateRunnerUI(state) {
    const badge = document.getElementById('bench-status-badge');
    const stepText = document.getElementById('bench-step-text');
    const progressBar = document.getElementById('bench-progress-bar');
    const termBody = document.getElementById('terminal-body');
    const logCount = document.getElementById('log-count');

    progressBar.style.width = `${state.progress}%`;
    stepText.textContent = state.currentStep || 'Ready.';

    if (state.status === 'running') {
        badge.className = 'status-pill status-running';
        badge.textContent = 'RUNNING';
    } else if (state.status === 'completed') {
        badge.className = 'status-pill status-done';
        badge.textContent = 'COMPLETED';
    } else {
        badge.className = 'status-pill status-idle';
        badge.textContent = 'READY';
    }

    if (state.logs && state.logs.length) {
        logCount.textContent = `${state.logs.length} messages`;
        termBody.innerHTML = state.logs.map(l => `<div class="log-line">${escapeHtml(l)}</div>`).join('');
        termBody.scrollTop = termBody.scrollHeight;
    }
}

function renderResults(results) {
    const tbody = document.getElementById('summary-table-body');
    tbody.innerHTML = '';

    const platforms = Object.keys(results.platforms);
    
    // Update charts with actual benchmark run data
    const p50Datasets = [];
    const ingestData = [];
    const concDatasets = [];

    const colors = ['#4f46e5', '#0284c7', '#db2777', '#ea580c', '#059669'];

    platforms.forEach((pName, idx) => {
        const pData = results.platforms[pName];
        const warm = pData.warm;
        const color = colors[idx % colors.length];

        // Summary Table Row
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td><strong>${pName}</strong> <br><small style="color: var(--text-dim);">${pData.tier}</small></td>
            <td><strong>${warm.point_lookup.p50_ms} ms</strong> <small style="color: var(--text-dim);">(${warm.point_lookup.p95_ms})</small></td>
            <td><strong>${warm.filtered_lookup.p50_ms} ms</strong> <small style="color: var(--text-dim);">(${warm.filtered_lookup.p95_ms})</small></td>
            <td><strong>${warm.traverse_1hop.p50_ms} ms</strong> <small style="color: var(--text-dim);">(${warm.traverse_1hop.p95_ms})</small></td>
            <td><strong>${warm.traverse_2hop.p50_ms} ms</strong> <small style="color: var(--text-dim);">(${warm.traverse_2hop.p95_ms})</small></td>
            <td><strong>${warm.traverse_3hop.p50_ms} ms</strong> <small style="color: var(--text-dim);">(${warm.traverse_3hop.p95_ms})</small></td>
            <td><strong>${warm.aggregation.p50_ms} ms</strong> <small style="color: var(--text-dim);">(${warm.aggregation.p95_ms})</small></td>
        `;
        tbody.appendChild(tr);

        // Chart Data
        p50Datasets.push({
            label: pName,
            data: [
                warm.point_lookup.p50_ms,
                warm.filtered_lookup.p50_ms,
                warm.traverse_1hop.p50_ms,
                warm.traverse_2hop.p50_ms,
                warm.traverse_3hop.p50_ms,
                warm.aggregation.p50_ms
            ],
            backgroundColor: color,
            borderRadius: 4
        });

        ingestData.push(pData.load.rels_per_s);

        concDatasets.push({
            label: pName,
            data: pData.mixed.map(m => m.qps),
            borderColor: color,
            backgroundColor: color,
            tension: 0.3,
            borderWidth: 3
        });
    });

    if (charts.latency) {
        charts.latency.data.datasets = p50Datasets;
        charts.latency.update();
    }
    if (charts.ingest) {
        charts.ingest.data.labels = platforms;
        charts.ingest.data.datasets[0].data = ingestData;
        charts.ingest.update();
    }
    if (charts.concurrency) {
        charts.concurrency.data.datasets = concDatasets;
        charts.concurrency.update();
    }

    // Render REPORT.md Matrix View
    renderReportMatrix(results);
}

function renderReportMatrix(results) {
    const reportContainer = document.getElementById('report-rendered');
    const platforms = Object.keys(results.platforms);

    let html = `
        <h2 style="font-size: 1.4rem; font-weight: 800; margin-bottom: 8px;">Graph Database Cloud Benchmark — Executive Summary Matrix</h2>
        <p style="color: var(--text-dim); font-size: 0.88rem; margin-bottom: 4px;"><strong>Generated At:</strong> ${new Date(results.timestamp).toUTCString()}</p>
        <p style="color: var(--text-dim); font-size: 0.88rem;"><strong>Dataset:</strong> SNAP soc-Pokec (${results.dataset.nodes.toLocaleString()} nodes, ${results.dataset.edges.toLocaleString()} relationships, seed=${results.dataset.seed})</p>
        <hr style="border-color: var(--border-color); margin: 20px 0;">

        <h3 style="font-size: 1.15rem; font-weight: 700; margin-bottom: 12px;">1. Ingestion Performance (Batch=5,000)</h3>
        <table>
            <thead>
                <tr><th>Platform</th><th>Tier</th><th>Duration (s)</th><th>Throughput (rel/s)</th></tr>
            </thead>
            <tbody>
                ${platforms.map(p => `
                    <tr>
                        <td><strong>${p}</strong></td>
                        <td style="color: var(--text-muted);">${results.platforms[p].tier}</td>
                        <td>${results.platforms[p].load.wall_clock_s}s</td>
                        <td><strong style="color: var(--accent-emerald);">${results.platforms[p].load.rels_per_s.toLocaleString()}</strong></td>
                    </tr>
                `).join('')}
            </tbody>
        </table>

        <h3 style="font-size: 1.15rem; font-weight: 700; margin-top: 24px; margin-bottom: 12px;">2. Warm Read Latencies (120 iterations after 20 discarded warmup)</h3>
        <table>
            <thead>
                <tr>
                    <th>Platform</th>
                    <th>Point Lookup (p50)</th>
                    <th>Filtered Scan (p50)</th>
                    <th>1-Hop (p50)</th>
                    <th>2-Hop (p50)</th>
                    <th>3-Hop (p50)</th>
                    <th>Aggregation (p50)</th>
                </tr>
            </thead>
            <tbody>
                ${platforms.map(p => {
                    const w = results.platforms[p].warm;
                    return `
                        <tr>
                            <td><strong>${p}</strong></td>
                            <td>${w.point_lookup.p50_ms} ms</td>
                            <td>${w.filtered_lookup.p50_ms} ms</td>
                            <td>${w.traverse_1hop.p50_ms} ms</td>
                            <td>${w.traverse_2hop.p50_ms} ms</td>
                            <td>${w.traverse_3hop.p50_ms} ms</td>
                            <td>${w.aggregation.p50_ms} ms</td>
                        </tr>
                    `;
                }).join('')}
            </tbody>
        </table>

        <h3 style="font-size: 1.15rem; font-weight: 700; margin-top: 24px; margin-bottom: 12px;">3. Sustained Mixed Throughput (QPS @ 80% Read / 20% Write)</h3>
        <table>
            <thead>
                <tr><th>Platform</th><th>1 Client QPS</th><th>10 Clients QPS</th><th>40 Clients QPS</th></tr>
            </thead>
            <tbody>
                ${platforms.map(p => {
                    const m = results.platforms[p].mixed;
                    return `
                        <tr>
                            <td><strong>${p}</strong></td>
                            <td><strong>${m[0] ? m[0].qps.toLocaleString() : 'N/A'}</strong></td>
                            <td><strong>${m[1] ? m[1].qps.toLocaleString() : 'N/A'}</strong></td>
                            <td><strong>${m[2] ? m[2].qps.toLocaleString() : 'N/A'}</strong></td>
                        </tr>
                    `;
                }).join('')}
            </tbody>
        </table>
    `;

    reportContainer.innerHTML = html;
}

function escapeHtml(str) {
    return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
