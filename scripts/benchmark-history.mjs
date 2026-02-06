#!/usr/bin/env node
/**
 * Fetch benchmark results from recent CI runs on main and generate
 * an interactive HTML report showing throughput over time.
 *
 * Usage:
 *   node scripts/benchmark-history.mjs [--days <n>]
 *
 * Requires: gh CLI authenticated with access to gadget-inc/silo
 */

import { execFileSync } from "node:child_process";
import { writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseArgs } from "node:util";

const { values: args } = parseArgs({
  options: {
    days: { type: "string", short: "d", default: "30" },
    help: { type: "boolean", short: "h", default: false },
  },
  strict: true,
});

if (args.help) {
  console.log(`
Usage: node scripts/benchmark-history.mjs [options]

Fetch benchmark results from recent main branch CI runs and open
an interactive HTML report.

Options:
  -d, --days <n>   Number of days of history to fetch (default: 30)
  -h, --help       Show this help message
`);
  process.exit(0);
}

const days = parseInt(args.days, 10);
if (Number.isNaN(days) || days < 1) {
  console.error("Error: --days must be a positive integer");
  process.exit(1);
}

const REPO = "gadget-inc/silo";
const WORKFLOW = "ci.yml";
const BENCHMARK_JOB_NAME = "Run benchmarks";

function gh(apiArgs) {
  const result = execFileSync("gh", apiArgs, {
    encoding: "utf-8",
    maxBuffer: 10 * 1024 * 1024,
  });
  return result;
}

function ghJson(apiArgs) {
  return JSON.parse(gh(apiArgs));
}

// Step 1: List workflow runs on main within date range
const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000)
  .toISOString()
  .split("T")[0];

console.log(`Fetching CI runs on main since ${since} (last ${days} days)...`);

const runs = ghJson([
  "run",
  "list",
  "--repo",
  REPO,
  "--branch",
  "main",
  "--workflow",
  WORKFLOW,
  "--limit",
  "200",
  "--json",
  "databaseId,headSha,createdAt,conclusion,displayTitle",
]);

// Filter to successful runs within date range
const filteredRuns = runs.filter(
  (r) => r.conclusion === "success" && r.createdAt >= since,
);

console.log(
  `Found ${filteredRuns.length} successful runs (out of ${runs.length} total).`,
);

if (filteredRuns.length === 0) {
  console.error("No successful benchmark runs found in the given date range.");
  process.exit(1);
}

// Step 2: For each run, get the benchmark job and parse its logs
const benchmarkPattern =
  /(\d+)\s+producer[s]?\s+x\s+(\d+)\s+jobs:\s+([\d.]+)\s+jobs\/sec/;
const dequeuePattern =
  /(\d+)\s+consumer[s]?\s+x\s+(\d+)\s+jobs\s+\(batch\s+(\d+)\):\s+([\d.]+)\s+jobs\/sec/;

const results = [];

// Process runs from oldest to newest for chronological ordering
const sortedRuns = filteredRuns.sort(
  (a, b) => new Date(a.createdAt) - new Date(b.createdAt),
);

for (let i = 0; i < sortedRuns.length; i++) {
  const run = sortedRuns[i];
  const shortSha = run.headSha.slice(0, 7);
  process.stdout.write(
    `\r  Fetching benchmark ${i + 1}/${sortedRuns.length} (${shortSha})...`,
  );

  try {
    // Get benchmark job ID
    const jobs = ghJson([
      "run",
      "view",
      String(run.databaseId),
      "--repo",
      REPO,
      "--json",
      "jobs",
    ]);

    const benchJob = jobs.jobs.find((j) => j.name === BENCHMARK_JOB_NAME);
    if (!benchJob || benchJob.conclusion !== "success") continue;

    // Fetch job logs
    const logs = gh([
      "api",
      `repos/${REPO}/actions/jobs/${benchJob.databaseId}/logs`,
    ]);

    // Parse enqueue results
    const enqueue = {};
    for (const line of logs.split("\n")) {
      const m = line.match(benchmarkPattern);
      if (m) {
        const producers = parseInt(m[1], 10);
        const rate = parseFloat(m[3]);
        enqueue[`enqueue_${producers}p`] = rate;
      }
    }

    // Parse dequeue results
    const dequeue = {};
    for (const line of logs.split("\n")) {
      const m = line.match(dequeuePattern);
      if (m) {
        const consumers = parseInt(m[1], 10);
        const rate = parseFloat(m[4]);
        dequeue[`dequeue_${consumers}c`] = rate;
      }
    }

    if (Object.keys(enqueue).length === 0 && Object.keys(dequeue).length === 0)
      continue;

    results.push({
      sha: shortSha,
      date: run.createdAt,
      title: run.displayTitle,
      runId: run.databaseId,
      ...enqueue,
      ...dequeue,
    });
  } catch {
    // Skip runs where we can't fetch logs
  }
}

console.log(
  `\nParsed ${results.length} benchmark results. Generating report...`,
);

if (results.length === 0) {
  console.error("No benchmark data could be parsed from the CI logs.");
  process.exit(1);
}

// Step 3: Generate HTML report
const metrics = [
  { key: "enqueue_1p", label: "Enqueue 1 producer", color: "#2563eb" },
  { key: "enqueue_4p", label: "Enqueue 4 producers", color: "#7c3aed" },
  { key: "enqueue_8p", label: "Enqueue 8 producers", color: "#db2777" },
  { key: "dequeue_1c", label: "Dequeue 1 consumer", color: "#059669" },
  { key: "dequeue_4c", label: "Dequeue 4 consumers", color: "#d97706" },
  { key: "dequeue_8c", label: "Dequeue 8 consumers", color: "#dc2626" },
];

// Build table header and rows as plain strings to avoid nested template literals
const headerCells = metrics.map((m) => "<th>" + m.label + "</th>").join("\n          ");

const reversed = results.slice().reverse();
const tableRows = reversed
  .map((r, i) => {
    const prev = reversed[i + 1];
    const dateStr = new Date(r.date).toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
    const safeTitle = r.title.replace(/"/g, "&quot;");
    const valueCells = metrics
      .map((m) => {
        const val = r[m.key];
        if (val == null) return "<td>-</td>";
        let delta = "";
        if (prev && prev[m.key] != null) {
          const pct = ((val - prev[m.key]) / prev[m.key]) * 100;
          if (Math.abs(pct) >= 1) {
            const cls = pct > 0 ? "positive" : "negative";
            const sign = pct > 0 ? "+" : "";
            delta =
              '<span class="delta ' +
              cls +
              '">' +
              sign +
              pct.toFixed(1) +
              "%</span>";
          }
        }
        return "<td>" + Math.round(val).toLocaleString() + delta + "</td>";
      })
      .join("\n          ");

    return (
      "<tr>\n" +
      '          <td>' + dateStr + '</td>\n' +
      '          <td><a href="https://github.com/' + REPO + '/commit/' + r.sha + '" title="' + safeTitle + '">' + r.sha + '</a></td>\n' +
      "          " + valueCells + "\n" +
      "        </tr>"
    );
  })
  .join("\n        ");

const enqueueColors = JSON.stringify(metrics.slice(0, 3).map((m) => m.color));
const enqueueLabels = JSON.stringify(metrics.slice(0, 3).map((m) => m.label));
const dequeueColors = JSON.stringify(metrics.slice(3).map((m) => m.color));
const dequeueLabels = JSON.stringify(metrics.slice(3).map((m) => m.label));
const dataJson = JSON.stringify(results);

const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Silo Benchmark History</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #f8fafc; color: #1e293b; padding: 24px; }
    h1 { font-size: 1.5rem; margin-bottom: 4px; }
    .subtitle { color: #64748b; margin-bottom: 24px; font-size: 0.9rem; }
    .chart-container { background: #fff; border-radius: 12px; padding: 24px; margin-bottom: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    .chart-container h2 { font-size: 1.1rem; margin-bottom: 16px; }
    .chart-wrap { position: relative; height: 400px; }
    table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
    th, td { padding: 8px 12px; text-align: right; border-bottom: 1px solid #e2e8f0; }
    th { background: #f1f5f9; font-weight: 600; position: sticky; top: 0; }
    td:first-child, th:first-child { text-align: left; }
    td:nth-child(2), th:nth-child(2) { text-align: left; }
    tr:hover td { background: #f8fafc; }
    .table-container { background: #fff; border-radius: 12px; padding: 24px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); overflow-x: auto; }
    .table-container h2 { font-size: 1.1rem; margin-bottom: 16px; }
    .delta { font-size: 0.75rem; margin-left: 4px; }
    .delta.positive { color: #059669; }
    .delta.negative { color: #dc2626; }
    a { color: #2563eb; text-decoration: none; }
    a:hover { text-decoration: underline; }
  </style>
</head>
<body>
  <h1>Silo Benchmark History</h1>
  <p class="subtitle">${results.length} runs from ${results[0].date.split("T")[0]} to ${results[results.length - 1].date.split("T")[0]} &mdash; generated ${new Date().toISOString().split("T")[0]}</p>

  <div class="chart-container">
    <h2>Enqueue Throughput (jobs/sec)</h2>
    <div class="chart-wrap"><canvas id="enqueueChart"></canvas></div>
  </div>

  <div class="chart-container">
    <h2>Dequeue Throughput (jobs/sec)</h2>
    <div class="chart-wrap"><canvas id="dequeueChart"></canvas></div>
  </div>

  <div class="table-container">
    <h2>All Results</h2>
    <table>
      <thead>
        <tr>
          <th>Date</th>
          <th>Commit</th>
          ${headerCells}
        </tr>
      </thead>
      <tbody>
        ${tableRows}
      </tbody>
    </table>
  </div>

  <script>
    const data = ${dataJson};
    const labels = data.map(r => r.sha);

    function makeChart(canvasId, metricKeys, colors, metricLabels) {
      const datasets = metricKeys.map((key, i) => ({
        label: metricLabels[i],
        data: data.map(r => r[key] ?? null),
        borderColor: colors[i],
        backgroundColor: colors[i] + "20",
        borderWidth: 2,
        pointRadius: 3,
        pointHoverRadius: 6,
        tension: 0.1,
        spanGaps: true,
      }));

      new Chart(document.getElementById(canvasId), {
        type: "line",
        data: { labels, datasets },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: { mode: "index", intersect: false },
          plugins: {
            tooltip: {
              callbacks: {
                title: (items) => {
                  const idx = items[0].dataIndex;
                  const r = data[idx];
                  return r.sha + " - " + new Date(r.date).toLocaleDateString();
                },
                label: (item) =>
                  item.dataset.label + ": " + Math.round(item.raw).toLocaleString() + " jobs/sec",
                afterBody: (items) => {
                  const idx = items[0].dataIndex;
                  return data[idx].title;
                },
              },
            },
          },
          scales: {
            y: {
              beginAtZero: true,
              title: { display: true, text: "jobs/sec" },
            },
            x: {
              title: { display: true, text: "Commit" },
            },
          },
        },
      });
    }

    makeChart(
      "enqueueChart",
      ["enqueue_1p", "enqueue_4p", "enqueue_8p"],
      ${enqueueColors},
      ${enqueueLabels}
    );

    makeChart(
      "dequeueChart",
      ["dequeue_1c", "dequeue_4c", "dequeue_8c"],
      ${dequeueColors},
      ${dequeueLabels}
    );
  </script>
</body>
</html>`;

const outPath = join(tmpdir(), "silo-benchmark-history.html");
writeFileSync(outPath, html);
console.log(`Report written to ${outPath}`);

// Open in browser
try {
  const platform = process.platform;
  const openCmd = platform === "darwin" ? "open" : platform === "win32" ? "start" : "xdg-open";
  execFileSync(openCmd, [outPath]);
  console.log("Opened in browser.");
} catch {
  console.log(`Open manually: ${outPath}`);
}
