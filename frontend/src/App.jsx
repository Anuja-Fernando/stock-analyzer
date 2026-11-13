import { useEffect, useMemo, useState } from "react";
import { ReactFlow, Background, Controls, Handle, Position } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { NODES, EDGES } from "./workflow";

const getJson = async (path) => {
  const r = await fetch(`/api/${path}`);
  if (!r.ok) throw new Error(`${path}: HTTP ${r.status}`);
  return r.json();
};

function usePoll(path, ms = 15000) {
  const [state, setState] = useState({ data: null, error: null });
  useEffect(() => {
    let alive = true;
    const load = () =>
      getJson(path)
        .then((data) => alive && setState({ data, error: null }))
        .catch((e) => alive && setState((s) => ({ data: s.data, error: e.message })));
    load();
    const id = setInterval(load, ms);
    return () => { alive = false; clearInterval(id); };
  }, [path, ms]);
  return state;
}

const ago = (ms) => {
  if (!ms) return "never";
  const s = Math.max(0, Math.floor((Date.now() - ms) / 1000));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  if (s < 86400) return `${Math.floor(s / 3600)} h ago`;
  return `${Math.floor(s / 86400)} d ago`;
};

const cell = (v) =>
  v == null ? "n/a" : typeof v === "number" ? v.toLocaleString("en-IN", { maximumFractionDigits: 4 }) : String(v);

function nodeStatus(node, assets, checks) {
  if (node.kind === "source") return { status: "external", sub: "external source" };
  if (node.kind === "check") {
    const res = checks?.results;
    if (!res || !res.length) return { status: "unknown", sub: "no data" };
    if (res.some((c) => c.blocking && !c.passed)) return { status: "failed", sub: "blocking check failing" };
    if (res.some((c) => !c.blocking && !c.passed)) return { status: "warn", sub: "passing, with warnings" };
    return { status: "success", sub: "all checks passing" };
  }
  if (!assets?.online) return { status: "unknown", sub: "Dagster offline" };
  const a = assets.assets[node.asset];
  if (!a || !a.last_materialized_ms) return { status: "never", sub: "no materialization found" };
  return { status: "success", sub: `last run ${ago(a.last_materialized_ms)}` };
}

function StageNode({ data }) {
  return (
    <div className={`stage ${data.kind} ${data.status} ${data.selected ? "sel" : ""}`}>
      <Handle type="target" position={Position.Left} />
      <div className="stage-title">{data.label}</div>
      <div className="stage-sub">{data.sub}</div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}
const nodeTypes = { stage: StageNode };

function Table({ rows }) {
  if (!rows?.length) return <p className="muted">No rows.</p>;
  const cols = Object.keys(rows[0]);
  return (
    <div className="table-wrap">
      <table>
        <thead><tr>{cols.map((c) => <th key={c}>{c.replaceAll("_", " ")}</th>)}</tr></thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>{cols.map((c) => <td key={c}>{cell(r[c])}</td>)}</tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Detail({ node, st, checks }) {
  const [tbl, setTbl] = useState(null);
  const [report, setReport] = useState(null);

  useEffect(() => {
    setTbl(null);
    setReport(null);
    if (node.table)
      getJson(`table/${node.table}`).then(setTbl).catch((e) => setTbl({ exists: false, error: e.message }));
    if (node.id === "daily_risk_report")
      getJson("report/latest").then(setReport).catch(() => setReport({ exists: false }));
  }, [node.id, node.table]);

  return (
    <div className="panel">
      <h3>{node.label} <span className={`badge b-${st.status}`}>{st.sub}</span></h3>
      <p>{node.desc}</p>
      {node.file && <p className="muted">Code: <code>{node.file}</code></p>}
      {node.asset && (
        <p><a href={`http://localhost:3000/assets/${node.asset}`} target="_blank" rel="noreferrer">
          Open in Dagster
        </a></p>
      )}

      {node.kind === "check" && (
        <ul className="checks">
          {(checks?.results || []).map((c) => (
            <li key={c.name}>
              <span className={`badge ${c.passed ? "b-success" : c.blocking ? "b-failed" : "b-warn"}`}>
                {c.passed ? "PASS" : "FAIL"}
              </span>{" "}
              <strong>{c.name}</strong> {c.blocking ? "(blocking)" : "(warning only)"}
              <div className="muted">{c.detail}</div>
            </li>
          ))}
          {checks?.error && <li className="error">{checks.error}</li>}
        </ul>
      )}

      {node.table && tbl && (tbl.exists ? (
        <>
          <p className="muted">{Number(tbl.row_count).toLocaleString()} rows. Preview of the first 10:</p>
          <Table rows={tbl.preview} />
        </>
      ) : <p className="error">Table not available yet: {tbl.error}</p>)}

      {report && (report.exists ? (
        <>
          <p className="muted">Latest report: {report.name}</p>
          <pre className="report">{report.text}</pre>
        </>
      ) : <p className="muted">No report file found in the reports folder yet.</p>)}
    </div>
  );
}

function Runs({ runs }) {
  if (!runs) return <p className="muted">Loading...</p>;
  if (!runs.online) return <p className="error">Dagster is offline: {runs.error}</p>;
  if (!runs.runs.length) return <p className="muted">No runs yet.</p>;
  return (
    <div className="table-wrap">
      <table>
        <thead><tr><th>Job</th><th>Status</th><th>Started</th><th>Duration</th></tr></thead>
        <tbody>
          {runs.runs.map((r) => (
            <tr key={r.runId}>
              <td>{r.jobName}</td>
              <td><span className={`badge ${r.status === "SUCCESS" ? "b-success" : r.status === "FAILURE" ? "b-failed" : "b-warn"}`}>{r.status}</span></td>
              <td>{r.startTime ? ago(r.startTime * 1000) : "not started"}</td>
              <td>{!r.startTime ? "-" : r.endTime ? `${Math.round(r.endTime - r.startTime)}s` : "running"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function App() {
  const assets = usePoll("dagster/assets");
  const runs = usePoll("dagster/runs");
  const checks = usePoll("checks");
  const [selected, setSelected] = useState("stock_prices");

  const running = runs.data?.runs?.some((r) => r.status === "STARTED");

  const statuses = useMemo(() => {
    const out = {};
    NODES.forEach((n) => { out[n.id] = nodeStatus(n, assets.data, checks.data); });
    return out;
  }, [assets.data, checks.data]);

  const flowNodes = useMemo(() => NODES.map((n) => ({
    id: n.id, type: "stage", position: { x: n.x, y: n.y },
    data: { label: n.label, kind: n.kind, status: statuses[n.id].status,
            sub: statuses[n.id].sub, selected: n.id === selected },
  })), [statuses, selected]);

  const flowEdges = useMemo(() => EDGES.map(([s, t]) => ({
    id: `${s}-${t}`, source: s, target: t, animated: !!running,
    style: { stroke: "#94a3b8", strokeWidth: 2 },
  })), [running]);

  const node = NODES.find((n) => n.id === selected);

  return (
    <div className="app">
      <header>
        <h1>Portfolio Risk Monitor: pipeline workflow</h1>
        <span className={`badge ${assets.data?.online ? "b-success" : "b-failed"}`}>
          Dagster {assets.data?.online ? "online" : "offline"}
        </span>{" "}
        {running && <span className="badge b-warn">run in progress</span>}
      </header>
      <p className="muted">
        Click a box to see what happens at that stage and the data it produced. Box colour shows the
        last successful run. Failures show up in the checks box and the recent runs table. Refreshes every 15 seconds.
      </p>

      <div className="canvas">
        <ReactFlow nodes={flowNodes} edges={flowEdges} nodeTypes={nodeTypes} fitView
          nodesDraggable={false} nodesConnectable={false} minZoom={0.2}
          onNodeClick={(_, n) => setSelected(n.id)}>
          <Background />
          <Controls showInteractive={false} />
        </ReactFlow>
      </div>

      <div className="legend">
        <span className="badge b-success">ran successfully</span>
        <span className="badge b-never">never run</span>
        <span className="badge b-warn">warning</span>
        <span className="badge b-failed">failing</span>
        <span className="badge b-external">external source</span>
      </div>

      <div className="grid">
        {node && <Detail node={node} st={statuses[node.id]} checks={checks.data} />}
        <div className="panel">
          <h3>Recent Dagster runs</h3>
          <Runs runs={runs.data} />
        </div>
      </div>
    </div>
  );
}