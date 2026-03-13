import { useState, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  MarkerType,
} from 'reactflow'
import 'reactflow/dist/style.css'
import { fetchDags, fetchDag, decomposeDag, submitDag } from '../api/client'
import { GitFork, Sparkles, Play, ChevronDown, ChevronRight, RefreshCw } from 'lucide-react'

// ── Color scheme by job_status ────────────────────────────────────────────────
const STATUS_COLOR = {
  pending:    { border: '#2a4060', bg: '#0d1117', text: '#94a3b8' },
  queued:     { border: '#3b82f6', bg: '#0f1e3d', text: '#60a5fa' },
  running:    { border: '#06b6d4', bg: '#061b22', text: '#22d3ee' },
  completed:  { border: '#10b981', bg: '#061a0e', text: '#34d399' },
  failed:     { border: '#ef4444', bg: '#1c0808', text: '#f87171' },
  paused:     { border: '#f59e0b', bg: '#1c1200', text: '#fbbf24' },
  cancelled:  { border: '#4b5563', bg: '#0d1117', text: '#6b7280' },
  dead_letter:{ border: '#dc2626', bg: '#2d0404', text: '#fca5a5' },
}

// ── Custom ReactFlow node ─────────────────────────────────────────────────────
function SimNode({ data }) {
  const cfg = STATUS_COLOR[data.status] || STATUS_COLOR.pending
  return (
    <div style={{
      background: cfg.bg,
      border: `1.5px solid ${cfg.border}`,
      borderRadius: 5,
      padding: '10px 14px',
      minWidth: 160,
      fontFamily: "'JetBrains Mono', monospace",
      boxShadow: `0 0 10px ${cfg.border}33`,
    }}>
      <div style={{ fontSize: 9, letterSpacing: '0.12em', textTransform: 'uppercase', color: cfg.text, opacity: 0.6, marginBottom: 3 }}>
        {data.job_type?.replace(/_/g, ' ')}
      </div>
      <div style={{ fontSize: 11, color: cfg.text, fontWeight: 600, marginBottom: 6 }}>
        {data.label}
      </div>
      <span style={{
        display: 'inline-block',
        padding: '2px 6px', borderRadius: 3,
        fontSize: 9, fontWeight: 700, letterSpacing: '0.08em', textTransform: 'uppercase',
        background: `${cfg.border}22`, color: cfg.text, border: `1px solid ${cfg.border}55`,
      }}>
        {data.status}
      </span>
    </div>
  )
}

const nodeTypes = { simNode: SimNode }

// ── Build ReactFlow nodes/edges from DagResponse ──────────────────────────────
// DagResponse.nodes: { node_id, job_id, job_type, job_status }
// DagResponse.edges: { parent_node_id, child_node_id }
function buildFlowFromDag(dag) {
  if (!dag?.nodes?.length) return { nodes: [], edges: [] }

  const cols = 4
  const nodes = dag.nodes.map((n, i) => ({
    id: n.node_id,
    type: 'simNode',
    position: { x: (i % cols) * 220 + 40, y: Math.floor(i / cols) * 140 + 40 },
    data: {
      label: n.node_id,
      status: n.job_status || 'pending',
      job_type: n.job_type,
    },
  }))

  const edges = (dag.edges || []).map((e, i) => ({
    id: `e${i}`,
    source: e.parent_node_id,
    target: e.child_node_id,
    animated: dag.status === 'running',
    style: { stroke: '#2a4060', strokeWidth: 1.5 },
    markerEnd: { type: MarkerType.ArrowClosed, color: '#2a4060' },
  }))

  return { nodes, edges }
}

// ── Build preview flow from DagSubmitRequest (dag_spec) ───────────────────────
// dag_spec.nodes: { node_id, job_type, depends_on, ... }
function buildFlowFromSpec(spec) {
  if (!spec?.nodes?.length) return { nodes: [], edges: [] }

  const cols = 4
  const nodes = spec.nodes.map((n, i) => ({
    id: n.node_id,
    type: 'simNode',
    position: { x: (i % cols) * 220 + 40, y: Math.floor(i / cols) * 140 + 40 },
    data: {
      label: n.node_id,
      status: 'pending',
      job_type: n.job_type,
    },
  }))

  const edges = []
  let edgeIdx = 0
  spec.nodes.forEach(n => {
    (n.depends_on || []).forEach(dep => {
      edges.push({
        id: `e${edgeIdx++}`,
        source: dep,
        target: n.node_id,
        animated: false,
        style: { stroke: '#f59e0b44', strokeWidth: 1.5 },
        markerEnd: { type: MarkerType.ArrowClosed, color: '#f59e0b' },
      })
    })
  })

  return { nodes, edges }
}

// ── DAG card in sidebar ───────────────────────────────────────────────────────
function DagCard({ dag, isSelected, onSelect }) {
  return (
    <div
      onClick={onSelect}
      style={{
        padding: '10px 12px',
        background: isSelected ? 'var(--bg-elevated)' : 'transparent',
        border: `1px solid ${isSelected ? 'var(--border-bright)' : 'var(--border)'}`,
        borderRadius: 4, cursor: 'pointer', transition: 'all 0.15s',
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 3 }}>
        <span style={{ fontSize: 11, color: 'var(--text-primary)', fontWeight: 500 }}>
          {dag.name}
        </span>
        {isSelected ? <ChevronDown size={11} color="var(--text-muted)" /> : <ChevronRight size={11} color="var(--text-muted)" />}
      </div>
      <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>
        {dag.nodes?.length ?? 0} nodes · <span style={{ color: STATUS_COLOR[dag.status]?.text || 'var(--text-muted)' }}>{dag.status}</span>
      </div>
    </div>
  )
}

// ── Main component ────────────────────────────────────────────────────────────
export default function DagVisualizer() {
  const qc = useQueryClient()
  const [prompt, setPrompt] = useState('')
  const [pendingSpec, setPendingSpec] = useState(null)   // DagSubmitRequest from LLM
  const [activeDagId, setActiveDagId] = useState(null)
  const [nodes, setNodes, onNodesChange] = useNodesState([])
  const [edges, setEdges, onEdgesChange] = useEdgesState([])

  // List all DAGs
  const { data: dagsData } = useQuery({
    queryKey: ['dags'],
    queryFn: fetchDags,
    refetchInterval: 5000,
  })
  const dags = dagsData?.items ?? []

  // Poll active DAG
  const { data: activeDag } = useQuery({
    queryKey: ['dag', activeDagId],
    queryFn: () => fetchDag(activeDagId),
    enabled: !!activeDagId,
    refetchInterval: 2000,
  })

  // Update flow when active DAG data changes
  useEffect(() => {
    if (activeDag && !pendingSpec) {
      const { nodes: n, edges: e } = buildFlowFromDag(activeDag)
      setNodes(n)
      setEdges(e)
    }
  }, [activeDag, pendingSpec, setNodes, setEdges])

  // Decompose mutation
  // POST /api/v1/dags/decompose → DecomposeResponse { dag, dag_spec, validation_result, ... }
  const decomposeMutation = useMutation({
    mutationFn: (instruction) => decomposeDag({ instruction, dry_run: false }),
    onSuccess: (resp) => {
      // If dry_run=false and valid, resp.dag is the created DAG
      if (resp.dag) {
        setActiveDagId(resp.dag.id)
        setPendingSpec(null)
        qc.invalidateQueries(['dags'])
        const { nodes: n, edges: e } = buildFlowFromDag(resp.dag)
        setNodes(n)
        setEdges(e)
      } else {
        // Validation failed or dry_run — show spec preview
        setPendingSpec(resp.dag_spec)
        setActiveDagId(null)
        const { nodes: n, edges: e } = buildFlowFromSpec(resp.dag_spec)
        setNodes(n)
        setEdges(e)
      }
    },
  })

  // Manual submit of pending spec
  const submitMutation = useMutation({
    mutationFn: submitDag,
    onSuccess: (dag) => {
      setActiveDagId(dag.id)
      setPendingSpec(null)
      qc.invalidateQueries(['dags'])
      const { nodes: n, edges: e } = buildFlowFromDag(dag)
      setNodes(n)
      setEdges(e)
    },
  })

  const handleDecompose = () => {
    if (!prompt.trim()) return
    setPendingSpec(null)
    decomposeMutation.mutate(prompt.trim())
  }

  const handleSubmitSpec = () => {
    if (!pendingSpec) return
    submitMutation.mutate(pendingSpec)
  }

  const handleSelectDag = (dag) => {
    if (activeDagId === dag.id) {
      setActiveDagId(null)
      setNodes([])
      setEdges([])
    } else {
      setActiveDagId(dag.id)
      setPendingSpec(null)
      const { nodes: n, edges: e } = buildFlowFromDag(dag)
      setNodes(n)
      setEdges(e)
    }
  }

  return (
    <div style={{ height: '100%', display: 'flex', overflow: 'hidden' }}>
      {/* ── Left panel ──────────────────────────────────────────── */}
      <div style={{
        width: 280, flexShrink: 0,
        borderRight: '1px solid var(--border)',
        display: 'flex', flexDirection: 'column',
        background: 'var(--bg-base)', overflow: 'hidden',
      }}>
        {/* Header */}
        <div style={{ padding: '18px 16px 12px', borderBottom: '1px solid var(--border)', flexShrink: 0 }}>
          <h2 style={{ fontFamily: 'var(--font-display)', fontSize: 16, fontWeight: 700, color: 'var(--text-primary)', marginBottom: 2 }}>
            DAG Planner
          </h2>
          <p style={{ fontSize: 10, color: 'var(--text-muted)' }}>
            Describe a task → AI decomposes → visual DAG
          </p>
        </div>

        {/* Decompose form */}
        <div style={{ padding: 14, borderBottom: '1px solid var(--border)', flexShrink: 0 }}>
          <textarea
            className="field"
            style={{ resize: 'vertical', minHeight: 88, fontSize: 11, marginBottom: 8 }}
            placeholder="e.g. Run a CFD simulation, then thermal analysis, then structural check..."
            value={prompt}
            onChange={e => setPrompt(e.target.value)}
            onKeyDown={e => { if (e.key === 'Enter' && e.ctrlKey) handleDecompose() }}
          />

          <button
            className="btn-primary"
            style={{ width: '100%' }}
            onClick={handleDecompose}
            disabled={decomposeMutation.isPending || !prompt.trim()}
          >
            {decomposeMutation.isPending
              ? <><RefreshCw size={12} className="animate-spin-slow" /> Decomposing…</>
              : <><Sparkles size={12} /> Decompose with AI</>
            }
          </button>

          {/* Show submit button if there's a pending spec (validation failed path) */}
          {pendingSpec && (
            <button
              className="btn-ghost"
              style={{ width: '100%', marginTop: 6, borderColor: 'var(--success)', color: 'var(--success)' }}
              onClick={handleSubmitSpec}
              disabled={submitMutation.isPending}
            >
              <Play size={12} />
              {submitMutation.isPending ? 'Submitting…' : `Submit (${pendingSpec.nodes?.length} nodes)`}
            </button>
          )}

          {decomposeMutation.isError && (
            <div style={{ fontSize: 11, color: '#f87171', marginTop: 8, padding: '6px 10px', background: 'rgba(239,68,68,0.08)', borderRadius: 3 }}>
              {decomposeMutation.error?.response?.data?.detail || 'Decomposition failed'}
            </div>
          )}

          {decomposeMutation.isSuccess && decomposeMutation.data && (
            <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 6 }}>
              {decomposeMutation.data.llm_model} · {decomposeMutation.data.llm_latency_ms}ms
            </div>
          )}
        </div>

        {/* DAG list */}
        <div style={{ flex: 1, overflowY: 'auto', padding: 12 }}>
          <div className="section-label" style={{ marginBottom: 8 }}>
            Existing DAGs ({dags.length})
          </div>
          {dags.length === 0 ? (
            <div style={{ fontSize: 11, color: 'var(--text-muted)', textAlign: 'center', paddingTop: 16 }}>
              No DAGs yet
            </div>
          ) : (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
              {dags.map(dag => (
                <DagCard
                  key={dag.id}
                  dag={dag}
                  isSelected={activeDagId === dag.id}
                  onSelect={() => handleSelectDag(dag)}
                />
              ))}
            </div>
          )}
        </div>
      </div>

      {/* ── Flow canvas ─────────────────────────────────────────── */}
      <div style={{ flex: 1, position: 'relative', background: 'var(--bg-void)' }}>
        {nodes.length === 0 ? (
          <div style={{
            position: 'absolute', inset: 0,
            display: 'flex', flexDirection: 'column',
            alignItems: 'center', justifyContent: 'center',
            gap: 12, color: 'var(--text-muted)',
          }}>
            <GitFork size={40} strokeWidth={1} />
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: 14, marginBottom: 4 }}>No DAG to display</div>
              <div style={{ fontSize: 11 }}>Type a prompt and press "Decompose with AI"</div>
              <div style={{ fontSize: 10, marginTop: 4, color: 'var(--text-muted)', opacity: 0.6 }}>Tip: Ctrl+Enter to submit</div>
            </div>
          </div>
        ) : (
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            nodeTypes={nodeTypes}
            fitView
            fitViewOptions={{ padding: 0.3 }}
            proOptions={{ hideAttribution: true }}
          >
            <Background color="#1e3048" gap={20} size={1} />
            <Controls style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)' }} />
            <MiniMap
              style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)' }}
              nodeColor={n => STATUS_COLOR[n.data?.status]?.border || '#2a4060'}
            />
          </ReactFlow>
        )}

        {/* Live badge */}
        {activeDagId && (
          <div style={{
            position: 'absolute', top: 12, right: 12,
            background: 'var(--bg-surface)', border: '1px solid var(--border)',
            borderRadius: 4, padding: '6px 12px', fontSize: 11,
            display: 'flex', alignItems: 'center', gap: 8,
          }}>
            <div className="live-dot" />
            <span style={{ color: 'var(--text-muted)' }}>Live — polling every 2s</span>
          </div>
        )}

        {/* Pending spec badge */}
        {pendingSpec && !activeDagId && (
          <div style={{
            position: 'absolute', top: 12, right: 12,
            background: 'rgba(245,158,11,0.1)', border: '1px solid rgba(245,158,11,0.3)',
            borderRadius: 4, padding: '6px 12px', fontSize: 11,
            display: 'flex', alignItems: 'center', gap: 8,
          }}>
            <Sparkles size={11} color="var(--accent)" />
            <span style={{ color: 'var(--accent)' }}>Preview — click Submit to run</span>
          </div>
        )}
      </div>
    </div>
  )
}
