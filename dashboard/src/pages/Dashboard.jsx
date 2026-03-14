import { useQuery } from '@tanstack/react-query'
import { fetchMetrics } from '../api/client'
import {
  AlertTriangle, TrendingUp, Clock, CheckCircle,
  XCircle, Users, Activity, Layers,
} from 'lucide-react'

const PRIORITY_CONFIG = {
  critical: { color: '#ef4444', glow: 'rgba(239,68,68,0.3)', label: 'CRITICAL', icon: AlertTriangle },
  high:     { color: '#f59e0b', glow: 'rgba(245,158,11,0.3)', label: 'HIGH',     icon: TrendingUp  },
  normal:   { color: '#3b82f6', glow: 'rgba(59,130,246,0.3)',  label: 'NORMAL',   icon: Activity    },
  low:      { color: '#6b7280', glow: 'rgba(107,114,128,0.2)', label: 'LOW',      icon: Clock       },
}

function QueueCard({ priority, depth }) {
  const cfg = PRIORITY_CONFIG[priority] || PRIORITY_CONFIG.normal
  const Icon = cfg.icon
  return (
    <div className="panel corner-accent" style={{ padding: '16px 20px', flex: 1, minWidth: 140 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 12 }}>
        <span className="section-label">{cfg.label}</span>
        <Icon size={13} color={cfg.color} />
      </div>
      <div className="metric-number" style={{ fontSize: 32, color: cfg.color, textShadow: `0 0 20px ${cfg.glow}` }}>
        {depth ?? 0}
      </div>
      <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 4, letterSpacing: '0.06em' }}>
        queued jobs
      </div>
    </div>
  )
}

function StatCard({ label, value, icon: Icon, color = 'var(--text-primary)', sub }) {
  return (
    <div className="panel" style={{ padding: '14px 18px', flex: 1, minWidth: 130 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
        <span className="section-label">{label}</span>
        <Icon size={13} color={color} />
      </div>
      <div className="metric-number" style={{ fontSize: 28, color }}>
        {value ?? '—'}
      </div>
      {sub && <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 4 }}>{sub}</div>}
    </div>
  )
}

function ThroughputBar({ completed, failed }) {
  const total = (completed || 0) + (failed || 0)
  const pct = total > 0 ? Math.round(((completed || 0) / total) * 100) : 0
  return (
    <div className="panel" style={{ padding: '16px 20px', gridColumn: '1 / -1' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 10 }}>
        <span className="section-label">Throughput — last hour</span>
        <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>{pct}% success rate</span>
      </div>
      <div style={{ display: 'flex', gap: 16, marginBottom: 12 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, color: 'var(--success)' }}>
          <CheckCircle size={12} /> <strong>{completed ?? 0}</strong> completed
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, color: '#f87171' }}>
          <XCircle size={12} /> <strong>{failed ?? 0}</strong> failed
        </div>
      </div>
      <div className="progress-bar" style={{ height: 6 }}>
        <div className="progress-fill" style={{ width: `${pct}%` }} />
      </div>
    </div>
  )
}

export default function Dashboard() {
  const { data, isLoading, error, dataUpdatedAt } = useQuery({
    queryKey: ['metrics'],
    queryFn: fetchMetrics,
    refetchInterval: 2000,
  })

  const lastUpdate = dataUpdatedAt ? new Date(dataUpdatedAt).toLocaleTimeString() : '—'

  return (
    <div style={{ height: '100%', overflowY: 'auto', padding: '24px', display: 'flex', flexDirection: 'column', gap: 24 }}>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <h1 style={{ fontFamily: 'var(--font-display)', fontSize: 20, fontWeight: 700, color: 'var(--text-primary)', letterSpacing: '-0.01em' }}>
            Mission Control
          </h1>
          <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 2 }}>
            Real-time system overview
          </div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 11, color: 'var(--text-muted)' }}>
          <div className="live-dot" />
          Updated {lastUpdate}
        </div>
      </div>

      {error && !data && (
        <div style={{ background: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.3)', borderRadius: 4, padding: '10px 14px', color: '#f87171', fontSize: 12 }}>
          Cannot reach backend — is the server running at localhost:8000?
        </div>
      )}

      {/* Getting started hint when system is empty */}
      {data && !error && data.total_queue_depth === 0 && data.running_jobs === 0 && data.completed_last_hour === 0 && (
        <div className="panel corner-accent" style={{ padding: '16px 20px', background: 'rgba(245,158,11,0.04)', border: '1px solid rgba(245,158,11,0.2)' }}>
          <div style={{ fontSize: 13, color: 'var(--accent)', fontWeight: 600, marginBottom: 6 }}>
            System is idle — seed it with demo data
          </div>
          <div style={{ fontSize: 11, color: 'var(--text-secondary)', lineHeight: 1.6 }}>
            Run <code style={{ background: 'var(--bg-void)', padding: '2px 6px', borderRadius: 3, fontSize: 11 }}>python scripts/seed_demo.py</code> from
            the <code style={{ background: 'var(--bg-void)', padding: '2px 6px', borderRadius: 3, fontSize: 11 }}>simulation-engine/</code> directory
            to submit sample jobs and a DAG. Or go to <strong>Submit</strong> in the sidebar to create a job manually.
            Make sure at least one <strong>worker</strong> is running to process jobs.
          </div>
        </div>
      )}

      {/* Queue depth cards */}
      <div>
        <div className="section-label" style={{ marginBottom: 10 }}>Queue Depth by Priority</div>
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
          {isLoading ? (
            Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="shimmer" style={{ height: 90, flex: 1, minWidth: 140 }} />
            ))
          ) : (
            ['critical', 'high', 'normal', 'low'].map(p => (
              <QueueCard key={p} priority={p} depth={data?.queue_depth?.[p]} />
            ))
          )}
        </div>
      </div>

      {/* Worker & job stats */}
      <div>
        <div className="section-label" style={{ marginBottom: 10 }}>System Status</div>
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
          {isLoading ? (
            Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="shimmer" style={{ height: 80, flex: 1, minWidth: 130 }} />
            ))
          ) : (
            <>
              <StatCard label="Active Workers"  value={data?.active_workers}    icon={Users}         color="var(--cyan)"   sub="status = busy" />
              <StatCard label="Running Jobs"    value={data?.running_jobs}      icon={Activity}      color="var(--cyan)"   sub="in execution" />
              <StatCard label="Total Queued"    value={data?.total_queue_depth} icon={Layers}        color="var(--accent)" sub="across all queues" />
              <StatCard label="Dead Letters"    value={data?.dead_letter_count} icon={AlertTriangle} color="#f87171"       sub="need attention" />
            </>
          )}
        </div>
      </div>

      {/* Throughput */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: 12 }}>
        {isLoading ? (
          <div className="shimmer" style={{ height: 100 }} />
        ) : (
          <ThroughputBar
            completed={data?.completed_last_hour}
            failed={data?.failed_last_hour}
          />
        )}
      </div>

      {/* Circuit breakers */}
      {data?.circuit_breakers && Object.keys(data.circuit_breakers).length > 0 && (
        <div>
          <div className="section-label" style={{ marginBottom: 10 }}>Circuit Breakers</div>
          <div className="panel" style={{ padding: '12px 16px' }}>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
              {Object.entries(data.circuit_breakers).map(([type, state]) => (
                <div key={type} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                  <span style={{ fontSize: 11, color: 'var(--text-secondary)' }}>{type}</span>
                  <span className={`badge badge-${state === 'CLOSED' ? 'completed' : state === 'OPEN' ? 'failed' : 'paused'}`}>
                    {state}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
