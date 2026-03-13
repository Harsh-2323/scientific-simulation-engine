import { useQuery } from '@tanstack/react-query'
import { fetchHealth, fetchMetrics } from '../api/client'
import { Database, Layers, AlertTriangle, CheckCircle, XCircle, RefreshCw } from 'lucide-react'

function ServiceCard({ name, status, icon: Icon, latency }) {
  const up = status === 'ok' || status === 'healthy' || status === true
  return (
    <div className="panel corner-accent" style={{ padding: '16px 20px', flex: 1, minWidth: 180 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <Icon size={14} color={up ? 'var(--success)' : '#f87171'} />
          <span style={{ fontFamily: 'var(--font-display)', fontSize: 14, fontWeight: 600, color: 'var(--text-primary)' }}>
            {name}
          </span>
        </div>
        <div className={`health-dot ${up ? 'up' : 'down'}`} />
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        {up
          ? <><CheckCircle size={13} color="var(--success)" /><span style={{ fontSize: 12, color: 'var(--success)' }}>Operational</span></>
          : <><XCircle size={13} color="#f87171" /><span style={{ fontSize: 12, color: '#f87171' }}>Down</span></>
        }
        {latency != null && (
          <span style={{ fontSize: 10, color: 'var(--text-muted)', marginLeft: 'auto' }}>{latency}ms</span>
        )}
      </div>
    </div>
  )
}

function CircuitBreakerCard({ jobType, state }) {
  const stateStyle = {
    CLOSED:    { badge: 'completed', label: 'Closed', hint: 'Normal operation' },
    OPEN:      { badge: 'failed',    label: 'Open',   hint: 'Blocking new jobs' },
    HALF_OPEN: { badge: 'paused',    label: 'Half-Open', hint: 'Probing recovery' },
  }
  const s = stateStyle[state] || stateStyle.CLOSED

  return (
    <div style={{
      padding: '12px 16px',
      background: 'var(--bg-void)',
      border: '1px solid var(--border)',
      borderRadius: 4,
      display: 'flex',
      justifyContent: 'space-between',
      alignItems: 'center',
    }}>
      <div>
        <div style={{ fontSize: 12, color: 'var(--text-primary)', fontWeight: 500, marginBottom: 3 }}>
          {jobType}
        </div>
        <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>{s.hint}</div>
      </div>
      <span className={`badge badge-${s.badge}`}>{s.label}</span>
    </div>
  )
}

export default function SystemHealth() {
  const { data: health, isLoading: hLoading, error: hError } = useQuery({
    queryKey: ['health'],
    queryFn: fetchHealth,
    refetchInterval: 5000,
  })

  const { data: metrics, isLoading: mLoading } = useQuery({
    queryKey: ['metrics'],
    queryFn: fetchMetrics,
    refetchInterval: 5000,
  })

  const isLoading = hLoading || mLoading

  const postgres = health?.postgres  // boolean
  const redis    = health?.redis     // boolean
  const circuitBreakers = {}         // not in current API — placeholder
  const deadLetter = metrics?.dead_letter_count ?? 0

  return (
    <div style={{ height: '100%', overflowY: 'auto', padding: '24px', display: 'flex', flexDirection: 'column', gap: 24 }}>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <h1 style={{ fontFamily: 'var(--font-display)', fontSize: 20, fontWeight: 700, color: 'var(--text-primary)' }}>
            System Health
          </h1>
          <p style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 2 }}>
            Infrastructure status &amp; circuit breaker state
          </p>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11, color: 'var(--text-muted)' }}>
          <RefreshCw size={11} />
          Polls every 5s
        </div>
      </div>

      {hError && (
        <div style={{ background: 'rgba(239,68,68,0.1)', border: '1px solid rgba(239,68,68,0.3)', borderRadius: 4, padding: '10px 14px', color: '#f87171', fontSize: 12 }}>
          Cannot reach health endpoint — is the server running?
        </div>
      )}

      {/* Services */}
      <div>
        <div className="section-label" style={{ marginBottom: 10 }}>Services</div>
        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
          {isLoading ? (
            Array.from({ length: 3 }).map((_, i) => (
              <div key={i} className="shimmer" style={{ height: 90, flex: 1, minWidth: 180 }} />
            ))
          ) : (
            <>
              <ServiceCard name="PostgreSQL" icon={Database} status={postgres} />
              <ServiceCard name="Redis"      icon={Layers}   status={redis} />
              <div className="panel corner-accent" style={{ padding: '16px 20px', flex: 1, minWidth: 180 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <AlertTriangle size={14} color={deadLetter > 0 ? '#f87171' : 'var(--text-muted)'} />
                    <span style={{ fontFamily: 'var(--font-display)', fontSize: 14, fontWeight: 600, color: 'var(--text-primary)' }}>
                      Dead Letter
                    </span>
                  </div>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <span className="metric-number" style={{ fontSize: 28, color: deadLetter > 0 ? '#f87171' : 'var(--text-muted)' }}>
                    {deadLetter}
                  </span>
                  <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>jobs</span>
                </div>
              </div>
            </>
          )}
        </div>
      </div>

      {/* Circuit breakers */}
      <div>
        <div className="section-label" style={{ marginBottom: 10 }}>Circuit Breakers</div>
        {isLoading ? (
          <div className="panel" style={{ padding: 16 }}>
            <div className="shimmer" style={{ height: 120 }} />
          </div>
        ) : Object.keys(circuitBreakers).length === 0 ? (
          <div className="panel" style={{ padding: '20px 24px' }}>
            <div style={{ fontSize: 12, color: 'var(--text-muted)', textAlign: 'center' }}>
              No circuit breaker data available. Submit some jobs to see breaker states.
            </div>
          </div>
        ) : (
          <div className="panel" style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 8 }}>
            {Object.entries(circuitBreakers).map(([type, state]) => (
              <CircuitBreakerCard key={type} jobType={type} state={state} />
            ))}
          </div>
        )}
      </div>

      {/* Raw health dump */}
      {health && (
        <div>
          <div className="section-label" style={{ marginBottom: 10 }}>Raw Health Response</div>
          <div className="panel" style={{ padding: 16 }}>
            <pre style={{
              fontFamily: 'var(--font-mono)', fontSize: 11,
              color: 'var(--text-secondary)', whiteSpace: 'pre-wrap',
              wordBreak: 'break-all',
            }}>
              {JSON.stringify(health, null, 2)}
            </pre>
          </div>
        </div>
      )}
    </div>
  )
}
