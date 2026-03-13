import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { fetchJobs, fetchJob, fetchJobEvents, pauseJob, resumeJob, cancelJob } from '../api/client'
import { X, ChevronLeft, ChevronRight, RotateCcw, Pause, Play, XCircle, Clock } from 'lucide-react'

const STATUS_OPTS = ['','pending','queued','running','completed','failed','cancelled','paused','dead_letter','retry_scheduled']
const PRIORITY_OPTS = ['','critical','high','normal','low']
const TYPE_OPTS = ['','cfd_simulation','thermal_analysis','structural_analysis','generic_computation']

function StatusBadge({ status }) {
  return <span className={`badge badge-${status}`}>{status?.replace('_', ' ')}</span>
}

function PriorityBadge({ priority }) {
  return <span className={`badge badge-${priority}`}>{priority}</span>
}

function JobDetailModal({ jobId, onClose }) {
  const { data: job } = useQuery({
    queryKey: ['job', jobId],
    queryFn: () => fetchJob(jobId),
    refetchInterval: 2000,
    enabled: !!jobId,
  })
  const { data: eventsData } = useQuery({
    queryKey: ['job-events', jobId],
    queryFn: () => fetchJobEvents(jobId),
    refetchInterval: 3000,
    enabled: !!jobId,
  })
  const events = eventsData?.items ?? []

  const [actionLoading, setActionLoading] = useState(null)

  const doAction = async (fn, name) => {
    setActionLoading(name)
    try { await fn(jobId) } catch(e) { console.error(e) }
    finally { setActionLoading(null) }
  }

  if (!job) return (
    <div className="modal-overlay" onClick={onClose}>
      <div style={{ background: 'var(--bg-surface)', border: '1px solid var(--border)', borderRadius: 6, padding: 40 }}>
        <div className="shimmer" style={{ width: 400, height: 300 }} />
      </div>
    </div>
  )

  const pct = job.progress_percent ?? 0

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div
        onClick={e => e.stopPropagation()}
        style={{
          background: 'var(--bg-surface)',
          border: '1px solid var(--border)',
          borderRadius: 6,
          width: '90vw', maxWidth: 680,
          maxHeight: '85vh',
          overflow: 'hidden',
          display: 'flex',
          flexDirection: 'column',
        }}
      >
        {/* Header */}
        <div style={{
          padding: '16px 20px',
          borderBottom: '1px solid var(--border)',
          display: 'flex', justifyContent: 'space-between', alignItems: 'center',
          background: 'var(--bg-void)',
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <StatusBadge status={job.status} />
            <span style={{ fontFamily: 'var(--font-mono)', fontSize: 12, color: 'var(--text-muted)' }}>
              {job.id?.slice(0, 20)}…
            </span>
          </div>
          <button className="btn-ghost" style={{ padding: '4px 8px' }} onClick={onClose}>
            <X size={13} />
          </button>
        </div>

        <div style={{ overflowY: 'auto', flex: 1, padding: 20, display: 'flex', flexDirection: 'column', gap: 16 }}>
          {/* Meta */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 12 }}>
            {[
              ['Type',     job.type],
              ['Priority', <PriorityBadge priority={job.priority} />],
              ['Attempts', `${job.retry_count ?? 0} / ${job.max_retries ?? '?'}`],
            ].map(([k, v]) => (
              <div key={k} className="panel" style={{ padding: '10px 14px' }}>
                <div className="section-label" style={{ marginBottom: 6 }}>{k}</div>
                <div style={{ fontSize: 12, color: 'var(--text-primary)' }}>{v}</div>
              </div>
            ))}
          </div>

          {/* Progress */}
          <div>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
              <span className="section-label">Progress</span>
              <span style={{ fontSize: 11, color: 'var(--accent)' }}>{pct}%</span>
            </div>
            <div className="progress-bar" style={{ height: 6 }}>
              <div className="progress-fill" style={{ width: `${pct}%` }} />
            </div>
          </div>

          {/* Actions */}
          <div style={{ display: 'flex', gap: 8 }}>
            {['running', 'queued'].includes(job.status) && (
              <button className="btn-ghost" onClick={() => doAction(pauseJob, 'pause')} disabled={!!actionLoading}>
                <Pause size={12} /> Pause
              </button>
            )}
            {job.status === 'paused' && (
              <button className="btn-ghost" onClick={() => doAction(resumeJob, 'resume')} disabled={!!actionLoading}>
                <Play size={12} /> Resume
              </button>
            )}
            {['pending','queued','running','paused'].includes(job.status) && (
              <button className="btn-danger" onClick={() => doAction(cancelJob, 'cancel')} disabled={!!actionLoading}>
                <XCircle size={12} /> Cancel
              </button>
            )}
          </div>

          {/* Timeline */}
          <div>
            <div className="section-label" style={{ marginBottom: 12 }}>Event Timeline</div>
            {events.length === 0 ? (
              <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>No events recorded.</div>
            ) : (
              <div>
                {events.map((ev, i) => (
                  <div key={i} className="timeline-item">
                    <div className={`timeline-dot badge-${ev.new_status}`}
                      style={{ borderColor: 'var(--border-bright)', background: 'var(--bg-base)' }} />
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                      <div>
                        <div style={{ fontSize: 12, color: 'var(--text-primary)', marginBottom: 2 }}>
                          <span style={{ color: 'var(--text-muted)' }}>{ev.old_status ?? 'init'}</span>
                          {' → '}
                          <StatusBadge status={ev.new_status} />
                        </div>
                        {ev.triggered_by && (
                          <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 2 }}>by {ev.triggered_by}</div>
                        )}
                      </div>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 10, color: 'var(--text-muted)', whiteSpace: 'nowrap', marginLeft: 12 }}>
                        <Clock size={9} />
                        {new Date(ev.timestamp).toLocaleTimeString()}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Error */}
          {job.error_message && (
            <div>
              <div className="section-label" style={{ marginBottom: 8 }}>Error</div>
              <div style={{
                background: 'rgba(239,68,68,0.08)', border: '1px solid rgba(239,68,68,0.2)',
                borderRadius: 4, padding: '10px 14px',
                fontSize: 11, color: '#f87171', fontFamily: 'var(--font-mono)',
                whiteSpace: 'pre-wrap', wordBreak: 'break-all',
              }}>
                {job.error_message}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

export default function JobExplorer() {
  const [filters, setFilters] = useState({ status: '', priority: '', job_type: '' })
  const [page, setPage] = useState(0)
  const [selectedJob, setSelectedJob] = useState(null)
  const limit = 20

  const params = { limit, offset: page * limit }
  if (filters.status)   params.status   = filters.status
  if (filters.priority) params.priority = filters.priority
  if (filters.job_type) params.type     = filters.job_type

  const { data, isLoading } = useQuery({
    queryKey: ['jobs', params],
    queryFn: () => fetchJobs(params),
    refetchInterval: 3000,
  })

  const jobs = data?.items ?? []
  const total = data?.total ?? 0

  const setFilter = (k, v) => { setFilters(f => ({ ...f, [k]: v })); setPage(0) }

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column', padding: '24px', gap: 16 }}>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexShrink: 0 }}>
        <h1 style={{ fontFamily: 'var(--font-display)', fontSize: 20, fontWeight: 700, color: 'var(--text-primary)' }}>
          Job Explorer
        </h1>
        <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>{total} jobs</span>
      </div>

      {/* Filters */}
      <div style={{ display: 'flex', gap: 10, flexShrink: 0, flexWrap: 'wrap' }}>
        {[
          { key: 'status',   opts: STATUS_OPTS,   placeholder: 'All statuses'   },
          { key: 'priority', opts: PRIORITY_OPTS, placeholder: 'All priorities' },
          { key: 'job_type', opts: TYPE_OPTS,     placeholder: 'All types'      },
        ].map(({ key, opts, placeholder }) => (
          <select
            key={key}
            className="field"
            style={{ width: 180 }}
            value={filters[key]}
            onChange={e => setFilter(key, e.target.value)}
          >
            <option value="">{placeholder}</option>
            {opts.filter(Boolean).map(o => (
              <option key={o} value={o}>{o}</option>
            ))}
          </select>
        ))}
        <button className="btn-ghost" onClick={() => { setFilters({ status: '', priority: '', job_type: '' }); setPage(0) }}>
          <RotateCcw size={12} /> Reset
        </button>
      </div>

      {/* Table */}
      <div className="panel" style={{ flex: 1, overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
        <div style={{ overflowY: 'auto', flex: 1 }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>Job ID</th>
                <th>Type</th>
                <th>Status</th>
                <th>Priority</th>
                <th>Progress</th>
                <th>Attempts</th>
                <th>Created</th>
              </tr>
            </thead>
            <tbody>
              {isLoading ? (
                Array.from({ length: 8 }).map((_, i) => (
                  <tr key={i}>
                    {Array.from({ length: 7 }).map((_, j) => (
                      <td key={j}><div className="shimmer" style={{ height: 12, width: '80%' }} /></td>
                    ))}
                  </tr>
                ))
              ) : jobs.length === 0 ? (
                <tr>
                  <td colSpan={7} style={{ textAlign: 'center', padding: 32, color: 'var(--text-muted)' }}>
                    No jobs found
                  </td>
                </tr>
              ) : (
                jobs.map(job => (
                  <tr key={job.id} onClick={() => setSelectedJob(job.id)}>
                    <td style={{ fontFamily: 'var(--font-mono)', fontSize: 11 }}>{String(job.id)?.slice(0, 12)}…</td>
                    <td style={{ fontSize: 11 }}>{job.type}</td>
                    <td><StatusBadge status={job.status} /></td>
                    <td><PriorityBadge priority={job.priority} /></td>
                    <td>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                        <div className="progress-bar" style={{ width: 60 }}>
                          <div className="progress-fill" style={{ width: `${job.progress_percent ?? 0}%` }} />
                        </div>
                        <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>{job.progress_percent ?? 0}%</span>
                      </div>
                    </td>
                    <td style={{ fontSize: 11 }}>{job.retry_count ?? 0}</td>
                    <td style={{ fontSize: 10, color: 'var(--text-muted)' }}>
                      {new Date(job.created_at).toLocaleString()}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        <div style={{
          padding: '10px 16px',
          borderTop: '1px solid var(--border)',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          flexShrink: 0,
        }}>
          <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>
            Page {page + 1} · {page * limit + 1}–{Math.min((page + 1) * limit, total)} of {total}
          </span>
          <div style={{ display: 'flex', gap: 6 }}>
            <button className="btn-ghost" style={{ padding: '4px 10px' }} onClick={() => setPage(p => Math.max(0, p - 1))} disabled={page === 0}>
              <ChevronLeft size={13} />
            </button>
            <button className="btn-ghost" style={{ padding: '4px 10px' }} onClick={() => setPage(p => p + 1)} disabled={(page + 1) * limit >= total}>
              <ChevronRight size={13} />
            </button>
          </div>
        </div>
      </div>

      {selectedJob && <JobDetailModal jobId={selectedJob} onClose={() => setSelectedJob(null)} />}
    </div>
  )
}
