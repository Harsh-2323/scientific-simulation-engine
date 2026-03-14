import { useState } from 'react'
import { useQuery, useMutation } from '@tanstack/react-query'
import { submitJob, fetchJob } from '../api/client'
import { Send, CheckCircle, AlertCircle } from 'lucide-react'

const JOB_TYPES = [
  'cfd_simulation',
  'thermal_analysis',
  'structural_analysis',
  'generic_computation',
]

function StatusBadge({ status }) {
  return <span className={`badge badge-${status}`}>{status?.replace('_', ' ')}</span>
}

function LiveJobCard({ jobId }) {
  const { data: job } = useQuery({
    queryKey: ['job', jobId],
    queryFn: () => fetchJob(jobId),
    refetchInterval: 2000,
    enabled: !!jobId,
  })

  if (!job) return (
    <div className="panel" style={{ padding: 16 }}>
      <div className="shimmer" style={{ height: 80 }} />
    </div>
  )

  const pct = job.progress_percent ?? 0
  const isDone = ['completed', 'failed', 'cancelled', 'dead_letter'].includes(job.status)

  return (
    <div className="panel corner-accent" style={{ padding: 20 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 14 }}>
        <div>
          <div style={{ fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--text-muted)', marginBottom: 4 }}>
            {job.id}
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <StatusBadge status={job.status} />
            <span style={{ fontSize: 11, color: 'var(--text-secondary)' }}>{job.type}</span>
          </div>
        </div>
        {!isDone && <div className="live-dot" />}
        {job.status === 'completed' && <CheckCircle size={18} color="var(--success)" />}
        {job.status === 'failed' && <AlertCircle size={18} color="#f87171" />}
      </div>

      <div style={{ marginBottom: 12 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
          <span className="section-label">Progress</span>
          <span style={{ fontSize: 11, color: 'var(--accent)' }}>{pct}%</span>
        </div>
        <div className="progress-bar" style={{ height: 5 }}>
          <div className="progress-fill" style={{ width: `${pct}%` }} />
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 10 }}>
        {[
          ['Priority', job.priority],
          ['Attempts', `${job.retry_count ?? 0}/${job.max_retries ?? '?'}`],
          ['Steps',    job.params?.simulation_steps ?? '—'],
        ].map(([k, v]) => (
          <div key={k}>
            <div className="section-label" style={{ marginBottom: 3 }}>{k}</div>
            <div style={{ fontSize: 12, color: 'var(--text-primary)' }}>{v}</div>
          </div>
        ))}
      </div>

      {job.error_message && (
        <div style={{
          marginTop: 12,
          background: 'rgba(239,68,68,0.08)', border: '1px solid rgba(239,68,68,0.2)',
          borderRadius: 3, padding: '8px 12px',
          fontSize: 11, color: '#f87171',
        }}>
          {job.error_message}
        </div>
      )}
    </div>
  )
}

export default function SubmitJob() {
  const [form, setForm] = useState({
    job_type: 'cfd_simulation',
    priority: 'normal',
    simulation_steps: 100,
    max_retries: 3,
    step_duration_seconds: 0.05,
  })
  const [submittedIds, setSubmittedIds] = useState([])

  const mutation = useMutation({
    mutationFn: submitJob,
    onSuccess: (data) => {
      setSubmittedIds(ids => [data.id, ...ids.slice(0, 4)])
    },
  })

  const handleSubmit = (e) => {
    e.preventDefault()
    mutation.mutate({
      type: form.job_type,
      priority: form.priority,
      max_retries: Number(form.max_retries),
      params: {
        simulation_steps: Number(form.simulation_steps),
        step_duration_seconds: Number(form.step_duration_seconds),
      },
    })
  }

  const setField = (k, v) => setForm(f => ({ ...f, [k]: v }))

  return (
    <div style={{ height: '100%', overflowY: 'auto', padding: '24px' }}>
      <div style={{ maxWidth: 680, margin: '0 auto', display: 'flex', flexDirection: 'column', gap: 24 }}>
        <div>
          <h1 style={{ fontFamily: 'var(--font-display)', fontSize: 20, fontWeight: 700, color: 'var(--text-primary)' }}>
            Submit Job
          </h1>
          <p style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 4 }}>
            Dispatch a simulation job to the execution engine
          </p>
        </div>

        <form onSubmit={handleSubmit} className="panel corner-accent" style={{ padding: 24 }}>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 20 }}>
            {/* Job Type */}
            <div>
              <label style={{ display: 'block', marginBottom: 6 }}>
                <span className="section-label">Job Type</span>
              </label>
              <select className="field" value={form.job_type} onChange={e => setField('job_type', e.target.value)}>
                {JOB_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
              </select>
            </div>

            {/* Priority */}
            <div>
              <label style={{ display: 'block', marginBottom: 6 }}>
                <span className="section-label">Priority</span>
              </label>
              <select className="field" value={form.priority} onChange={e => setField('priority', e.target.value)}>
                {['critical','high','normal','low'].map(p => <option key={p} value={p}>{p}</option>)}
              </select>
            </div>

            {/* Steps */}
            <div>
              <label style={{ display: 'block', marginBottom: 6 }}>
                <span className="section-label">Simulation Steps</span>
              </label>
              <input
                type="number" className="field"
                min={1} max={10000}
                value={form.simulation_steps}
                onChange={e => setField('simulation_steps', e.target.value)}
              />
            </div>

            {/* Max retries */}
            <div>
              <label style={{ display: 'block', marginBottom: 6 }}>
                <span className="section-label">Max Retries</span>
              </label>
              <input
                type="number" className="field"
                min={0} max={10}
                value={form.max_retries}
                onChange={e => setField('max_retries', e.target.value)}
              />
            </div>

            {/* Step duration */}
            <div>
              <label style={{ display: 'block', marginBottom: 6 }}>
                <span className="section-label">Step Duration (seconds)</span>
              </label>
              <input
                type="number" className="field"
                min={0.01} max={10} step={0.01}
                value={form.step_duration_seconds}
                onChange={e => setField('step_duration_seconds', e.target.value)}
              />
            </div>
          </div>

          {mutation.isError && (
            <div style={{
              background: 'rgba(239,68,68,0.08)', border: '1px solid rgba(239,68,68,0.2)',
              borderRadius: 3, padding: '10px 14px',
              fontSize: 11, color: '#f87171', marginBottom: 16,
            }}>
              {mutation.error?.response?.data?.detail || 'Failed to submit job'}
            </div>
          )}

          <button type="submit" className="btn-primary" disabled={mutation.isPending}>
            <Send size={13} />
            {mutation.isPending ? 'Dispatching…' : 'Dispatch Job'}
          </button>
        </form>

        {/* Live status of submitted jobs */}
        {submittedIds.length > 0 && (
          <div>
            <div className="section-label" style={{ marginBottom: 12 }}>Live Job Status</div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
              {submittedIds.map(id => <LiveJobCard key={id} jobId={id} />)}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
