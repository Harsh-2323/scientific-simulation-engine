import axios from 'axios'

export const api = axios.create({
  baseURL: 'http://localhost:8000/api/v1',
  timeout: 15000,
  headers: { 'Content-Type': 'application/json' },
})

// ── Metrics & Health ──────────────────────────────────────────────────────────
export const fetchMetrics = () => api.get('/metrics').then(r => r.data)
export const fetchHealth  = () => api.get('/health').then(r => r.data)

// ── Jobs ──────────────────────────────────────────────────────────────────────
export const fetchJobs = (params = {}) =>
  api.get('/jobs', { params }).then(r => r.data)

export const fetchJob = (id) =>
  api.get(`/jobs/${id}`).then(r => r.data)

export const fetchJobEvents = (id) =>
  api.get(`/jobs/${id}/events`).then(r => r.data)

export const submitJob = (payload) =>
  api.post('/jobs', payload).then(r => r.data)

export const pauseJob = (id) =>
  api.post(`/jobs/${id}/pause`).then(r => r.data)

export const resumeJob = (id) =>
  api.post(`/jobs/${id}/resume`).then(r => r.data)

export const cancelJob = (id) =>
  api.post(`/jobs/${id}/cancel`).then(r => r.data)

// ── DAGs ──────────────────────────────────────────────────────────────────────
export const fetchDags = () =>
  api.get('/dags').then(r => r.data)

export const fetchDag = (id) =>
  api.get(`/dags/${id}`).then(r => r.data)

export const decomposeDag = (payload) =>
  api.post('/dags/decompose', payload).then(r => r.data)

export const submitDag = (payload) =>
  api.post('/dags', payload).then(r => r.data)
