import { Routes, Route } from 'react-router-dom'
import Sidebar from './components/Sidebar'
import Dashboard from './pages/Dashboard'
import JobExplorer from './pages/JobExplorer'
import DagVisualizer from './pages/DagVisualizer'
import SubmitJob from './pages/SubmitJob'
import SystemHealth from './pages/SystemHealth'

export default function App() {
  return (
    <div style={{ display: 'flex', height: '100vh', width: '100vw', overflow: 'hidden' }}>
      <Sidebar />
      <main style={{ flex: 1, overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/jobs" element={<JobExplorer />} />
          <Route path="/dags" element={<DagVisualizer />} />
          <Route path="/submit" element={<SubmitJob />} />
          <Route path="/health" element={<SystemHealth />} />
        </Routes>
      </main>
    </div>
  )
}
