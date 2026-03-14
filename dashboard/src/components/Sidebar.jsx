import { NavLink } from 'react-router-dom'
import {
  LayoutDashboard,
  List,
  GitFork,
  Send,
  Activity,
  Cpu,
} from 'lucide-react'

const links = [
  { to: '/',       icon: LayoutDashboard, label: 'Dashboard'   },
  { to: '/jobs',   icon: List,            label: 'Jobs'        },
  { to: '/dags',   icon: GitFork,         label: 'DAG Planner' },
  { to: '/submit', icon: Send,            label: 'Submit'      },
  { to: '/health', icon: Activity,        label: 'Health'      },
]

export default function Sidebar() {
  return (
    <nav style={{
      width: 200,
      minWidth: 200,
      background: 'var(--bg-base)',
      borderRight: '1px solid var(--border)',
      display: 'flex',
      flexDirection: 'column',
      padding: '0',
      flexShrink: 0,
    }}>
      {/* Logo */}
      <div style={{
        padding: '20px 16px 16px',
        borderBottom: '1px solid var(--border)',
        display: 'flex',
        alignItems: 'center',
        gap: 10,
      }}>
        <div style={{
          width: 28, height: 28,
          background: 'var(--accent)',
          borderRadius: 3,
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          flexShrink: 0,
        }}>
          <Cpu size={15} color="#000" />
        </div>
        <div>
          <div style={{ fontFamily: 'var(--font-display)', fontWeight: 700, fontSize: 13, color: 'var(--text-primary)', letterSpacing: '0.02em' }}>
            SimEngine
          </div>
          <div style={{ fontSize: 9, color: 'var(--text-muted)', letterSpacing: '0.15em', textTransform: 'uppercase' }}>
            Mission Control
          </div>
        </div>
      </div>

      {/* Nav links */}
      <div style={{ flex: 1, padding: '12px 8px', display: 'flex', flexDirection: 'column', gap: 2 }}>
        {links.map(({ to, icon: Icon, label }) => (
          <NavLink
            key={to}
            to={to}
            end={to === '/'}
            style={({ isActive }) => ({
              display: 'flex',
              alignItems: 'center',
              gap: 10,
              padding: '8px 10px',
              borderRadius: 3,
              textDecoration: 'none',
              fontSize: 12,
              fontWeight: isActive ? 600 : 400,
              letterSpacing: '0.04em',
              color: isActive ? 'var(--accent)' : 'var(--text-secondary)',
              background: isActive ? 'var(--accent-dim)' : 'transparent',
              border: isActive ? '1px solid rgba(245,158,11,0.2)' : '1px solid transparent',
              transition: 'all 0.15s',
            })}
          >
            {({ isActive }) => (
              <>
                <Icon size={14} color={isActive ? 'var(--accent)' : 'var(--text-muted)'} />
                {label}
              </>
            )}
          </NavLink>
        ))}
      </div>

      {/* Footer */}
      <div style={{
        padding: '12px 16px',
        borderTop: '1px solid var(--border)',
        fontSize: 9,
        color: 'var(--text-muted)',
        letterSpacing: '0.1em',
        textTransform: 'uppercase',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <div className="live-dot" />
          Live — 2s poll
        </div>
      </div>
    </nav>
  )
}
