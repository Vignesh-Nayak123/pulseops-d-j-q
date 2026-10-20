import { useCallback, useEffect, useState } from 'react'
import {
  Activity,
  ArrowDown,
  ArrowUp,
  Check,
  CheckCircle2,
  ChevronDown,
  CircleAlert,
  Clock3,
  Command,
  ExternalLink,
  LoaderCircle,
  Plus,
  RefreshCw,
  Send,
  Server,
  Settings2,
  Terminal,
  X,
} from 'lucide-react'
import './App.css'

const API = import.meta.env.VITE_API_URL || 'http://localhost:8000'

type Job = {
  id: string
  type: string
  status: string
  priority?: string
  payload?: Record<string, unknown>
  attempts?: number
  last_error?: string
  created_at?: number
  completed_at?: number
}

type Stats = {
  completed: number
  dead: number
  enqueued: number
  failed: number
  inflight: number
  ready_total: number
  scheduled: number
  ready?: Record<string, number>
}

function formatTime(timestamp?: number) {
  if (!timestamp) return 'Just now'
  return new Date(timestamp * 1000).toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
  })
}

function shortId(id: string) {
  return `${id.slice(0, 8)}…${id.slice(-4)}`
}

function App() {
  const [stats, setStats] = useState<Stats | null>(null)
  const [jobs, setJobs] = useState<Job[]>([])
  const [taskType, setTaskType] = useState('sleep')
  const [priority, setPriority] = useState('default')
  const [seconds, setSeconds] = useState('2')
  const [failRate, setFailRate] = useState('0.3')
  const [activeView, setActiveView] = useState('New job')
  const [jobId, setJobId] = useState('')
  const [selectedJob, setSelectedJob] = useState<Job | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  const [connected, setConnected] = useState(false)
  const [refreshing, setRefreshing] = useState(false)

  const refresh = useCallback(async (quiet = false) => {
    if (!quiet) setRefreshing(true)
    try {
      const response = await fetch(`${API}/stats`)
      if (!response.ok) throw new Error('API is unavailable')
      const data = (await response.json()) as Stats
      setStats(data)
      setConnected(true)
    } catch {
      setConnected(false)
    } finally {
      setRefreshing(false)
    }
  }, [])

  useEffect(() => {
    void refresh()
    const timer = window.setInterval(() => void refresh(true), 3000)
    return () => window.clearInterval(timer)
  }, [refresh])

  async function submitJob(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setLoading(true)
    setError('')
    setNotice('')
    setSelectedJob(null)

    const payload =
      taskType === 'sleep'
        ? { seconds: Math.max(0, Number(seconds) || 0) }
        : { fail_rate: Math.min(1, Math.max(0, Number(failRate) || 0)) }

    try {
      const response = await fetch(`${API}/enqueue`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: taskType, payload, priority }),
      })

      const data = await response.json()
      if (!response.ok) {
        throw new Error(data.error || data.message || 'Could not submit job')
      }

      setJobId(data.id)
      setNotice('Job submitted successfully.')
      setActiveView('Job history')

      const jobResponse = await fetch(`${API}/jobs/${data.id}`)
      if (jobResponse.ok) {
        const job = (await jobResponse.json()) as Job
        setSelectedJob(job)
        setJobs((previous) => [
          job,
          ...previous.filter((item) => item.id !== job.id),
        ])
      }
      await refresh(true)
    } catch (cause) {
      setError(
        cause instanceof Error
          ? `${cause.message}. Check that the API is running and CORS allows localhost:5173.`
          : 'Could not connect to the API.',
      )
    } finally {
      setLoading(false)
    }
  }

  async function lookupJob(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!jobId.trim()) return

    setError('')
    setNotice('')
    try {
      const response = await fetch(
        `${API}/jobs/${encodeURIComponent(jobId.trim())}`,
      )
      const data = await response.json()
      if (!response.ok) throw new Error(data.error || 'Job not found')

      const job = data as Job
      setSelectedJob(job)
      setJobs((previous) => [
        job,
        ...previous.filter((item) => item.id !== job.id),
      ])
      setNotice('Job status refreshed.')
      setActiveView('Job history')
    } catch (cause) {
      setError(
        cause instanceof Error
          ? cause.message
          : 'Could not retrieve this job.',
      )
    }
  }

  async function refreshSelectedJob() {
    if (!selectedJob) return
    try {
      const response = await fetch(`${API}/jobs/${selectedJob.id}`)
      if (!response.ok) throw new Error('Could not refresh job')
      const job = (await response.json()) as Job
      setSelectedJob(job)
      setJobs((previous) => [
        job,
        ...previous.filter((item) => item.id !== job.id),
      ])
    } catch {
      setError('Could not refresh this job. Check the API connection.')
    }
  }

  const navigation = [
    { label: 'New job', icon: Plus },
    { label: 'Job history', icon: Clock3 },
    { label: 'Workers & queue', icon: Activity },
  ]

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <button
          className="brand"
          onClick={() => {
            setActiveView('New job')
            setError('')
            setNotice('')
          }}
        >
          <span className="brand-mark">
            <Activity size={19} strokeWidth={2.3} />
          </span>
          <span>PulseOps</span>
        </button>

        <div className="sidebar-label">WORKSPACE</div>
        <nav className="navigation">
          {navigation.map(({ label, icon: Icon }) => (
            <button
              key={label}
              className={`nav-item ${activeView === label ? 'active' : ''}`}
              onClick={() => {
                setActiveView(label)
                setError('')
                setNotice('')
              }}
            >
              <Icon size={17} strokeWidth={1.8} />
              <span>{label}</span>
            </button>
          ))}
        </nav>

        <div className="sidebar-spacer" />

        <div className="sidebar-connection">
          <span className={`connection-dot ${connected ? 'online' : ''}`} />
          <span>{connected ? 'API connected' : 'API disconnected'}</span>
        </div>

        <div className="sidebar-footer">
          <span className="avatar">P</span>
          <div className="footer-copy">
            <strong>PulseOps</strong>
            <span>Local environment</span>
          </div>
          <Settings2 size={17} className="muted-icon" />
        </div>
      </aside>

      <main className="main-area">
        <header className="topbar">
          <div className="breadcrumbs">
            <span>Workspace</span>
            <span className="breadcrumb-divider">/</span>
            <strong>{activeView}</strong>
          </div>
          <div className="topbar-actions">
            <span className={`status-label ${connected ? 'healthy' : ''}`}>
              <span className="connection-dot" />
              {connected ? 'Operational' : 'Disconnected'}
            </span>
            <button
              className="icon-button"
              aria-label="Refresh statistics"
              onClick={() => void refresh()}
              title="Refresh"
            >
              <RefreshCw size={16} className={refreshing ? 'spin' : ''} />
            </button>
          </div>
        </header>

        <div className="content">
          {activeView === 'New job' && (
            <section className="welcome-section">
              <div className="welcome-icon">
                <Command size={22} strokeWidth={1.7} />
              </div>
              <h1>What are we running today?</h1>
              <p className="welcome-subtitle">
                Submit a job, follow its progress, and keep your queue moving.
              </p>

              <form className="composer" onSubmit={submitJob}>
                <div className="composer-heading">
                  <span>New job</span>
                  <span className="composer-hint">Runs asynchronously</span>
                </div>

                <label className="field-label" htmlFor="task-type">
                  Task type
                </label>
                <div className="select-wrap">
                  <select
                    id="task-type"
                    value={taskType}
                    onChange={(event) => setTaskType(event.target.value)}
                  >
                    <option value="sleep">Sleep — simulate a task</option>
                    <option value="flaky">Flaky — test failure handling</option>
                  </select>
                  <ChevronDown size={16} />
                </div>

                <div className="field-row">
                  <div className="field-column">
                    <label className="field-label" htmlFor="priority">
                      Priority
                    </label>
                    <div className="select-wrap">
                      <select
                        id="priority"
                        value={priority}
                        onChange={(event) => setPriority(event.target.value)}
                      >
                        <option value="high">High</option>
                        <option value="default">Default</option>
                        <option value="low">Low</option>
                      </select>
                      <ChevronDown size={16} />
                    </div>
                  </div>
                  <div className="field-column">
                    <label className="field-label" htmlFor="task-value">
                      {taskType === 'sleep' ? 'Duration (seconds)' : 'Failure rate (0–1)'}
                    </label>
                    <input
                      id="task-value"
                      type="number"
                      min={0}
                      max={taskType === 'sleep' ? 60 : 1}
                      step={taskType === 'sleep' ? 0.5 : 0.1}
                      value={taskType === 'sleep' ? seconds : failRate}
                      onChange={(event) =>
                        taskType === 'sleep'
                          ? setSeconds(event.target.value)
                          : setFailRate(event.target.value)
                      }
                      required
                    />
                  </div>
                </div>

                <div className="composer-bottom">
                  <span className="composer-footnote">
                    <Terminal size={14} />
                    Submitted to your local worker queue
                  </span>
                  <button className="submit-button" disabled={loading} type="submit">
                    {loading ? (
                      <LoaderCircle size={16} className="spin" />
                    ) : (
                      <ArrowUp size={17} />
                    )}
                    <span>{loading ? 'Submitting' : 'Run job'}</span>
                  </button>
                </div>
              </form>

              {error && (
                <div className="feedback error-feedback">
                  <CircleAlert size={16} />
                  <span>{error}</span>
                  <button aria-label="Dismiss error" onClick={() => setError('')}>
                    <X size={15} />
                  </button>
                </div>
              )}
              {notice && (
                <div className="feedback success-feedback">
                  <CheckCircle2 size={16} />
                  <span>{notice}</span>
                </div>
              )}

              {selectedJob && (
                <JobResult
                  job={selectedJob}
                  onRefresh={() => void refreshSelectedJob()}
                />
              )}

              <div className="stats-heading">
                <span>Queue overview</span>
                <span className="live-indicator">
                  <span className="connection-dot online" />
                  Updates every 3 sec
                </span>
              </div>
              <StatsGrid stats={stats} />

              <div className="quick-links">
                <button onClick={() => setActiveView('Job history')}>
                  <Clock3 size={16} />
                  Find a job by ID
                  <ArrowDown size={14} className="quick-link-arrow" />
                </button>
                <a href="http://localhost:3000" target="_blank" rel="noreferrer">
                  <Activity size={16} />
                  Open Grafana
                  <ExternalLink size={14} className="quick-link-arrow" />
                </a>
              </div>
            </section>
          )}

          {activeView === 'Job history' && (
            <section className="page-section">
              <div className="page-heading">
                <div>
                  <h1>Job history</h1>
                  <p>Look up a job and inspect its latest status.</p>
                </div>
              </div>

              <form className="lookup-form" onSubmit={lookupJob}>
                <input
                  value={jobId}
                  onChange={(event) => setJobId(event.target.value)}
                  placeholder="Paste a job ID..."
                  aria-label="Job ID"
                  required
                />
                <button type="submit">
                  <Send size={15} />
                  Look up
                </button>
              </form>

              {error && (
                <div className="feedback error-feedback">
                  <CircleAlert size={16} />
                  <span>{error}</span>
                </div>
              )}
              {notice && (
                <div className="feedback success-feedback">
                  <CheckCircle2 size={16} />
                  <span>{notice}</span>
                </div>
              )}

              {selectedJob && (
                <JobResult
                  job={selectedJob}
                  onRefresh={() => void refreshSelectedJob()}
                />
              )}

              <div className="section-divider" />
              <h2 className="subheading">Recently inspected</h2>
              {jobs.length ? (
                <div className="job-list">
                  {jobs.map((job) => (
                    <button
                      className="job-row"
                      key={job.id}
                      onClick={() => {
                        setSelectedJob(job)
                        setJobId(job.id)
                      }}
                    >
                      <span className="job-row-icon">
                        {job.status === 'completed' ? (
                          <Check size={17} />
                        ) : (
                          <Clock3 size={17} />
                        )}
                      </span>
                      <span className="job-row-main">
                        <strong>{job.type}</strong>
                        <span>{shortId(job.id)} · {formatTime(job.created_at)}</span>
                      </span>
                      <StatusBadge status={job.status} />
                    </button>
                  ))}
                </div>
              ) : (
                <div className="empty-state">
                  <Clock3 size={24} />
                  <strong>No inspected jobs yet</strong>
                  <span>Submit a job or look one up by its ID.</span>
                </div>
              )}
            </section>
          )}

          {activeView === 'Workers & queue' && (
            <section className="page-section">
              <div className="page-heading">
                <div>
                  <h1>Workers & queue</h1>
                  <p>A quick view of queue activity from your API.</p>
                </div>
                <button className="secondary-button" onClick={() => void refresh()}>
                  <RefreshCw size={15} />
                  Refresh
                </button>
              </div>

              <StatsGrid stats={stats} large />

              <div className="section-divider" />
              <h2 className="subheading">Queue by priority</h2>
              <div className="priority-list">
                {['high', 'default', 'low'].map((name) => {
                  const count = stats?.ready?.[name] ?? 0
                  const total = Math.max(stats?.ready_total ?? 0, 1)
                  return (
                    <div className="priority-row" key={name}>
                      <div className="priority-label">
                        <span>{name}</span>
                        <strong>{count}</strong>
                      </div>
                      <div className="priority-track">
                        <div
                          className="priority-fill"
                          style={{ width: `${Math.min(100, (count / total) * 100)}%` }}
                        />
                      </div>
                    </div>
                  )
                })}
              </div>

              <div className="monitoring-note">
                <Server size={18} />
                <div>
                  <strong>Detailed observability</strong>
                  <p>Use Prometheus for raw metrics and Grafana for time-series dashboards.</p>
                  <div className="monitoring-links">
                    <a href="http://localhost:9090" target="_blank" rel="noreferrer">Prometheus <ExternalLink size={13} /></a>
                    <a href="http://localhost:3000" target="_blank" rel="noreferrer">Grafana <ExternalLink size={13} /></a>
                  </div>
                </div>
              </div>
            </section>
          )}
        </div>

        <footer className="app-footer">
          <span>PulseOps</span>
          <span>Distributed job processing</span>
          <span className="footer-status">
            <span className={`connection-dot ${connected ? 'online' : ''}`} />
            {connected ? 'Connected to local API' : 'Waiting for API'}
          </span>
        </footer>
      </main>
    </div>
  )
}

function StatsGrid({ stats, large = false }: { stats: Stats | null; large?: boolean }) {
  const metrics = [
    { label: 'Completed', value: stats?.completed, detail: 'Jobs processed', icon: CheckCircle2 },
    { label: 'Waiting', value: stats?.ready_total, detail: 'Ready in queue', icon: Clock3 },
    { label: 'In progress', value: stats?.inflight, detail: 'Currently claimed', icon: Activity },
    { label: 'Dead-lettered', value: stats?.dead, detail: 'Permanently failed', icon: CircleAlert },
  ]

  return (
    <div className={`stats-grid ${large ? 'stats-grid-large' : ''}`}>
      {metrics.map(({ label, value, detail, icon: Icon }) => (
        <div className="metric" key={label}>
          <div className="metric-top">
            <span>{label}</span>
            <Icon size={16} />
          </div>
          <strong>{value === undefined ? '—' : value.toLocaleString()}</strong>
          <span className="metric-detail">{stats ? detail : 'Waiting for API'}</span>
        </div>
      ))}
    </div>
  )
}

function StatusBadge({ status }: { status: string }) {
  return (
    <span className={`status-badge status-${status.toLowerCase()}`}>
      <span className="badge-dot" />
      {status.replaceAll('_', ' ')}
    </span>
  )
}

function JobResult({ job, onRefresh }: { job: Job; onRefresh: () => void }) {
  return (
    <div className="job-result">
      <div className="job-result-heading">
        <div className="job-result-title">
          <span className="result-icon"><Terminal size={17} /></span>
          <div>
            <strong>Job details</strong>
            <span>{shortId(job.id)}</span>
          </div>
        </div>
        <div className="result-actions">
          <StatusBadge status={job.status} />
          <button
            className="icon-button"
            aria-label="Refresh job status"
            title="Refresh job status"
            onClick={onRefresh}
          >
            <RefreshCw size={15} />
          </button>
        </div>
      </div>
      <div className="job-details-grid">
        <div><span>Job type</span><strong>{job.type}</strong></div>
        <div><span>Priority</span><strong>{job.priority ?? 'default'}</strong></div>
        <div><span>Attempts</span><strong>{job.attempts ?? 0}</strong></div>
        <div><span>Created</span><strong>{formatTime(job.created_at)}</strong></div>
      </div>
      {job.last_error && (
        <div className="job-error">
          <CircleAlert size={15} />
          <span>{job.last_error}</span>
        </div>
      )}
      <div className="job-id-copy">
        <span>Full job ID</span>
        <code>{job.id}</code>
        <button
          className="icon-button"
          title="Copy job ID"
          aria-label="Copy job ID"
          onClick={() => void navigator.clipboard.writeText(job.id)}
        >
          <ExternalLink size={14} />
        </button>
      </div>
    </div>
  )
}

export default App
