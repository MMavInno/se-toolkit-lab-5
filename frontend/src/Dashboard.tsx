import { useState, useEffect, useReducer, FormEvent } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  LineElement,
  PointElement,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  LineElement,
  PointElement
)

const STORAGE_KEY = 'api_key'

interface ScoreBucket {
  bucket: string
  count: number
}

interface PassRate {
  task: string
  avg_score: number
  attempts: number
}

interface TimelineEntry {
  date: string
  submissions: number
}

interface LabItem {
  id: number
  type: string
  title: string
}

type FetchState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; scores: ScoreBucket[]; passRates: PassRate[]; timeline: TimelineEntry[] }
  | { status: 'error'; message: string }

type FetchAction =
  | { type: 'fetch_start' }
  | { type: 'fetch_success'; scores: ScoreBucket[]; passRates: PassRate[]; timeline: TimelineEntry[] }
  | { type: 'fetch_error'; message: string }

function fetchReducer(_state: FetchState, action: FetchAction): FetchState {
  switch (action.type) {
    case 'fetch_start':
      return { status: 'loading' }
    case 'fetch_success':
      return { status: 'success', scores: action.scores, passRates: action.passRates, timeline: action.timeline }
    case 'fetch_error':
      return { status: 'error', message: action.message }
  }
}

interface DashboardProps {
  token: string
  onBack: () => void
}

function Dashboard({ token, onBack }: DashboardProps) {
  const [selectedLab, setSelectedLab] = useState<string>('lab-04')
  const [labs, setLabs] = useState<LabItem[]>([])
  const [fetchState, dispatch] = useReducer(fetchReducer, { status: 'idle' })

  useEffect(() => {
    fetch('/items/', {
      headers: { Authorization: `Bearer ${token}` },
    })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json()
      })
      .then((data: LabItem[]) => {
        const labItems = data.filter((item) => item.type === 'lab')
        setLabs(labItems)
        if (labItems.length > 0 && !selectedLab) {
          const firstLabId = labItems[0].title.toLowerCase().replace('lab ', 'lab-').split(' ')[0]
          setSelectedLab(firstLabId)
        }
      })
      .catch(() => {})
  }, [token])

  useEffect(() => {
    if (!token || !selectedLab) return

    dispatch({ type: 'fetch_start' })

    const fetchAnalytics = async () => {
      try {
        const headers = { Authorization: `Bearer ${token}` }

        const [scoresRes, passRatesRes, timelineRes] = await Promise.all([
          fetch(`/analytics/scores?lab=${selectedLab}`, { headers }),
          fetch(`/analytics/pass-rates?lab=${selectedLab}`, { headers }),
          fetch(`/analytics/timeline?lab=${selectedLab}`, { headers }),
        ])

        if (!scoresRes.ok) throw new Error(`Scores: HTTP ${scoresRes.status}`)
        if (!passRatesRes.ok) throw new Error(`Pass rates: HTTP ${passRatesRes.status}`)
        if (!timelineRes.ok) throw new Error(`Timeline: HTTP ${timelineRes.status}`)

        const scores = await scoresRes.json()
        const passRates = await passRatesRes.json()
        const timeline = await timelineRes.json()

        dispatch({ type: 'fetch_success', scores, passRates, timeline })
      } catch (err) {
        dispatch({ type: 'fetch_error', message: err instanceof Error ? err.message : 'Unknown error' })
      }
    }

    fetchAnalytics()
  }, [token, selectedLab])

  const scoresChartData = {
    labels: fetchState.status === 'success' ? fetchState.scores.map((s) => s.bucket) : [],
    datasets: [
      {
        label: 'Number of Submissions',
        data: fetchState.status === 'success' ? fetchState.scores.map((s) => s.count) : [],
        backgroundColor: 'rgba(54, 162, 235, 0.6)',
        borderColor: 'rgba(54, 162, 235, 1)',
        borderWidth: 1,
      },
    ],
  }

  const timelineChartData = {
    labels: fetchState.status === 'success' ? fetchState.timeline.map((t) => t.date) : [],
    datasets: [
      {
        label: 'Submissions',
        data: fetchState.status === 'success' ? fetchState.timeline.map((t) => t.submissions) : [],
        borderColor: 'rgba(75, 192, 192, 1)',
        backgroundColor: 'rgba(75, 192, 192, 0.2)',
        tension: 0.1,
        fill: true,
      },
    ],
  }

  const handleLabChange = (e: FormEvent<HTMLSelectElement>) => {
    setSelectedLab(e.currentTarget.value)
  }

  return (
    <div>
      <header className="app-header">
        <h1>Analytics Dashboard</h1>
        <button className="btn-disconnect" onClick={onBack}>
          Back to Items
        </button>
      </header>

      <div className="dashboard-controls">
        <label>Select Lab: </label>
        <select value={selectedLab} onChange={handleLabChange}>
          {labs.map((lab) => {
            const labId = lab.title.toLowerCase().replace('lab ', 'lab-').split(' ')[0]
            return (
              <option key={lab.id} value={labId}>
                {lab.title}
              </option>
            )
          })}
          <option value="lab-01">Lab 01</option>
          <option value="lab-02">Lab 02</option>
          <option value="lab-03">Lab 03</option>
          <option value="lab-04">Lab 04</option>
        </select>
      </div>

      {fetchState.status === 'loading' && <p>Loading analytics...</p>}
      {fetchState.status === 'error' && <p>Error: {fetchState.message}</p>}

      {fetchState.status === 'success' && (
        <div className="dashboard-charts">
          <div className="chart-container">
            <h2>Score Distribution</h2>
            <Bar
              data={scoresChartData}
              options={{
                responsive: true,
                plugins: {
                  legend: { position: 'top' as const },
                  title: { display: true, text: 'Score Distribution by Bucket' },
                },
              }}
            />
          </div>

          <div className="chart-container">
            <h2>Submissions Over Time</h2>
            <Line
              data={timelineChartData}
              options={{
                responsive: true,
                plugins: {
                  legend: { position: 'top' as const },
                  title: { display: true, text: 'Daily Submissions' },
                },
              }}
            />
          </div>

          <div className="chart-container">
            <h2>Pass Rates by Task</h2>
            <table>
              <thead>
                <tr>
                  <th>Task</th>
                  <th>Avg Score</th>
                  <th>Attempts</th>
                </tr>
              </thead>
              <tbody>
                {fetchState.passRates.map((pr) => (
                  <tr key={pr.task}>
                    <td>{pr.task}</td>
                    <td>{pr.avg_score}%</td>
                    <td>{pr.attempts}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}

export default Dashboard
