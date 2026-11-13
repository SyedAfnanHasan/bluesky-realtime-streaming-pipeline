
import { useState, useEffect } from 'react'

import MetricsCards from './components/MetricsCards'
import DatabaseMetrics from './components/DatabaseMetrics'
import TrendingTopics from './components/TrendingTopics'
import Throughput from './components/Throughput'
import Latency from './components/Latency'
import Accuracy from './components/Accuracy'

import './App.css'


/**
 * Main application component for the Bluesky analytics dashboard.
 *
 * Receives live event and database metrics through a WebSocket
 * connection and displays the different dashboard components.
 */
function App() {

  const [metrics, setMetrics] = useState({
    posts: 0,
    likes: 0,
    reposts: 0,
    follows: 0
  })

  const [databaseMetrics, setDatabaseMetrics] = useState(null)

  useEffect(() => {

    // Connect to the event metrics WebSocket server
    const ws = new WebSocket('ws://localhost:8765')

    ws.onopen = () => {
      console.log('Connected to Bluesky WebSocket server')
    }

    // Handle incoming WebSocket messages
    ws.onmessage = (event) => {

      const data = JSON.parse(event.data)

      if (data.type === 'event_metrics') {

        setMetrics({
          posts: data.posts,
          likes: data.likes,
          reposts: data.reposts,
          follows: data.follows
        })

      }

      else if (data.type === 'database_metrics') {

        setDatabaseMetrics({
          avgPosts: data.avg_posts_per_second,
          avgLikes: data.avg_likes_per_second,
          avgReposts: data.avg_reposts_per_second,
          avgFollows: data.avg_follows_per_second
        })

      }

    }

    ws.onerror = (error) => {
      console.error('WebSocket error:', error)
    }

    ws.onclose = () => {
      console.log('WebSocket connection closed')
    }

    // Close the connection when the component unmounts
    return () => {
      ws.close()
    }

  }, [])

  return (
    <main className="dashboard">

      <header className="dashboard-header">
        <div className="header-label">
          <span className="header-dot"></span>
          REAL-TIME DATA PLATFORM
        </div>

        <h1>Bluesky Analytics</h1>

        <p>
          Live activity, database metrics and trending topics
        </p>
      </header>

      <div className="dashboard-content">

        {/* Pipeline throughput */}
        <Throughput />

        {/* Latest sampled event latency */}
        <Latency />

        {/* Processing accuracy */}
        <Accuracy />

        {/* Live event metrics */}
        <MetricsCards
          posts={metrics.posts}
          likes={metrics.likes}
          reposts={metrics.reposts}
          follows={metrics.follows}
        />

        {/* PostgreSQL metrics */}
        <DatabaseMetrics metrics={databaseMetrics} />

        {/* Trending topics */}
        <TrendingTopics />

      </div>

      <footer className="dashboard-footer">
        Bluesky Streaming Analytics
      </footer>

    </main>
  )
}

export default App
