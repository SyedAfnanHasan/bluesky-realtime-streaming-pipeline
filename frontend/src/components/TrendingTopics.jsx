
import { useState, useEffect } from 'react'


/**
 * Displays the latest trending topics received from the Bluesky trends processor.
 *
 * Receives trend snapshots through a WebSocket connection and displays
 * their rank, topic name, post count, category, and status in a table.
 */
function TrendingTopics() {

  const [trends, setTrends] = useState([])

  useEffect(() => {

    // Connect to the trends WebSocket server
    const ws = new WebSocket('ws://localhost:8766')

    ws.onopen = () => {
      console.log('Connected to Trends WebSocket')
    }

    // Listen for incoming trend snapshots
    ws.onmessage = (event) => {

      const data = JSON.parse(event.data)

      if (data.type === 'trend_snapshot') {
        setTrends(data.trends)
      }

    }

    ws.onerror = (error) => {
      console.error('Trends WebSocket error:', error)
    }

    ws.onclose = () => {
      console.log('Trends WebSocket disconnected')
    }

    // Close the connection when the component unmounts
    return () => ws.close()

  }, [])

  return (
    <section className="dashboard-section">

      <div className="section-heading">
        <div>
          <h2>Trending Topics</h2>
          <p>Latest Bluesky trending topics snapshot</p>
        </div>

        <span className="topics-count">
          {trends.length} topics
        </span>
      </div>

      {trends.length === 0 ? (

        <div className="empty-state">
          Waiting for trending topics...
        </div>

      ) : (

        <div className="table-wrapper">

          <table className="trends-table">

            <thead>
              <tr>
                <th>Rank</th>
                <th>Topic</th>
                <th>Post Count</th>
                <th>Category</th>
                <th>Status</th>
              </tr>
            </thead>

            <tbody>

              {trends.map((trend) => (

                <tr key={trend.rank}>

                  <td>
                    <span className="rank-badge">
                      #{trend.rank}
                    </span>
                  </td>

                  <td className="topic-name">
                    {trend.topic_name}
                  </td>

                  <td className="post-count">
                    {Number(trend.post_count).toLocaleString()}
                  </td>

                  <td>
                    {trend.category || '—'}
                  </td>

                  <td>
                    <span className="status-badge">
                      {trend.status || '—'}
                    </span>
                  </td>

                </tr>

              ))}

            </tbody>

          </table>

        </div>

      )}

    </section>
  )
}

export default TrendingTopics
