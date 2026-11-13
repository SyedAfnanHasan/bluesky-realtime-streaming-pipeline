
/**
 * Displays average event processing metrics retrieved from PostgreSQL.
 *
 * Shows the average posts, likes, reposts, and follows per second
 * over the last 60-second interval.
 */
function DatabaseMetrics({ metrics }) {

  // Prepare the database metrics for display
  const databaseCards = [
    { label: 'Average Posts', value: metrics?.avgPosts },
    { label: 'Average Likes', value: metrics?.avgLikes },
    { label: 'Average Reposts', value: metrics?.avgReposts },
    { label: 'Average Follows', value: metrics?.avgFollows }
  ]

  return (
    <section className="dashboard-section">

      <div className="section-heading">
        <div>
          <h2>PostgreSQL Averages</h2>
          <p>Average events per second over the last 60-second interval</p>
        </div>

        <span className="database-badge">
          <span className="database-dot"></span>
          DATABASE
        </span>
      </div>

      {!metrics ? (

        <div className="empty-state">
          Waiting for the first database update...
        </div>

      ) : (

        <div className="metrics-grid">

          {databaseCards.map((metric) => (

            <div className="metric-card database-card" key={metric.label}>

              <div className="metric-card-top">
                <span className="metric-label">
                  {metric.label}
                </span>
              </div>

              <div className="metric-value">
                {Number(metric.value).toLocaleString()}
              </div>

              <div className="metric-unit">
                average events / sec
              </div>

            </div>

          ))}

        </div>

      )}

    </section>
  )
}

export default DatabaseMetrics
