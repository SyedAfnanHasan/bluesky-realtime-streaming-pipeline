
/**
 * Displays live Bluesky activity metrics.
 *
 * Shows the incoming number of posts, likes, reposts, and follows
 * per second using individual metric cards.
 */
function MetricsCards({ posts, likes, reposts, follows }) {

  // Prepare the incoming event metrics for display
  const metrics = [
    { label: 'Posts', value: posts, icon: '✍️' },
    { label: 'Likes', value: likes, icon: '♥' },
    { label: 'Reposts', value: reposts, icon: '↗' },
    { label: 'Follows', value: follows, icon: '♙' }
  ]

  return (
    <section className="dashboard-section">

      <div className="section-heading">
        <div>
          <h2>Live Activity</h2>
          <p>Incoming Bluesky events per second</p>
        </div>

        <span className="live-badge">
          <span className="live-dot"></span>
          LIVE
        </span>
      </div>

      <div className="metrics-grid">

        {metrics.map((metric) => (

          <div className="metric-card" key={metric.label}>

            <div className="metric-card-top">
              <span className="metric-label">
                {metric.label}
              </span>

              <span className="metric-icon">
                {metric.icon}
              </span>
            </div>

            <div className="metric-value">
              {Number(metric.value).toLocaleString()}
            </div>

            <div className="metric-unit">
              events / sec
            </div>

          </div>

        ))}

      </div>

    </section>
  )
}

export default MetricsCards
