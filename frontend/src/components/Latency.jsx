
import { useState, useEffect } from 'react'


/**
 * Displays the latest sampled event processing latency.
 *
 * Receives latency measurements in milliseconds from the
 * event processor through a WebSocket connection.
 */
function Latency() {

  const [latency, setLatency] = useState(null)

  useEffect(() => {

    // Connect to the event metrics WebSocket server
    const ws = new WebSocket('ws://localhost:8765')

    ws.onopen = () => {
      console.log('Connected to Latency WebSocket')
    }

    // Listen for latency updates
    ws.onmessage = (event) => {

      const data = JSON.parse(event.data)

      if (data.type === 'latency_metrics') {
        setLatency(data.latency_ms)
      }

    }

    ws.onerror = (error) => {
      console.error('Latency WebSocket error:', error)
    }

    // Close the WebSocket connection when the component unmounts
    return () => {
      ws.close()
    }

  }, [])

  return (

    <div className="latency-card">

      <h3>Event Processing Latency</h3>

      <div className="latency-value">

        {latency !== null ? (
          <>
            {latency}
            <span> ms</span>
          </>
        ) : (
          'Waiting...'
        )}

      </div>

      <p>Latest sampled event</p>

    </div>

  )

}

export default Latency
