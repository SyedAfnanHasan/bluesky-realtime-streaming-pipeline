
import { useState, useEffect } from 'react'


/**
 * Displays processing accuracy metrics received from the WebSocket server.
 *
 * Shows the success rate, successfully processed events,
 * failed events, and total attempted events.
 */
function Accuracy() {

  const [accuracy, setAccuracy] = useState(null)

  useEffect(() => {

    // Connect to the event metrics WebSocket server
    const ws = new WebSocket('ws://localhost:8765')

    ws.onopen = () => {
      console.log('Connected to Accuracy WebSocket')
    }

    // Listen for processing accuracy updates
    ws.onmessage = (event) => {

      const data = JSON.parse(event.data)

      if (data.type === 'accuracy_metrics') {
        setAccuracy(data)
      }

    }

    ws.onerror = (error) => {
      console.error('Accuracy WebSocket error:', error)
    }

    // Close the WebSocket connection when the component unmounts
    return () => {
      ws.close()
    }

  }, [])

  return (

    <div className="accuracy-card">

      <h3>Processing Accuracy</h3>

      {accuracy !== null ? (

        <>
          <div className="accuracy-value">
            {accuracy.success_rate}
            <span>%</span>
          </div>

          <div className="accuracy-details">
            <p>
              Successfully processed:
              <strong>{accuracy.successfully_processed.toLocaleString()}</strong>
            </p>

            <p>
              Failed events:
              <strong>{accuracy.failed_events.toLocaleString()}</strong>
            </p>

            <p>
              Total attempted:
              <strong>{accuracy.total_attempted.toLocaleString()}</strong>
            </p>
          </div>
        </>

      ) : (

        <div className="accuracy-value">Waiting...</div>

      )}

    </div>

  )

}

export default Accuracy
