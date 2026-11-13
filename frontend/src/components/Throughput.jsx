
import { useState, useEffect } from 'react'


/**
 * Displays real-time throughput across the Bluesky streaming pipeline.
 *
 * Compares incoming events from the Bluesky producer with events
 * processed by the Kafka consumer.
 */
function Throughput() {

  const [incoming, setIncoming] = useState(null)
  const [processed, setProcessed] = useState(null)

  useEffect(() => {

    // Incoming throughput from Bluesky producer
    const incomingWS = new WebSocket('ws://localhost:8767')

    incomingWS.onmessage = (event) => {

      const data = JSON.parse(event.data)

      if (data.type === 'incoming_throughput') {
        setIncoming(data.events_per_second)
      }
    }

    incomingWS.onerror = (error) => {
      console.error('Incoming throughput WebSocket error:', error)
    }


    // Processing throughput from Kafka consumer
    const processingWS = new WebSocket('ws://localhost:8765')

    processingWS.onmessage = (event) => {

      const data = JSON.parse(event.data)

      if (data.type === 'processing_throughput') {
        setProcessed(data.events_per_second)
      }
    }

    processingWS.onerror = (error) => {
      console.error('Processing throughput WebSocket error:', error)
    }


    // Close both connections when component unmounts
    return () => {
      incomingWS.close()
      processingWS.close()
    }

  }, [])


  return (
    <section className="throughput-section">

      <div className="section-heading">
        <div>
          <h2>Pipeline Throughput</h2>
          <p>Real-time event flow across the pipeline</p>
        </div>

        <span className="live-badge">LIVE</span>
      </div>

      <div className="throughput-grid">

        <div className="throughput-card incoming-card">
          <span className="throughput-label">
            Incoming Throughput
          </span>

          <h3>
            {incoming !== null ? incoming.toFixed(2) : '--'}
          </h3>

          <span className="throughput-unit">
            Events / second
          </span>

          <p>Bluesky → Kafka Producer</p>
        </div>


        <div className="throughput-card processing-card">
          <span className="throughput-label">
            Processing Throughput
          </span>

          <h3>
            {processed !== null ? processed.toFixed(2) : '--'}
          </h3>

          <span className="throughput-unit">
            Events / second
          </span>

          <p>Kafka → Python Processor</p>
        </div>

      </div>

    </section>
  )
}

export default Throughput
