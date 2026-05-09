# CS323 Distributed Voting System

## System Overview
A distributed voting system built on GCP where multiple edge nodes generate votes and cloud services process and aggregate them. The system remains functional even when failures occur.

## Architecture

## Components
- **Edge Nodes** — Python scripts simulating distributed vote generation with retry logic
- **Render API** — Flask app deployed on Render receiving votes via HTTP POST and publishing to Pub/Sub
- **GCP Pub/Sub** — Message queue decoupling ingestion from processing (topic: vote-topic, subscription: vote-sub)
- **Worker Service** — Python service subscribing to Pub/Sub, deduplicating and writing to Firestore
- **Firestore** — GCP NoSQL database storing final processed votes (database: voting-system-database)

## Deployed API Endpoint
https://cs323-api.onrender.com

## Setup Instructions

### 1. GCP Setup
```bash
gcloud config set project lustrous-baton-495804-r7
gcloud pubsub topics create vote-topic
gcloud pubsub subscriptions create vote-sub --topic=vote-topic --ack-deadline=60
```

### 2. Clone Repository
```bash
git clone https://github.com/jsilmaro/cs323-voting-system.git
cd cs323-voting-system
```

### 3. Run Worker
```bash
cd worker
pip install -r requirements.txt
python main.py
```

### 4. Run Edge Node
```bash
cd edge_node
pip install requests
API_URL=https://cs323-api.onrender.com/vote python edge_node.py
```

### 5. Simulate Duplicates
```bash
API_URL=https://cs323-api.onrender.com/vote python edge_node.py --duplicate
```
### 6. Run Edge Node in Burst Mode
```bash
API_URL=https://cs323-api.onrender.com/vote python edge_node.py --burst
```

Burst mode simulates a sudden spike of votes from the edge node, sending multiple votes rapidly in succession before pausing. This reflects real-world scenarios such as a surge in votes near the end of a poll period.

You can configure burst behavior using environment variables:

| Variable | Default | Description |
|---|---|---|
| `BURST_SIZE` | `5` | Number of votes sent per burst |
| `BURST_INTERVAL` | `10.0` | Seconds to wait between bursts |

## Fault Tolerance Testing Results

### Normal Operation
- Edge node sent votes continuously, all received Status 200
- Worker processed and stored votes in Firestore in real time

### Duplicate Simulation
- Edge node sent each vote twice using --duplicate flag
- Worker correctly detected and skipped duplicates using doc_id = user_id + poll_id
- Idempotency confirmed: duplicate votes did not create multiple Firestore documents

### Worker Failure & Recovery
- Worker stopped while edge node continued sending votes
- Firestore count frozen at 320 — no new documents added
- Pub/Sub buffered all incoming messages during downtime
- Worker restarted and automatically processed all queued messages
- Firestore count jumped from 320 to 1,062 — zero data loss

## Individual Reflection
### Janelle Silmaro
Implementing this distributed voting system gave me a hands-on understanding of how real-world distributed systems handle failures and maintain consistency. During normal operation, I observed how the edge node, API, Pub/Sub, and worker each played independent roles — the system was not a single program but a pipeline of loosely coupled services. This was very different from sequential execution where each step waits for the previous one to finish.

One of the most insightful moments was during fault injection testing. When the worker was stopped, the API continued accepting votes and Pub/Sub continued buffering messages without any system-wide failure. Firestore simply stopped updating, which demonstrated how distributed systems isolate failures to specific components rather than crashing entirely. When the worker was restored, it automatically processed over 700 queued messages in a batch, showing how message persistence enables recovery without manual intervention.

I also observed the importance of idempotency. During duplicate simulation, the same vote was sent twice, but the worker correctly identified duplicates using the combined user_id and poll_id as the document ID, preventing double-counting. This showed how design decisions at the data layer directly affect system correctness under real distributed conditions.

The main challenge I encountered was GCP configuration — particularly Firestore not using the default database name, which caused 24,000+ errors before being resolved by explicitly specifying the database ID. This experience reinforced that distributed systems require careful coordination across components, and that small configuration mismatches can have large cascading effects.

# Hannah Gentrolizo
This activity helped me understand how distributed systems work in real-world scenarios. Unlike traditional programs, this system runs across multiple components like edge nodes, Cloud Run, Pub/Sub, and Firestore, which communicate asynchronously. I learned that systems don’t always process data immediately, but instead rely on messaging to handle tasks efficiently.

I also realized the importance of fault tolerance. Even when the worker service was down, the system continued to accept votes because Pub/Sub buffered the messages. Once the worker recovered, it processed everything automatically. This showed me how distributed systems are designed to handle failures without stopping the entire system.

### Charesh Angeline Rapirap
Working on the edge node part of this activity helped me understand how the different components of a distributed system work together. My contribution was adding burst mode to the edge node, which sends multiple votes rapidly in a short period to simulate a sudden spike in activity, similar to what might happen when many users vote at the same time near the end of a poll.

Following the structure of the existing code and extending it taught me how edge nodes are responsible for generating and sending data to the cloud, and that they can behave unpredictably in real scenarios. I observed that even when votes were sent in bursts, the API still accepted them and the rest of the pipeline continued working normally, which showed me that each component handles its own part independently.

I also got a better understanding of why retry logic matters. When a vote fails to send, the edge node tries again instead of just stopping, which helps make sure votes are not lost due to temporary network issues. Overall, this activity gave me a clearer picture of how distributed systems are designed to keep working even when things do not go perfectly.
## System Architecture Diagram
┌─────────────────┐     HTTP POST      ┌─────────────────┐     publish      ┌─────────────────┐
│   Edge Node 1   │ ─────────────────► │                 │ ───────────────► │                 │
├─────────────────┤                    │   Render API    │                  │  GCP Pub/Sub    │
│   Edge Node 2   │ ─────────────────► │  (Flask app)    │                  │  vote-topic     │
├─────────────────┤                    │                 │                  │  vote-sub       │
│   Edge Node 3   │ ─────────────────► │                 │                  │                 │
└─────────────────┘                    └─────────────────┘                  └────────┬────────┘
retry + idempotency                   validates votes                               │ subscribe
publishes to topic                            │
▼
┌─────────────────────┐
│   Render Worker     │
│  dedup by doc_id    │
│  user_id + poll_id  │
└──────────┬──────────┘
│ set()
▼
┌─────────────────────┐
│      Firestore      │
│  voting-system-db   │
│  votes collection   │
└─────────────────────┘
Note: Cloud Run was unavailable due to billing restrictions.
Render.com was used as an equivalent alternative for deploying
both the API and Worker services.

## ⚠️ Known Limitations

### Firestore Read Quota
The Firestore free tier allows **50,000 reads per day**. The dashboard fetches vote documents to display statistics, which consumes read quota. 

**What caused quota exhaustion during development:**
- Running `watch -n 3` with a Firestore query that downloaded all 2880+ documents every 3 seconds
- This burned through the entire daily quota in under 1 minute (2880 reads × 20/min = 57,600 reads/min)

**How to avoid:**
- Never use `watch` with Firestore queries
- Always use `limit()` when querying large collections
- Run count scripts manually only when needed

**If quota is exceeded:**
- The dashboard will show 0 votes and empty feed
- Votes are still being accepted and stored correctly — only the *display* is affected
- Quota resets automatically every 24 hours at midnight Pacific Time (~3–4 PM Philippine Time)
- The pipeline (Edge Node → API → Pub/Sub → Worker → Firestore) continues working normally regardless of read quota

## Live Dashboard
🌐 **https://cs323-voting-system-8547.onrender.com/**

Interactive dashboard showing:
- Real-time vote distribution (A, B, C)
- Submit a test vote directly from the browser
- Recent votes live feed
- System status (API, Worker, Firestore, Pub/Sub)

> Note: Dashboard reads from Firestore — if quota is exceeded, vote counts will show 0 but the pipeline still works. Quota resets daily.
