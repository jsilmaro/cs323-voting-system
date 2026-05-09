# System Architecture
## Data Flow
1. Each **edge node** generates votes with a unique user_id and sends them via HTTP POST to the Render API
2. The **Render API** (Flask) validates each vote and publishes it to GCP Pub/Sub (vote-topic)
3. **GCP Pub/Sub** buffers messages asynchronously — decoupling ingestion from processing
4. The **Worker Service** subscribes to vote-sub, deduplicates using `user_id_poll_id` as doc ID, and writes to Firestore
5. **Firestore** stores the final processed votes in the `votes` collection
