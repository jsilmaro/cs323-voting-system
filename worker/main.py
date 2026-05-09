import json
import os
import base64
import time
import threading
from flask import Flask, jsonify
from google.cloud import pubsub_v1, firestore
from google.oauth2 import service_account

app = Flask(__name__)

PROJECT_ID = "lustrous-baton-495804-r7"
SUBSCRIPTION_ID = "vote-sub"
DATABASE_ID = "voting-system-database"

print("[Worker] Initializing credentials...")
sa_key_b64 = os.environ.get("GCP_SA_KEY")
if sa_key_b64:
    print("[Worker] GCP_SA_KEY found, loading credentials...")
    try:
        sa_info = json.loads(base64.b64decode(sa_key_b64).decode("utf-8"))
        credentials = service_account.Credentials.from_service_account_info(sa_info)
        db = firestore.Client(project=PROJECT_ID, credentials=credentials, database=DATABASE_ID)
        subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
        print("[Worker] Credentials loaded successfully!")
    except Exception as e:
        print(f"[Worker] ERROR loading credentials: {e}")
        raise
else:
    print("[Worker] No GCP_SA_KEY found, using default credentials...")
    db = firestore.Client(project=PROJECT_ID, database=DATABASE_ID)
    subscriber = pubsub_v1.SubscriberClient()

subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
print(f"[Worker] Subscription path: {subscription_path}")

votes_processed = 0
votes_duplicate = 0
votes_errored = 0

def process_vote(message):
    global votes_processed, votes_duplicate, votes_errored
    try:
        vote = json.loads(message.data.decode("utf-8"))
        print(f"[Worker] Received: {vote.get('user_id')} | Choice: {vote.get('choice')}")

        required = {"user_id", "poll_id", "choice"}
        if not required.issubset(vote.keys()):
            print("[Worker] Malformed message, discarding.")
            message.ack()
            return

        doc_id = f"{vote['user_id']}_{vote['poll_id']}"
        vote["id"] = doc_id
        vote["processed_at"] = time.time()

        doc_ref = db.collection("votes").document(doc_id)
        if doc_ref.get().exists:
            votes_duplicate += 1
            print(f"[Worker] Duplicate skipped: {doc_id} | Total dupes: {votes_duplicate}")
        else:
            doc_ref.set(vote)
            votes_processed += 1
            print(f"[Worker] Stored: {doc_id} | Total: {votes_processed}")

        message.ack()

    except json.JSONDecodeError as e:
        print(f"[Worker] JSON error: {e}. Discarding.")
        message.ack()
    except Exception as e:
        votes_errored += 1
        print(f"[Worker] Error: {e}. Will retry.")
        message.nack()

def run_worker():
    try:
        print("[Worker] Starting Pub/Sub listener...")
        streaming_pull_future = subscriber.subscribe(subscription_path, callback=process_vote)
        print("[Worker] Listening for messages...")
        streaming_pull_future.result()
    except Exception as e:
        print(f"[Worker] FATAL ERROR in worker thread: {e}")
        import traceback
        traceback.print_exc()

# Start worker thread at module level
print("[Worker] Spawning background thread...")
worker_thread = threading.Thread(target=run_worker, daemon=True)
worker_thread.start()
print("[Worker] Background thread started!")

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "thread_alive": worker_thread.is_alive(),
        "processed": votes_processed,
        "duplicates": votes_duplicate,
        "errors": votes_errored
    }), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
