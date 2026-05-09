import json
import os
import base64
import time
import threading
from flask import Flask, jsonify

app = Flask(__name__)

PROJECT_ID = "lustrous-baton-495804-r7"
SUBSCRIPTION_ID = "vote-sub"
DATABASE_ID = "voting-system-database"

votes_processed = 0
votes_duplicate = 0
votes_errored = 0
worker_thread = None

def get_credentials():
    sa_key_b64 = os.environ.get("GCP_SA_KEY")
    if sa_key_b64:
        from google.oauth2 import service_account
        sa_info = json.loads(base64.b64decode(sa_key_b64).decode("utf-8"))
        return service_account.Credentials.from_service_account_info(sa_info)
    return None

def run_worker():
    global votes_processed, votes_duplicate, votes_errored
    from google.cloud import pubsub_v1, firestore

    print("[Worker] Thread started, initializing clients...")
    credentials = get_credentials()

    if credentials:
        print("[Worker] Using service account credentials")
        db = firestore.Client(project=PROJECT_ID, credentials=credentials, database=DATABASE_ID)
        subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    else:
        print("[Worker] Using default credentials")
        db = firestore.Client(project=PROJECT_ID, database=DATABASE_ID)
        subscriber = pubsub_v1.SubscriberClient()

    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

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

    try:
        print(f"[Worker] Subscribing to {subscription_path}")
        streaming_pull_future = subscriber.subscribe(subscription_path, callback=process_vote)
        print("[Worker] Listening for messages...")
        streaming_pull_future.result()
    except Exception as e:
        print(f"[Worker] FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()

def start_worker_thread():
    global worker_thread
    worker_thread = threading.Thread(target=run_worker, daemon=True)
    worker_thread.start()
    print("[Worker] Background thread spawned!")

# Use gunicorn post_fork hook via app startup
import atexit
start_worker_thread()

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "thread_alive": worker_thread.is_alive() if worker_thread else False,
        "processed": votes_processed,
        "duplicates": votes_duplicate,
        "errors": votes_errored
    }), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
