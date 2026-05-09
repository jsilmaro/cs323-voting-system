import json
import os
import base64
from flask import Flask, request, jsonify
from flask_cors import CORS
from google.cloud import pubsub_v1
from google.oauth2 import service_account

app = Flask(__name__)
CORS(app)

PROJECT_ID = "lustrous-baton-495804-r7"
TOPIC_ID = "vote-topic"

sa_key_b64 = os.environ.get("GCP_SA_KEY")
if sa_key_b64:
    sa_info = json.loads(base64.b64decode(sa_key_b64).decode("utf-8"))
    credentials = service_account.Credentials.from_service_account_info(sa_info)
    publisher = pubsub_v1.PublisherClient(credentials=credentials)
else:
    publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

REQUIRED_FIELDS = {"user_id", "poll_id", "choice"}
VALID_CHOICES = {"A", "B", "C"}

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200

@app.route("/vote", methods=["POST"])
def receive_vote():
    vote = request.get_json(silent=True)
    if not vote:
        return jsonify({"error": "Invalid or missing JSON payload"}), 400
    missing = REQUIRED_FIELDS - set(vote.keys())
    if missing:
        return jsonify({"error": f"Missing fields: {list(missing)}"}), 400
    if vote.get("choice") not in VALID_CHOICES:
        return jsonify({"error": "Invalid choice. Must be A, B, or C"}), 400
    try:
        message_data = json.dumps(vote).encode("utf-8")
        future = publisher.publish(topic_path, data=message_data)
        message_id = future.result()
        return jsonify({"status": "accepted", "message_id": message_id}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
