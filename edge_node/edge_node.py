import uuid
import random
import time
import requests
import os
import sys

API_URL = os.environ.get("API_URL", "https://cs323-api.onrender.com/vote")
NODE_ID = os.environ.get("NODE_ID", "node-" + uuid.uuid4().hex[:6])
MAX_RETRIES = 3
RETRY_DELAY = 2
BURST_SIZE = int(os.environ.get("BURST_SIZE", 5))  
BURST_INTERVAL = float(os.environ.get("BURST_INTERVAL", 10.0))  

votes_generated = 0
votes_sent = 0
votes_failed = 0

def generate_vote():
    global votes_generated
    votes_generated += 1
    vote = {
        "user_id": str(uuid.uuid4()),
        "poll_id": "poll_1",
        "choice": random.choice(["A", "B", "C"]),
        "timestamp": time.time(),
        "time_created": time.time(),
        "edge_node_id": NODE_ID,
    }
    print(f"[{NODE_ID}] Generated: {vote['user_id']} | Choice: {vote['choice']}")
    return vote

def send_vote(vote, simulate_duplicate=False):
    global votes_sent, votes_failed
    transmissions = 2 if simulate_duplicate else 1
    for _ in range(transmissions):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.post(API_URL, json=vote, timeout=5)
                response.raise_for_status()
                votes_sent += 1
                print(f"[{NODE_ID}] Sent: {vote['user_id']} | Status: {response.status_code}")
                break
            except Exception as e:
                print(f"[{NODE_ID}] Attempt {attempt} failed: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY * (2 ** (attempt - 1)))
                else:
                    votes_failed += 1

def burst_votes():
    """Simulate a sudden spike of votes from this edge node."""
    print(f"[{NODE_ID}] ⚡ BURST MODE: Sending {BURST_SIZE} votes rapidly...")
    for _ in range(BURST_SIZE):
        vote = generate_vote()
        send_vote(vote)
        time.sleep(0.2) 
    print(f"[{NODE_ID}] ⚡ Burst complete.")

def run_edge_node(simulate_duplicate=False, burst_mode=False):
    print("=" * 50)
    print(f"  Edge Node: {NODE_ID}")
    print(f"  API: {API_URL}")
    print(f"  Duplicate Mode: {simulate_duplicate}")
    print(f"  Burst Mode: {burst_mode}")
    print("=" * 50)
    try:
        while True:
            if burst_mode:
                burst_votes()
                time.sleep(BURST_INTERVAL)
            else:
                vote = generate_vote()
                send_vote(vote, simulate_duplicate=simulate_duplicate)
                if votes_generated % 10 == 0:
                    print(f"[{NODE_ID}] STATS -> Generated: {votes_generated} | Sent: {votes_sent} | Failed: {votes_failed}")
                time.sleep(random.uniform(1, 3))
    except KeyboardInterrupt:
        print(f"\n[{NODE_ID}] Stopped. Generated: {votes_generated} | Sent: {votes_sent} | Failed: {votes_failed}")

if __name__ == "__main__":
    duplicate_mode = "--duplicate" in sys.argv
    burst_mode = "--burst" in sys.argv
    run_edge_node(simulate_duplicate=duplicate_mode, burst_mode=burst_mode)
