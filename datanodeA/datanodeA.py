from flask import Flask, request, jsonify, send_file
import requests, os, json, time, threading

app = Flask(__name__)

# ---------------- CONFIG ----------------
CONFIG_FILE = "../config.json"

with open(CONFIG_FILE) as f:
    config = json.load(f)

NODE_ID = "DatanodeA"
NAMENODE_URL = f"http://{config['namenode']['host']}:{config['namenode']['http_port']}"
STORAGE_DIR = "storage"
os.makedirs(STORAGE_DIR, exist_ok=True)


# ---------------- ROUTES ----------------
@app.route("/ping")
def ping():
    return jsonify({"status": "datanodeA-ok", "time": time.time()})


@app.route("/store_chunk/<chunk_id>", methods=["POST"])
def store_chunk(chunk_id):
    """Store chunk sent by Namenode."""
    os.makedirs(STORAGE_DIR, exist_ok=True)
    file = request.files["file"]
    chunk_path = os.path.join(STORAGE_DIR, chunk_id)
    file.save(chunk_path)
    print(f"[CHUNK STORED] {chunk_id}")
    return jsonify({"status": "stored", "chunk": chunk_id}), 200


@app.route("/chunk/<chunk_id>", methods=["GET"])
def get_chunk(chunk_id):
    """Return stored chunk bytes for download reconstruction."""
    chunk_path = os.path.join(STORAGE_DIR, chunk_id)
    if not os.path.exists(chunk_path):
        return jsonify({"error": "Chunk not found"}), 404
    print(f"[CHUNK FETCHED] {chunk_id}")
    return send_file(chunk_path, as_attachment=False)


# ---------------- HEARTBEAT + REGISTER ----------------
def register():
    """Register Datanode with Namenode."""
    try:
        data = {"node_id": NODE_ID, "timestamp": time.time()}
        requests.post(f"{NAMENODE_URL}/register", json=data, timeout=5)
        print(f"[REGISTERED] {NODE_ID} with Namenode")
    except Exception as e:
        print("[ERROR] Registration failed:", e)


def heartbeat():
    """Send periodic heartbeat to Namenode."""
    while True:
        try:
            data = {"node_id": NODE_ID}
            requests.post(f"{NAMENODE_URL}/heartbeat", json=data, timeout=5)
            time.sleep(10)
        except Exception as e:
            print("[WARN] Heartbeat failed:", e)
            time.sleep(5)


# ---------------- MAIN ----------------
if __name__ == "__main__":
    threading.Thread(target=heartbeat, daemon=True).start()
    register()
    print(f"[DATANODE A] Running on {config['datanodeA']['host']}:{config['datanodeA']['port']}")
    app.run(host=config["datanodeA"]["host"], port=config["datanodeA"]["port"])
