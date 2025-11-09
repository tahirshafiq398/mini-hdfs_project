from flask import Flask, request, jsonify, send_file
import json, os, time, requests, tempfile
from werkzeug.utils import secure_filename

app = Flask(__name__)

# ---------------- CONFIG AND PATHS ----------------
APP_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(APP_DIR, ".."))
CONFIG_PATH = os.path.join(REPO_ROOT, "config.json")
META_PATH = os.path.join(APP_DIR, "metadata.json")

# Load configuration
with open(CONFIG_PATH, "r") as f:
    config = json.load(f)

# Initialize metadata
if os.path.exists(META_PATH):
    with open(META_PATH, "r") as f:
        metadata = json.load(f)
else:
    metadata = {"datanodes": {}, "files": {}}


def save_metadata():
    """Save metadata to file"""
    with open(META_PATH, "w") as f:
        json.dump(metadata, f, indent=2)


# ---------------- ROUTES ----------------

@app.route("/ping")
def ping():
    return jsonify({"status": "namenode-ok", "time": time.time()})


@app.route("/register", methods=["POST"])
def register():
    data = request.json
    node_id = data.get("node_id")
    if not node_id:
        return jsonify({"error": "Missing node_id"}), 400

    metadata["datanodes"][node_id] = {
        "last_heartbeat": time.time(),
        "info": data
    }
    save_metadata()
    print(f"[REGISTER] {node_id} registered successfully.")
    return jsonify({"message": f"Datanode {node_id} registered"}), 200


@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    data = request.json
    node_id = data.get("node_id")
    if node_id in metadata["datanodes"]:
        metadata["datanodes"][node_id]["last_heartbeat"] = time.time()
        save_metadata()
        print(f"[HEARTBEAT] {node_id} updated heartbeat.")
        return jsonify({"status": "heartbeat-received"}), 200
    else:
        return jsonify({"error": "Unknown datanode"}), 404


@app.route("/status", methods=["GET"])
def status():
    """
    Return the current status of the Namenode:
    - Active datanodes with last heartbeat
    - Files and their chunk metadata
    """
    try:
        if "datanodes" not in metadata:
            metadata["datanodes"] = {}
        if "files" not in metadata:
            metadata["files"] = {}

        response = {
            "datanodes": metadata["datanodes"],
            "files": metadata["files"]
        }

        print("[STATUS] Returning namenode metadata to client")
        return jsonify(response), 200

    except Exception as e:
        print("[ERROR] /status failed:", e)
        return jsonify({"error": f"Failed to get status: {str(e)}"}), 500


@app.route("/upload", methods=["POST"])
def upload_file():
    """
    Handle file upload:
    - Split into chunks
    - Send chunks to both Datanodes (replication)
    - Update metadata
    """
    try:
        file = request.files.get("file")
        if not file:
            return jsonify({"error": "No file provided"}), 400

        filename = secure_filename(file.filename)
        file.save(filename)

        chunk_size = config.get("chunk_size_bytes", 2048000)
        # file_id used as key in metadata (filename without extension)
        file_id = os.path.splitext(filename)[0]

        metadata["files"][file_id] = {
            "filename": filename,
            "size": os.path.getsize(filename),
            "chunk_size": chunk_size,
            "chunks": []
        }

        with open(filename, "rb") as f:
            seq = 0
            while chunk := f.read(chunk_size):
                chunk_id = f"{file_id}_chunk_{seq}"
                for node in ["datanodeA", "datanodeB"]:
                    url = f"http://{config[node]['host']}:{config[node]['port']}/store_chunk/{chunk_id}"
                    try:
                        # send binary chunk as file
                        requests.post(url, files={"file": (chunk_id, chunk)}, timeout=10)
                        print(f"[UPLOAD] Sent {chunk_id} to {node}")
                    except Exception as e:
                        print(f"[ERROR] Upload to {node} failed:", e)

                metadata["files"][file_id]["chunks"].append({
                    "chunk_id": chunk_id,
                    "replicas": [
                        f"http://{config['datanodeA']['host']}:{config['datanodeA']['port']}",
                        f"http://{config['datanodeB']['host']}:{config['datanodeB']['port']}"
                    ]
                })
                seq += 1

        save_metadata()
        os.remove(filename)
        print(f"[UPLOAD COMPLETE] {filename} split into {seq} chunks.")
        return jsonify({"message": f"{filename} uploaded", "chunks": seq}), 200

    except Exception as e:
        print("[ERROR] Upload failed:", e)
        return jsonify({"error": str(e)}), 500


@app.route("/download/<filename>", methods=["GET"])
def download_file(filename):
    """
    Reconstructs a file from all its chunks stored in Datanodes
    and returns it to the client as a single file.
    - filename parameter is the metadata key (file_id). In this project file_id
      was created as the filename without extension when uploading.
    """
    try:
        if filename not in metadata["files"]:
            return jsonify({"error": "File not found"}), 404

        file_info = metadata["files"][filename]
        chunks = file_info.get("chunks", [])

        reconstructed = b""

        # Download chunks from Datanodes
        for chunk in chunks:
            chunk_id = chunk["chunk_id"]
            replicas = chunk.get("replicas", [])

            # Try multiple URL patterns for each replica until one works
            chunk_data = None
            for replica_base in replicas:
                # try a few possible endpoints that datanode might expose
                candidates = [
                    f"{replica_base}/chunk/{chunk_id}",
                    f"{replica_base}/store_chunk/{chunk_id}",
                    f"{replica_base}/storage/{chunk_id}"
                ]
                for url in candidates:
                    try:
                        print(f"[DOWNLOAD] Trying {url} for {chunk_id}")
                        res = requests.get(url, timeout=8)
                        if res.status_code == 200 and res.content:
                            chunk_data = res.content
                            break
                    except Exception as e:
                        print(f"[WARN] {url} failed: {e}")
                if chunk_data:
                    break

            if chunk_data is None:
                return jsonify({"error": f"Chunk {chunk_id} could not be retrieved from any replica"}), 500

            reconstructed += chunk_data

        # Save reconstructed file temporarily
        temp_path = os.path.join(tempfile.gettempdir(), file_info["filename"])
        with open(temp_path, "wb") as out:
            out.write(reconstructed)

        print(f"[DOWNLOAD COMPLETE] {filename} reconstructed successfully -> {temp_path}")
        return send_file(temp_path, as_attachment=True)

    except Exception as e:
        print("[ERROR] Download failed:", e)
        return jsonify({"error": str(e)}), 500


# ---------------- MAIN ----------------

if __name__ == "__main__":
    host = config["namenode"]["host"]
    port = config["namenode"]["http_port"]
    print(f"[NAMENODE] Running on {host}:{port}")
    # listen on the configured interface (0.0.0.0 recommended if you want all interfaces)
    app.run(host=host, port=port)

