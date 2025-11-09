from flask import Flask, render_template, request, redirect, send_file
import requests, json, os, time

app = Flask(__name__)

CONFIG_FILE = "../config.json"
UPLOAD_FOLDER = "uploads"

with open(CONFIG_FILE) as f:
    config = json.load(f)

NAMENODE_URL = f"http://{config['namenode']['host']}:{config['namenode']['http_port']}"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route("/")
def home():
    try:
        res = requests.get(f"{NAMENODE_URL}/status")
        data = res.json()
    except Exception as e:
        data = {"error": str(e), "datanodes": {}, "files": {}}
    return render_template(
    	"index.html",
    	data=data,
    	now=time.strftime("%Y-%m-%d %H:%M:%S"),
    	time=time  # ✅ send the time module to the template
    )

@app.route("/upload", methods=["POST"])
def upload_file():
    file = request.files["file"]
    if not file:
        return "No file selected", 400
    try:
        files = {"file": (file.filename, file.read())}
        res = requests.post(f"{NAMENODE_URL}/upload", files=files)
        print(res.json())
        return redirect("/")
    except Exception as e:
        return f"Upload failed:{e}",500

@app.route("/download/<filename>")
def download_file(filename):
    path = os.path.join(UPLOAD_FOLDER, filename)
    return send_file(path, as_attachment=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
