import requests, json, time

CONFIG_FILE = "../config.json"

with open(CONFIG_FILE) as f:
    config = json.load(f)

NAMENODE_URL = f"http://{config['namenode']['host']}:{config['namenode']['http_port']}"

def check_status():
    res = requests.get(f"{NAMENODE_URL}/status")
    print(json.dumps(res.json(), indent=2))

if __name__ == "__main__":
    print("[CLIENT] Checking Namenode status...")
    check_status()
