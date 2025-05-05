
import os
import glob
import requests
import json

cred_path = os.path.join(os.path.dirname(__file__), "credentials", "databricks.json")
with open(cred_path, "r") as f:
    creds = json.load(f)

HOST   = creds["HOST"]
TOKEN  = creds["TOKEN"]
VOLUME = creds["VOLUME"]
JOB_ID = creds["JOB_ID"]

HEADERS_DIR  = {"Authorization": f"Bearer {TOKEN}"}
HEADERS_FILE = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/octet-stream",
}

def ensure_volume_dir(volume_path: str):
    
    url = f"https://{HOST}/api/2.0/fs/directories/Volumes/{volume_path}/"
    resp = requests.put(url, headers=HEADERS_DIR)
    
    if resp.status_code not in (200, 202, 409):
        resp.raise_for_status()

def upload_csv(local_path: str, volume_path: str):
    filename = os.path.basename(local_path)
    url = (
        f"https://{HOST}/api/2.0/fs/files/"
        f"Volumes/{volume_path}/{filename}"
        f"?overwrite=true"
    )
    with open(local_path, "rb") as f:
        data = f.read()
    resp = requests.put(url, headers=HEADERS_FILE, data=data)
    resp.raise_for_status()
    print(f"Subido {filename} -> Volumes/{volume_path}/{filename}")

def trigger_databricks_job(job_id: int):
    url = f"https://{HOST}/api/2.0/jobs/run-now"
    payload = {"job_id": job_id}
    resp = requests.post(url, headers=HEADERS_DIR, json=payload)
    resp.raise_for_status()
    run_id = resp.json().get("run_id")
    print(f"Workspace {job_id} disparado exitosamente (run_id={run_id})")

def Upload_and_runJob():
    ensure_volume_dir(VOLUME)

    base = os.path.join(os.path.dirname(__file__), "csv")
    for csv_path in glob.glob(os.path.join(base, "*.csv")):
        try:
            upload_csv(csv_path, VOLUME)
        except Exception as e:
            print(f"Error subiendo {csv_path!r}: {e}")
    
    trigger_databricks_job(JOB_ID)