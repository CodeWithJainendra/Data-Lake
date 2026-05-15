#!/usr/bin/env python3
"""Create NiFi data ingestion flow via REST API."""
import json
import urllib.request
import urllib.parse
import ssl
import sys

NIFI_URL = "https://localhost:8443"
USER = "admin"
PASS = "ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB"
ROOT_PG = None  # fetched dynamically after auth

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def req(method, path, data=None, token=None):
    url = NIFI_URL + path
    body = json.dumps(data).encode() if data else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    r = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(r, context=ctx) as resp:
            return json.loads(resp.read()), resp.status
    except urllib.error.HTTPError as e:
        return {"error": e.read().decode()}, e.code

# 1. Get token
print("Getting NiFi token...")
data = urllib.parse.urlencode({"username": USER, "password": PASS}).encode()
r = urllib.request.Request(
    NIFI_URL + "/nifi-api/access/token",
    data=data,
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    method="POST"
)
with urllib.request.urlopen(r, context=ctx) as resp:
    TOKEN = resp.read().decode().strip()
print(f"Token acquired: {TOKEN[:20]}...")

# Fetch root process group ID dynamically
r2 = urllib.request.Request(
    NIFI_URL + "/nifi-api/flow/process-groups/root",
    headers={"Authorization": f"Bearer {TOKEN}"}
)
with urllib.request.urlopen(r2, context=ctx) as resp:
    ROOT_PG = json.loads(resp.read())["processGroupFlow"]["id"]
print(f"Root PG: {ROOT_PG}")

# 2. Check idempotency — skip if processors already exist
existing, code = req("GET", f"/nifi-api/process-groups/{ROOT_PG}/processors", token=TOKEN)
if code == 200 and len(existing.get("processors", [])) > 0:
    count = len(existing["processors"])
    print(f"✅ NiFi flow already exists ({count} processors). Nothing to do.")
    sys.exit(0)

def make_processor(name, ptype, x, y, props, auto_terminate=None):
    body = {
        "component": {
            "type": ptype,
            "name": name,
            "position": {"x": x, "y": y},
            "config": {
                "schedulingStrategy": "TIMER_DRIVEN",
                "schedulingPeriod": "30 sec",
                "properties": props,
                "autoTerminatedRelationships": auto_terminate or []
            }
        },
        "revision": {"version": 0}
    }
    result, code = req("POST", f"/nifi-api/process-groups/{ROOT_PG}/processors", body, TOKEN)
    if code != 201:
        print(f"  ERROR {code}: {result}")
        return None
    pid = result["id"]
    print(f"  Created [{pid}] {name}")
    return pid

def make_connection(src_id, dst_id, relationships=["success"]):
    body = {
        "component": {
            "source": {"id": src_id, "type": "PROCESSOR", "groupId": ROOT_PG},
            "destination": {"id": dst_id, "type": "PROCESSOR", "groupId": ROOT_PG},
            "selectedRelationships": relationships,
            "backPressureObjectThreshold": "10000",
            "backPressureDataSizeThreshold": "1 GB"
        },
        "revision": {"version": 0}
    }
    result, code = req("POST", f"/nifi-api/process-groups/{ROOT_PG}/connections", body, TOKEN)
    if code != 201:
        print(f"  Connection ERROR {code}: {result}")
        return None
    print(f"  Connected {src_id[:8]}... -> {dst_id[:8]}...")
    return result["id"]

# ─── Tables Ingestion Flow ───────────────────────────────────────────────────
print("\n=== Creating Tables Ingestion Flow ===")

p1 = make_processor(
    "GetFile (CSV/JSON/Parquet tables)",
    "org.apache.nifi.processors.standard.GetFile",
    100, 200,
    {
        "Input Directory": "/opt/data/incoming/tables",
        "File Filter": ".*\\.(csv|json|jsonl|parquet|tsv|xlsx|sql|hl7)$",
        "Keep Source File": "true",
        "Recurse Subdirectories": "true",
        "Polling Interval": "10 sec",
        "Batch Size": "10"
    }
)

p2 = make_processor(
    "Set S3 Key Path",
    "org.apache.nifi.processors.attributes.UpdateAttribute",
    450, 200,
    {
        "s3.key": "tables/${filename}"
    }
)

p3 = make_processor(
    "PutS3Object → MinIO raw/tables",
    "org.apache.nifi.processors.aws.s3.PutS3Object",
    800, 200,
    {
        "Bucket": "raw",
        "Object Key": "${s3.key}",
        "Access Key": "admin",
        "Secret Key": "admin123456",
        "Region": "us-east-1",
        "Endpoint Override URL": "http://minio:9000",
        "Signer Override": "AWSS3V4SignerType",
        "use-path-style-access": "true",
        "Communications Timeout": "30 secs",
    },
    auto_terminate=["success", "failure"]
)

if p1 and p2:
    make_connection(p1, p2)
if p2 and p3:
    make_connection(p2, p3)


# ─── PDF Ingestion Flow ───────────────────────────────────────────────────────
print("\n=== Creating PDF Ingestion Flow ===")

p4 = make_processor(
    "GetFile (Medical PDFs)",
    "org.apache.nifi.processors.standard.GetFile",
    100, 500,
    {
        "Input Directory": "/opt/data/incoming/pdfs",
        "File Filter": ".*\\.pdf$",
        "Keep Source File": "true",
        "Recurse Subdirectories": "true",
        "Polling Interval": "30 sec",
        "Batch Size": "5"
    }
)

p5 = make_processor(
    "Set PDF S3 Key",
    "org.apache.nifi.processors.attributes.UpdateAttribute",
    450, 500,
    {
        "s3.key": "pdfs/${filename}",
        "mime.type": "application/pdf"
    }
)

p6 = make_processor(
    "PutS3Object → MinIO raw/pdfs",
    "org.apache.nifi.processors.aws.s3.PutS3Object",
    800, 500,
    {
        "Bucket": "raw",
        "Object Key": "${s3.key}",
        "Access Key": "admin",
        "Secret Key": "admin123456",
        "Region": "us-east-1",
        "Endpoint Override URL": "http://minio:9000",
        "Signer Override": "AWSS3V4SignerType",
        "use-path-style-access": "true",
        "Communications Timeout": "30 secs",
    },
    auto_terminate=["success", "failure"]
)

if p4 and p5:
    make_connection(p4, p5)
if p5 and p6:
    make_connection(p5, p6)

print("\n✅ NiFi flow creation complete!")

# Auto-start all processors
print("\nStarting all processors...")
import time
time.sleep(2)  # brief settle time
procs_all, _ = req("GET", f"/nifi-api/process-groups/{ROOT_PG}/processors", token=TOKEN)
for p in procs_all.get("processors", []):
    pid = p["id"]
    pname = p["component"]["name"]
    detail, _ = req("GET", f"/nifi-api/processors/{pid}", token=TOKEN)
    if detail["component"]["state"] == "RUNNING":
        print(f"  ✅ Already running: {pname}")
        continue
    run, code = req("PUT", f"/nifi-api/processors/{pid}/run-status", {
        "revision": detail["revision"],
        "state": "RUNNING",
        "disconnectedNodeAcknowledged": False
    }, TOKEN)
    new_state = run.get("component", {}).get("state", "?")
    val_err = run.get("component", {}).get("validationErrors", [])
    if val_err:
        print(f"  ⚠️  Cannot start {pname}: {val_err[0]}")
    else:
        print(f"  ▶ Started: {pname} → {new_state}")

print("\nOpen Firefox: https://localhost:8443/nifi")
print("Canvas shows: GetFile → UpdateAttribute → PutS3Object (x2 rows)")
print("MinIO Console: http://localhost:9001  →  Buckets → raw → tables/ or pdfs/")
