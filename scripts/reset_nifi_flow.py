#!/usr/bin/env python3
"""Completely wipe and recreate the NiFi flow with correct S3 properties."""
import json, urllib.request, urllib.parse, ssl, time, sys

NIFI_URL = "https://localhost:8443"
USER     = "admin"
PASS     = "ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB"
ROOT_PG  = "2a71a802-019e-1000-3a9a-0584a76ca790"

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode    = ssl.CERT_NONE

def req(method, path, data=None, token=None, raise_on_error=False):
    url  = NIFI_URL + path
    body = json.dumps(data).encode() if data else None
    hdrs = {"Content-Type": "application/json"}
    if token:
        hdrs["Authorization"] = f"Bearer {token}"
    r = urllib.request.Request(url, data=body, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(r, context=ctx) as resp:
            raw = resp.read()
            return (json.loads(raw) if raw else {}), resp.status
    except urllib.error.HTTPError as e:
        body_txt = e.read().decode()
        if raise_on_error:
            raise RuntimeError(f"HTTP {e.code}: {body_txt}")
        return {"error": body_txt}, e.code

# ── 1. Auth ──────────────────────────────────────────────────────────────────
print("1. Authenticating...")
data = urllib.parse.urlencode({"username": USER, "password": PASS}).encode()
r = urllib.request.Request(
    NIFI_URL + "/nifi-api/access/token",
    data=data,
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    method="POST"
)
with urllib.request.urlopen(r, context=ctx) as resp:
    TOKEN = resp.read().decode().strip()
print(f"   Token: {TOKEN[:20]}...")

# ── 2. Stop all processors ───────────────────────────────────────────────────
print("\n2. Stopping all processors...")
procs, _ = req("GET", f"/nifi-api/process-groups/{ROOT_PG}/processors", token=TOKEN)
for p in procs.get("processors", []):
    pid   = p["id"]
    pname = p["component"]["name"]
    state = p["component"]["state"]
    if state == "STOPPED":
        print(f"   Already stopped: {pname}")
        continue
    detail, _ = req("GET", f"/nifi-api/processors/{pid}", token=TOKEN)
    res, code = req("PUT", f"/nifi-api/processors/{pid}/run-status", {
        "revision": detail["revision"],
        "state": "STOPPED",
        "disconnectedNodeAcknowledged": False
    }, TOKEN)
    print(f"   Stopped ({code}): {pname}")

time.sleep(2)

# ── 3. Drop all queued FlowFiles from every connection ───────────────────────
print("\n3. Purging queued FlowFiles...")
conns, _ = req("GET", f"/nifi-api/process-groups/{ROOT_PG}/connections", token=TOKEN)
for c in conns.get("connections", []):
    cid   = c["id"]
    count = c.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued", 0)
    print(f"   Connection {cid[:12]}... queued={count}")
    if count == 0:
        continue
    # Correct path: /nifi-api/flowfile-queues/{id}/drop-requests
    drop, dcode = req("POST", f"/nifi-api/flowfile-queues/{cid}/drop-requests", token=TOKEN)
    req_id = (drop.get("dropRequest") or {}).get("id") or drop.get("id")
    if not req_id:
        print(f"   WARNING: no drop-request id returned ({dcode}): {drop}")
        continue
    # Poll until finished
    for _ in range(30):
        time.sleep(1)
        status, _ = req("GET", f"/nifi-api/flowfile-queues/{cid}/drop-requests/{req_id}", token=TOKEN)
        finished = (status.get("dropRequest") or {}).get("finished", False)
        if finished:
            break
    print(f"   Purged: {cid[:12]}")

time.sleep(1)

# ── 4. Delete all connections ────────────────────────────────────────────────
print("\n4. Deleting connections...")
conns, _ = req("GET", f"/nifi-api/process-groups/{ROOT_PG}/connections", token=TOKEN)
for c in conns.get("connections", []):
    cid = c["id"]
    ver = c["revision"]["version"]
    res, code = req("DELETE", f"/nifi-api/connections/{cid}?version={ver}", token=TOKEN)
    print(f"   Deleted connection ({code}): {cid[:12]}")

time.sleep(1)

# ── 5. Delete all processors ─────────────────────────────────────────────────
print("\n5. Deleting processors...")
procs, _ = req("GET", f"/nifi-api/process-groups/{ROOT_PG}/processors", token=TOKEN)
for p in procs.get("processors", []):
    pid   = p["id"]
    pname = p["component"]["name"]
    ver   = p["revision"]["version"]
    res, code = req("DELETE", f"/nifi-api/processors/{pid}?version={ver}", token=TOKEN)
    print(f"   Deleted ({code}): {pname}")

time.sleep(1)

# ── 5b. Second pass – delete any still-lingering connections then processors ──
print("\n5b. Second-pass cleanup...")
for attempt in range(3):
    conns2, _ = req("GET", f"/nifi-api/process-groups/{ROOT_PG}/connections", token=TOKEN)
    procs2, _ = req("GET", f"/nifi-api/process-groups/{ROOT_PG}/processors", token=TOKEN)
    if not conns2.get("connections") and not procs2.get("processors"):
        break
    # Purge any queued files again
    for c in conns2.get("connections", []):
        cid   = c["id"]
        count = c.get("status", {}).get("aggregateSnapshot", {}).get("flowFilesQueued", 0)
        if count > 0:
            drop, _ = req("POST", f"/nifi-api/flowfile-queues/{cid}/drop-requests", token=TOKEN)
            req_id = (drop.get("dropRequest") or {}).get("id")
            if req_id:
                for _ in range(15):
                    time.sleep(1)
                    st, _ = req("GET", f"/nifi-api/flowfile-queues/{cid}/drop-requests/{req_id}", token=TOKEN)
                    if (st.get("dropRequest") or {}).get("finished"):
                        break
    # Stop lingering processors
    for p in procs2.get("processors", []):
        pid = p["id"]
        if p["component"]["state"] != "STOPPED":
            detail, _ = req("GET", f"/nifi-api/processors/{pid}", token=TOKEN)
            req("PUT", f"/nifi-api/processors/{pid}/run-status",
                {"revision": detail["revision"], "state": "STOPPED",
                 "disconnectedNodeAcknowledged": False}, TOKEN)
    time.sleep(1)
    # Delete connections
    for c in conns2.get("connections", []):
        cid = c["id"]; ver = c["revision"]["version"]
        res, code = req("DELETE", f"/nifi-api/connections/{cid}?version={ver}", token=TOKEN)
        print(f"   2nd-pass del conn ({code}): {cid[:12]}")
    time.sleep(1)
    # Delete processors
    for p in procs2.get("processors", []):
        pid = p["id"]; ver = p["revision"]["version"]
        res, code = req("DELETE", f"/nifi-api/processors/{pid}?version={ver}", token=TOKEN)
        print(f"   2nd-pass del proc ({code}): {p['component']['name']}")
    time.sleep(1)

# ── 6. Verify canvas is clean ────────────────────────────────────────────────
procs, _ = req("GET", f"/nifi-api/process-groups/{ROOT_PG}/processors", token=TOKEN)
conns, _ = req("GET", f"/nifi-api/process-groups/{ROOT_PG}/connections", token=TOKEN)
print(f"\n   Canvas: {len(procs.get('processors',[]))} processors, {len(conns.get('connections',[]))} connections")

# ── 7. Recreate flow with CORRECT S3 keys ────────────────────────────────────
print("\n6. Creating processors with correct S3 property keys...")

def make_proc(name, ptype, x, y, props, auto_terminate=None):
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
        print(f"   ERROR {code}: {result}")
        return None
    pid = result["id"]
    print(f"   Created [{pid[:12]}] {name}")
    return pid

def make_conn(src, dst, rels=["success"]):
    body = {
        "component": {
            "source":      {"id": src, "type": "PROCESSOR", "groupId": ROOT_PG},
            "destination": {"id": dst, "type": "PROCESSOR", "groupId": ROOT_PG},
            "selectedRelationships": rels,
            "backPressureObjectThreshold": "10000",
            "backPressureDataSizeThreshold": "1 GB"
        },
        "revision": {"version": 0}
    }
    result, code = req("POST", f"/nifi-api/process-groups/{ROOT_PG}/connections", body, TOKEN)
    if code != 201:
        print(f"   Connection ERROR {code}: {result}")
        return None
    print(f"   Connected {src[:8]} -> {dst[:8]}")
    return result["id"]

# Correct S3 properties (NiFi PutS3Object real descriptor names)
# canned-acl is required in NiFi 1.27 — cannot be empty string
S3_PROPS_TABLES = {
    "Bucket":               "raw",
    "Object Key":           "${s3.key}",
    "Access Key":           "admin",
    "Secret Key":           "admin123456",
    "Region":               "us-east-1",
    "Endpoint Override URL":"http://minio:9000",
    "Signer Override":      "AWSS3V4SignerType",
    "use-path-style-access":"true",
    "canned-acl":           "BucketOwnerFullControl",
    "Communications Timeout":"30 secs",
}

S3_PROPS_PDFS = {**S3_PROPS_TABLES}   # same MinIO target

# Tables flow
p1 = make_proc("GetFile (CSV/JSON/Parquet tables)",
               "org.apache.nifi.processors.standard.GetFile", 100, 200,
               {"Input Directory": "/opt/data/incoming/tables",
                "File Filter": ".*\\.(csv|json|jsonl|parquet|tsv|xlsx|sql|hl7)$",
                "Keep Source File": "true",
                "Recurse Subdirectories": "true",
                "Polling Interval": "10 sec",
                "Batch Size": "10"})

p2 = make_proc("Set S3 Key Path",
               "org.apache.nifi.processors.attributes.UpdateAttribute", 450, 200,
               {"s3.key": "tables/${filename}"})

p3 = make_proc("PutS3Object → MinIO raw/tables",
               "org.apache.nifi.processors.aws.s3.PutS3Object", 800, 200,
               S3_PROPS_TABLES, auto_terminate=["success","failure"])

# PDF flow
p4 = make_proc("GetFile (Medical PDFs)",
               "org.apache.nifi.processors.standard.GetFile", 100, 500,
               {"Input Directory": "/opt/data/incoming/pdfs",
                "File Filter": ".*\\.pdf$",
                "Keep Source File": "true",
                "Recurse Subdirectories": "true",
                "Polling Interval": "30 sec",
                "Batch Size": "5"})

p5 = make_proc("Set PDF S3 Key",
               "org.apache.nifi.processors.attributes.UpdateAttribute", 450, 500,
               {"s3.key": "pdfs/${filename}", "mime.type": "application/pdf"})

p6 = make_proc("PutS3Object → MinIO raw/pdfs",
               "org.apache.nifi.processors.aws.s3.PutS3Object", 800, 500,
               S3_PROPS_PDFS, auto_terminate=["success","failure"])

# Connections
if p1 and p2: make_conn(p1, p2)
if p2 and p3: make_conn(p2, p3)
if p4 and p5: make_conn(p4, p5)
if p5 and p6: make_conn(p5, p6)

# ── 8. Start all processors ───────────────────────────────────────────────────
print("\n7. Starting all processors...")
time.sleep(3)   # let NiFi validate
procs_all, _ = req("GET", f"/nifi-api/process-groups/{ROOT_PG}/processors", token=TOKEN)
all_good = True
for p in procs_all.get("processors", []):
    pid   = p["id"]
    pname = p["component"]["name"]
    detail, _ = req("GET", f"/nifi-api/processors/{pid}", token=TOKEN)
    comp   = detail["component"]
    errors = comp.get("validationErrors", [])
    if errors:
        print(f"   ⚠️  INVALID {pname}: {errors}")
        all_good = False
        continue
    run, code = req("PUT", f"/nifi-api/processors/{pid}/run-status", {
        "revision": detail["revision"],
        "state": "RUNNING",
        "disconnectedNodeAcknowledged": False
    }, TOKEN)
    new_state = run.get("component", {}).get("state", "?")
    print(f"   ▶ {pname} → {new_state}")

print()
if all_good:
    print("✅ NiFi flow recreated and running!")
    print("   Now inject a test file:")
    print("   docker exec nifi bash -c \"echo 'id,name' > /opt/data/incoming/tables/test_probe.csv\"")
    print("   Then check MinIO: docker exec minio mc stat local/raw/tables/test_probe.csv")
else:
    print("⚠️  Some processors have validation errors — check above output.")
    sys.exit(1)
