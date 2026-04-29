"""
Retail360 — Fabric Workspace Setup (REST API)
=============================================
Creates Workspace, Lakehouse, and SQL Database via Fabric REST API.

Usage:
  python 02_fabric_workspace/create_workspace.py

Prerequisites:
  - config/config.json populated with tenant_id, client_id,
    client_secret, capacity_id
  - pip install msal requests
"""

import json, os, sys, time
import requests, msal

config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')
with open(config_path) as f:
    CONFIG = json.load(f)

API          = "https://api.fabric.microsoft.com/v1"
SCOPE        = "https://api.fabric.microsoft.com/.default"
TENANT_ID    = CONFIG["azure"]["tenant_id"]
CLIENT_ID    = CONFIG["fabric"]["client_id"]
CLIENT_SECRET= CONFIG["fabric"]["client_secret"]
CAPACITY_ID  = CONFIG["fabric"]["capacity_id"]
WS_NAME      = CONFIG["fabric"]["workspace_name"]
LH_NAME      = CONFIG["fabric"]["lakehouse_name"]
SQL_NAME     = CONFIG["fabric"]["sql_database_name"]


def get_token():
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential=CLIENT_SECRET,
    )
    r = app.acquire_token_for_client(scopes=[SCOPE])
    if "access_token" not in r:
        print(f"Auth error: {r}"); sys.exit(1)
    return r["access_token"]

def hdrs(token):
    return {"Authorization":f"Bearer {token}", "Content-Type":"application/json"}

def poll(token, location, retry=3):
    if not location: return {}
    time.sleep(int(retry))
    for _ in range(30):
        r = requests.get(location, headers=hdrs(token))
        d = r.json(); st = d.get("status","").lower()
        if st in ("succeeded","completed"): return d.get("result", d)
        if st in ("failed","cancelled"):    raise RuntimeError(f"Failed: {d}")
        time.sleep(int(r.headers.get("Retry-After",3)))
    raise TimeoutError("Timed out")

def post(token, path, payload):
    r = requests.post(f"{API}/{path}", headers=hdrs(token), json=payload)
    if r.status_code == 202:
        return poll(token, r.headers.get("Location"), r.headers.get("Retry-After",3))
    r.raise_for_status()
    return r.json() if r.content else {}

def get(token, path):
    r = requests.get(f"{API}/{path}", headers=hdrs(token))
    r.raise_for_status(); return r.json()

def get_or_create_workspace(token):
    print(f"  Checking for workspace: {WS_NAME}...")
    for ws in get(token, "workspaces").get("value", []):
        if ws["displayName"] == WS_NAME:
            print(f"  Found: {ws['id']}")
            return ws["id"]
    print(f"  Creating workspace: {WS_NAME}...")
    result = post(token, "workspaces", {"displayName":WS_NAME,"capacityId":CAPACITY_ID})
    print(f"  Created: {result.get('id')}")
    return result.get("id")

def create_item(token, ws_id, name, item_type):
    print(f"  Creating {item_type}: {name}...")
    r = post(token, f"workspaces/{ws_id}/items",
             {"displayName":name,"type":item_type})
    print(f"  Created: {r.get('id')}")
    return r.get("id")

def update_config(ws_id, lh_id):
    with open(config_path) as f: cfg = json.load(f)
    cfg["fabric"]["workspace_id"]  = ws_id
    cfg["fabric"]["lakehouse_id"]  = lh_id
    cfg["onelake"]["workspace_id"] = ws_id
    cfg["onelake"]["lakehouse_id"] = lh_id
    with open(config_path,"w") as f: json.dump(cfg, f, indent=2)
    print("  config.json updated with IDs.")

def main():
    print("="*50)
    print("  RETAIL360 — FABRIC WORKSPACE SETUP")
    print("="*50)
    token  = get_token()
    ws_id  = get_or_create_workspace(token)
    lh_id  = create_item(token, ws_id, LH_NAME, "Lakehouse")
    sql_id = create_item(token, ws_id, SQL_NAME, "SQLDatabase")
    update_config(ws_id, lh_id)
    print("\n" + "="*50)
    print("  COMPLETE")
    print(f"  Workspace ID : {ws_id}")
    print(f"  Lakehouse ID : {lh_id}")
    print(f"  SQL Database : {sql_id}")
    print("="*50)
    print("\n  Next steps:")
    print("  1. Create folder structure in Lakehouse Files/raw/")
    print("  2. Upload output_files/ contents")
    print("  3. Configure PostgreSQL Mirroring in Fabric portal")
    print("  4. Run Databricks notebooks")
    print("  5. Run 03_lakehouse/nb_silver_to_gold.py")

if __name__ == "__main__":
    main()
