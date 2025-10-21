import os
import sys
import re
import logging
import time
from pathlib import Path
from datetime import date, datetime
import pytz
import pandas as pd
import requests
from google.oauth2 import service_account
import gspread
from gspread_dataframe import set_with_dataframe
from dotenv import load_dotenv

# ===== Setup Logging =====
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
log = logging.getLogger()

# ===== Load environment variables =====
load_dotenv()
ODOO_URL = os.getenv("ODOO_URL")
DB = os.getenv("ODOO_DB")
USERNAME = os.getenv("ODOO_USERNAME")
PASSWORD = os.getenv("ODOO_PASSWORD")

SHEET_INFO = {
    "zipper": {
        "sheet_id": "1WNwp_7AnRVHuxV5WR6zw6ijF2kxTS3tNiaXW92D1O9I",
        "worksheet_name": "ZIP_FG_stock"  # Adjusted for new report; change if needed
    },
    "metal_trims": {
        "sheet_id": "1WNwp_7AnRVHuxV5WR6zw6ijF2kxTS3tNiaXW92D1O9I",
        "worksheet_name": "MT_FG_stock"  # Adjusted for new report; change if needed
    }
}

# ===== Default: current month 1st to today if env vars are empty =====
today = date.today()
from_date_env = os.getenv("FROM_DATE", "").strip()
to_date_env = os.getenv("TO_DATE", "").strip()

FROM_DATE = from_date_env if from_date_env else today.replace(day=1).isoformat()
TO_DATE = to_date_env if to_date_env else today.isoformat()

log.info(f"Using FROM_DATE={FROM_DATE}, TO_DATE={TO_DATE}")

DOWNLOAD_DIR = os.path.join(os.getcwd(), "download")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

session = requests.Session()
USER_ID = None

# ===== Utility Functions =====
def login():
    global USER_ID
    payload = {
        "jsonrpc": "2.0",
        "params": {
            "db": DB,
            "login": USERNAME,
            "password": PASSWORD
        }
    }
    r = session.post(f"{ODOO_URL}/web/session/authenticate", json=payload)
    r.raise_for_status()
    result = r.json().get("result")
    if result and "uid" in result:
        USER_ID = result["uid"]
        log.info(f"✅ Logged in (uid={USER_ID})")
        return result
    else:
        raise Exception("❌ Login failed")

def get_companies():
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "res.company",
            "method": "search_read",
            "args": [],
            "kwargs": {
                "domain": [["id", "not in", [4, 2]]],
                "fields": ["id", "name"],
                "context": {
                    "lang": "en_US",
                    "tz": "Asia/Dhaka",
                    "uid": USER_ID,
                    "allowed_company_ids": [1]
                }
            }
        }
    }
    r = session.post(f"{ODOO_URL}/web/dataset/call_kw/res.company/search_read", json=payload)
    r.raise_for_status()
    companies = {c["id"]: c["name"] for c in r.json()["result"]}
    log.info(f"🏢 Fetched companies: {companies}")
    return companies

def switch_company(company_id):
    if USER_ID is None:
        raise Exception("User not logged in yet")
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "res.users",
            "method": "write",
            "args": [[USER_ID], {"company_id": company_id}],
            "kwargs": {
                "context": {
                    "allowed_company_ids": [company_id],
                    "company_id": company_id
                }
            }
        }
    }
    r = session.post(f"{ODOO_URL}/web/dataset/call_kw", json=payload)
    r.raise_for_status()
    if "error" in r.json():
        log.error(f"❌ Failed to switch to company {company_id}: {r.json()['error']}")
        return False
    else:
        log.info(f"🔄 Session switched to company {company_id}")
        return True

def fetch_fg_store_datas(company_id, cname, from_date, to_date):
    context = {
        "lang": "en_US",
        "tz": "Asia/Dhaka",
        "uid": USER_ID,
        "allowed_company_ids": [company_id]
    }
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "operation.details",
            "method": "retrieve_fg_store_datas",
            "args": [[company_id], from_date, to_date],
            "kwargs": {"context": context}
        }
    }
    r = session.post(f"{ODOO_URL}/web/dataset/call_kw/operation.details/retrieve_fg_store_datas", json=payload)
    r.raise_for_status()
    try:
        data = r.json()["result"]
        if isinstance(data, list):
            def flatten_record(record):
                return {
                    k: v[1] if isinstance(v, list) and len(v) == 2 else 
                    (v.get("display_name") if isinstance(v, dict) and "display_name" in v else v) 
                    for k, v in record.items()
                }
            flattened = [flatten_record(rec) for rec in data if isinstance(rec, dict)]
            log.info(f"📊 {cname}: {len(flattened)} rows fetched (flattened)")
            return flattened
        else:
            log.warning(f"⚠️ Unexpected data format for {cname}: {type(data)}")
            return []
    except Exception as e:
        log.error(f"❌ {cname}: Failed to parse data: {e}")
        return []

# ====== Function to save records using regex-friendly pattern ======
def save_records_to_excel(records, company_name):
    if records:
        df = pd.DataFrame(records)
        company_clean = re.sub(r'\W+', '_', company_name.lower())
        output_file = os.path.join(DOWNLOAD_DIR, f"{company_clean}_fg_store_datas_{today.isoformat()}.xlsx")
        df.to_excel(output_file, index=False)
        log.info(f"📂 Saved: {output_file}")
        return output_file
    else:
        log.warning(f"❌ No data fetched for {company_name}")
        return None

# ====== Function to paste downloaded files into Google Sheet ======
def paste_downloaded_file_to_gsheet(company_name, sheet_key, worksheet_name):
    try:
        company_clean = re.sub(r'\W+', '_', company_name.lower())
        files = list(Path(DOWNLOAD_DIR).glob(f"{company_clean}_fg_store_datas_*.xlsx"))
        if not files:
            log.warning(f"⚠️ No downloaded file found for {company_name}")
            return
        
        files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        latest_file = files[0]
        df = pd.read_excel(latest_file)
        
        # Drop first column if exists
        if df.shape[1] > 1:
            df = df.iloc[:, 1:]
        
        log.info(f"✅ Loaded file {latest_file.name} into DataFrame (first column dropped)")

        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = service_account.Credentials.from_service_account_file('gcreds.json', scopes=scope)
        client = gspread.authorize(creds)
        
        sheet = client.open_by_key(sheet_key)
        worksheet = sheet.worksheet(worksheet_name)
        
        if df.empty:
            log.warning(f"⚠️ DataFrame for {company_name} is empty. Skipping paste.")
            return
        df = df.replace(False, "") 
        worksheet.batch_clear(["A:L"])
        time.sleep(2)
        set_with_dataframe(worksheet, df)
        log.info(f"✅ Data pasted into Google Sheet ({worksheet_name}) for {company_name}")
        
        local_tz = pytz.timezone('Asia/Dhaka')
        local_time = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
        worksheet.update("M1", [[f"{local_time}"]])
        log.info(f"✅ Timestamp updated: {local_time}")
        
    except Exception as e:
        log.error(f"❌ Error in paste_downloaded_file_to_gsheet({company_name}): {e}")

# ====== Main Workflow ======
if __name__ == "__main__":
    userinfo = login()
    log.info(f"User info (allowed companies): {userinfo.get('user_companies', {})}")

    # Explicitly define which company IDs to process
    target_companies = {
        1: "Zipper",        # Company ID 1 → Zipper
        3: "Metal_Trims"    # Company ID 3 → Metal Trims
    }

    for cid, cname in target_companies.items():
        if not switch_company(cid):
            log.warning(f"⚠️ Failed to switch to company {cid} ({cname}), skipping...")
            continue

        log.info(f"🔍 Processing {cname} (Company ID: {cid})")

        records = fetch_fg_store_datas(cid, cname, FROM_DATE, TO_DATE)
        save_records_to_excel(records, cname)

        # Push to Google Sheet
        sheet_info = SHEET_INFO.get(re.sub(r'\W+', '_', cname.lower()))
        if sheet_info:
            paste_downloaded_file_to_gsheet(
                cname,
                sheet_info["sheet_id"],
                sheet_info["worksheet_name"]
            )
        else:
            log.warning(f"⚠️ No Google Sheet mapping found for {cname}")
