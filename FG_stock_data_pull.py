import os
import requests
import pandas as pd
from datetime import datetime
import pytz
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

# ========= CONFIG ==========
ODOO_URL = os.getenv("ODOO_URL")
DB = os.getenv("ODOO_DB")
USERNAME = os.getenv("ODOO_USERNAME")
PASSWORD = os.getenv("ODOO_PASSWORD")

SHEET_ID = "1WNwp_7AnRVHuxV5WR6zw6ijF2kxTS3tNiaXW92D1O9I"

COMPANIES = {
    1: {"name": "Zipper", "sheet": "Zip FG live Stock"},
    3: {"name": "Metal Trims", "sheet": "MT FG live Stock"},
}

session = requests.Session()
USER_ID = None

# ========= GOOGLE SHEETS ==========
creds = Credentials.from_service_account_file(
    "gcreds.json",
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
client = gspread.authorize(creds)

# ========= ODOO LOGIN ==========
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
        print(f"✅ Logged in (uid={USER_ID})")
    else:
        raise Exception("❌ Odoo login failed")

# ========= FETCH OPERATION DETAILS ==========
# The Odoo web UI fetches ALL companies in ONE call. It sends args=[[]] (empty)
# and relies on context.allowed_company_ids to decide which companies' data to
# return. The response is {"result": {"datas": {OA_no: [entry, entry, ...]}}}.
# Each entry carries its own company_id, so we split locally afterwards.
def fetch_all_data():
    context = {
        "lang": "en_US",
        "tz": "Asia/Dhaka",
        "uid": USER_ID,
        "allowed_company_ids": list(COMPANIES.keys()),
    }
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "operation.details",
            "method": "retrive_data_from_operation_details",
            "args": [[]],
            "kwargs": {"context": context},
        },
    }
    r = session.post(
        f"{ODOO_URL}/web/dataset/call_kw/operation.details/retrive_data_from_operation_details",
        json=payload,
    )
    r.raise_for_status()
    body = r.json()
    if "error" in body:
        raise Exception(f"❌ Odoo error: {body['error']}")
    datas = body.get("result", {}).get("datas", {})
    total = sum(len(v) for v in datas.values())
    print(f"📦 Fetched {total} line entries across {len(datas)} OAs")
    return datas

# ========= PROCESS DATA ==========
# datas is a dict: {OA_no: [entry, ...]}. Flatten every entry and keep only the
# rows whose company_id matches the company we are building the sheet for.
def process_data(datas, company_id):
    all_rows = []
    for entries in datas.values():
        if not isinstance(entries, list):
            continue
        for e in entries:
            if e.get('company_id') != company_id:
                continue
            all_rows.append({
                'OA': e.get('oa_name'),
                'Order Date': e.get('date_order'),
                'Closing Date': e.get('closing_date'),
                'Sample': e.get('sample'),
                'PI': e.get('pi'),
                'Customer': e.get('customer_name'),
                'Buyer': e.get('buyer_name'),
                'Invoice No': e.get('invoice_line_id'),
                'Invoice Date': e.get('invoice_date'),
                'LC Number': e.get('lc_number'),
                'LC Date': e.get('lc_date'),
                'Sales Person': e.get('sales_person_name'),
                'Region': e.get('region_name'),
                'DSM': e.get('core_leader_name'),
                'Item': e.get('fg_categ_type'),
                'Product': e.get('product_id'),
                'Order QTY': e.get('order_qty'),
                'Order Value': e.get('order_value'),
                'Recived QTY': e.get('received_qty'),
                'Recived Value': e.get('received_value'),
                'Goods In Date': e.get('goods_in_date'),
                'Delivered QTY': e.get('delivered_qty'),
                'Delivered Value': e.get('delivered_value'),
                'Delivered Date': e.get('delivery_date'),
                'Pending QTY': e.get('pending_qty'),
                'Stock QTY': e.get('stock_qty'),
                'Stock Value': e.get('stock_value'),
                'Age': e.get('days_passed'),
                'Invoice QTY': e.get('invoice_qty'),
                'Invoice Value': e.get('invoice_value'),
            })
    return pd.DataFrame(all_rows)

# ========= PASTE TO GOOGLE SHEET ==========
def paste_to_gsheet(df, sheet_name):
    sheet = client.open_by_key(SHEET_ID)
    ws = sheet.worksheet(sheet_name)
    if df.empty:
        print(f"Skip: {sheet_name} is empty")
        return
    ws.batch_clear(['A:AD'])
    set_with_dataframe(ws, df)
    print(f"✅ Data pasted to {sheet_name}")
    local_tz = pytz.timezone('Asia/Dhaka')
    timestamp = datetime.now(local_tz).strftime("%Y-%m-%d %H:%M:%S")
    ws.update("AF2", [[timestamp]])
    print(f"Timestamp written to AF2: {timestamp}")

# ========= MAIN ==========
if __name__ == "__main__":
    login()
    datas = fetch_all_data()
    for cid, info in COMPANIES.items():
        df = process_data(datas, cid)
        print(f"   {info['name']} (company {cid}): {len(df)} rows")
        paste_to_gsheet(df, info["sheet"])
