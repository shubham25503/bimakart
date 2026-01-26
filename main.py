
import os
import io
import re
import httpx
import math
import pandas as pd
from fastapi import FastAPI, Response, UploadFile, File, Query
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from pymongo import MongoClient
try:
    from bson.objectid import ObjectId
except Exception:
    ObjectId = None
from datetime import datetime

# --- Configuration ---
load_dotenv()
BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8080")
DATABASE_URL = os.environ.get("DATABASE_URL")

# Mongo client
_mongo_client = None
def get_mongo_client():
    global _mongo_client
    if _mongo_client is None:
        if not DATABASE_URL:
            raise RuntimeError('DATABASE_URL not set')
        _mongo_client = MongoClient(DATABASE_URL, serverSelectionTimeoutMS=5000)
    return _mongo_client

def try_objectid(s):
    if ObjectId is None:
        return s
    try:
        return ObjectId(s)
    except Exception:
        return s

# --- App Initialization ---
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Routes ---
@app.get("/")
def serve_index():
    index_path = os.path.join(os.path.dirname(__file__), "index.html")
    return FileResponse(index_path, media_type="text/html")

@app.get("/api/products/active")
def get_active_products():
    url = f"{BACKEND_URL}/api/products/active"
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(url)
            return JSONResponse(resp.json(), status_code=resp.status_code)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


def _range_to_dates(range_key: str):
    now = pd.Timestamp.now()
    if range_key == '1D':
        start = now - pd.Timedelta(days=1)
    elif range_key == '1W':
        start = now - pd.Timedelta(weeks=1)
    elif range_key == '1M':
        start = now - pd.DateOffset(months=1)
    elif range_key == 'last_month':
        start = (now.replace(day=1) - pd.DateOffset(months=1)).normalize()
        end = now.replace(day=1) - pd.DateOffset(days=1)
        return pd.Timestamp(start.to_pydatetime()).to_pydatetime(), pd.Timestamp(end.to_pydatetime()).to_pydatetime()
    elif range_key == 'this_quarter':
        q = (now.month - 1) // 3 + 1
        start = pd.Timestamp(datetime(now.year, 3 * (q - 1) + 1, 1))
    elif range_key == 'last_quarter':
        q = (now.month - 1) // 3 + 1
        start = pd.Timestamp(datetime(now.year, 3 * (q - 2) + 1, 1))
    elif range_key == 'this_fy':
        fy_start_month = 4
        if now.month >= fy_start_month:
            start = pd.Timestamp(datetime(now.year, fy_start_month, 1))
        else:
            start = pd.Timestamp(datetime(now.year - 1, fy_start_month, 1))
    elif range_key == 'last_fy':
        fy_start_month = 4
        if now.month >= fy_start_month:
            start = pd.Timestamp(datetime(now.year - 1, fy_start_month, 1))
        else:
            start = pd.Timestamp(datetime(now.year - 2, fy_start_month, 1))
    else:
        start = pd.Timestamp.min
    end = pd.Timestamp.now()
    return pd.Timestamp(start.to_pydatetime()).to_pydatetime(), pd.Timestamp(end.to_pydatetime()).to_pydatetime()


@app.get('/api/meta/ranges')
def meta_ranges():
    return JSONResponse({
        'ranges': ['1D','1W','1M','last_month','this_quarter','last_quarter','this_fy','last_fy','all_time']
    })


@app.get('/api/policies')
def list_policies(q: str = None, page: int = 1, limit: int = 50):
    db = get_mongo_client().get_database('test')
    coll = db['products']
    qry = {}
    if q:
        qry['name'] = {'$regex': q, '$options': 'i'}
    total = coll.count_documents(qry)
    docs = list(coll.find(qry).skip((page-1)*limit).limit(limit))
    data = [{'policyId': str(d.get('_id')), 'policyName': d.get('name')} for d in docs]
    return JSONResponse({'data': data, 'pagination': {'page': page, 'limit': limit, 'total': total}})


@app.get('/api/agents/{agent_id}/dashboard/stats')
def agent_stats(agent_id: str, range: str = Query('1M')):
    db = get_mongo_client().get_database('test')
    start, end = _range_to_dates(range)
    # find applications for agent
    try:
        agent_oid = try_objectid(agent_id)
        apps = list(db['policyapplications'].find({'agentId': agent_id}))
    except Exception:
        apps = list(db['policyapplications'].find({'agentId': agent_id}))
    app_ids = [a.get('_id') for a in apps]

    # issuances
    iss_q = {'applicationId': {'$in': app_ids}, 'createdAt': {'$gte': start, '$lte': end}} if app_ids else {'createdAt': {'$gte': start, '$lte': end}}
    issuances = list(db['policyissuances'].find(iss_q)) if app_ids else []
    policies_issued = len(issuances)

    # payments
    pay_q = {'applicationId': {'$in': app_ids}, 'createdAt': {'$gte': start, '$lte': end}} if app_ids else {'createdAt': {'$gte': start, '$lte': end}}
    payments = list(db['paymentorders'].find(pay_q)) if app_ids else []
    ok_status = set(['paid','success','captured','completed','PAID','SUCCESS','CAPTURED','COMPLETED'])
    revenue = 0
    for p in payments:
        try:
            amt = int(p.get('amount') or 0)
        except Exception:
            try:
                amt = int(float(p.get('amount') or 0))
            except Exception:
                amt = 0
        revenue += amt

    # best selling policy & top policies
    prod_count = {}
    appid_to_product = {}
    for a in apps:
        aid = a.get('_id')
        prod = a.get('productId')
        appid_to_product[str(aid)] = str(prod) if prod else None
    for ins in issuances:
        pid = ins.get('productId') or appid_to_product.get(str(ins.get('applicationId')))
        pid = str(pid) if pid else 'unknown'
        entry = prod_count.setdefault(pid, {'sold':0, 'revenue':0})
        entry['sold'] += 1
    for p in payments:
        pid = appid_to_product.get(str(p.get('applicationId'))) or 'unknown'
        pid = str(pid)
        try:
            amt = int(p.get('amount') or 0)
        except Exception:
            amt = 0
        prod_count.setdefault(pid, {'sold':0, 'revenue':0})
        prod_count[pid]['revenue'] += amt

    total_revenue = sum(v['revenue'] for v in prod_count.values()) if prod_count else 0
    top_list = []
    # resolve product names from products collection when available
    prod_names = {}
    try:
        prod_coll = db['products']
        for pid in list(prod_count.keys()):
            if pid and pid != 'unknown':
                try:
                    lookup = try_objectid(pid)
                    doc = prod_coll.find_one({'_id': lookup})
                except Exception:
                    doc = prod_coll.find_one({'_id': pid})
                if doc and doc.get('name'):
                    prod_names[pid] = doc.get('name')
                else:
                    prod_names[pid] = pid
            else:
                prod_names[pid] = 'Unknown'
    except Exception:
        # fallback: use pid as name
        for pid in prod_count.keys():
            prod_names[pid] = pid

    for pid, v in prod_count.items():
        top_list.append({'policyId': pid, 'policyName': prod_names.get(pid, pid), 'sold': v['sold'], 'revenue': v['revenue'], 'contribution': int((v['revenue']/total_revenue*100) if total_revenue else 0)})
    top_list = sorted(top_list, key=lambda x: x['sold'], reverse=True)
    best = top_list[0] if top_list else None
    
    return JSONResponse({'policiesIssued': policies_issued, 'revenue': revenue, 'bestSellingPolicy': best, 'topPolicies': top_list, 'period': {'from': start.isoformat(), 'to': end.isoformat()}})


@app.get('/api/agents/{agent_id}/dashboard/chart')
def agent_chart(agent_id: str, start: str = None, end: str = None, interval: str = 'day', policyId: str = None):
    db = get_mongo_client().get_database('test')
    if start:
        start_dt = datetime.fromisoformat(start)
    else:
        start_dt, _ = _range_to_dates('1M')
    if end:
        end_dt = datetime.fromisoformat(end)
    else:
        _, end_dt = _range_to_dates('1M')
    apps = list(db['policyapplications'].find({'agentId': agent_id}))
    app_ids = [a.get('_id') for a in apps]
    if not app_ids:
        return JSONResponse({'series': [], 'meta': {'interval': interval, 'points': 0}})
    q = {'applicationId': {'$in': app_ids}, 'createdAt': {'$gte': start_dt, '$lte': end_dt}}
    issuances = list(db['policyissuances'].find(q))
    rows = []
    for ins in issuances:
        dt = ins.get('createdAt')
        date_key = dt.date().isoformat()
        rows.append({'date': date_key, 'revenue': int(ins.get('amount') or 0), 'sales': 1, 'productId': str(ins.get('productId') or '')})
    df = pd.DataFrame(rows)
    if df.empty:
        return JSONResponse({'series': [], 'meta': {'interval': interval, 'points': 0}})
    grp = df.groupby('date').agg({'sales':'sum','revenue':'sum'}).reset_index()
    series = grp.to_dict(orient='records')
    return JSONResponse({'series': series, 'meta': {'interval': interval, 'points': len(series)}})


@app.get('/api/agents/{agent_id}/dashboard/policies')
def agent_policies(agent_id: str, page: int = 1, limit: int = 10, sortBy: str = 'sold', order: str = 'desc', range: str = '1M'):
    db = get_mongo_client().get_database('test')
    start, end = _range_to_dates(range)
    apps = list(db['policyapplications'].find({'agentId': agent_id}))
    app_ids = [a.get('_id') for a in apps]
    if not app_ids:
        return JSONResponse({'data': [], 'pagination': {'page': page, 'limit': limit, 'total': 0}})
    iss_q = {'applicationId': {'$in': app_ids}, 'createdAt': {'$gte': start, '$lte': end}}
    issuances = list(db['policyissuances'].find(iss_q))
    perf = {}
    for ins in issuances:
        pid = str(ins.get('productId') or '')
        entry = perf.setdefault(pid, {'sold':0, 'revenue':0})
        entry['sold'] += 1
    payments = list(db['paymentorders'].find({'applicationId': {'$in': app_ids}, 'createdAt': {'$gte': start, '$lte': end}}))
    for p in payments:
        pid = str(p.get('productId') or '')
        try:
            amt = int(p.get('amount') or 0)
        except Exception:
            amt = 0
        perf.setdefault(pid, {'sold':0, 'revenue':0})
        perf[pid]['revenue'] += amt
    total = sum(v['sold'] for v in perf.values()) if perf else 0
    rows = []
    for pid, v in perf.items():
        rows.append({'policyId': pid, 'policyName': pid, 'icon': None, 'sold': v['sold'], 'revenue': v['revenue'], 'contribution': int((v['revenue']/sum(x['revenue'] for x in perf.values())*100) if perf and sum(x['revenue'] for x in perf.values()) else 0)})
    if sortBy in ('sold','revenue','contribution'):
        rows = sorted(rows, key=lambda x: x.get(sortBy,0), reverse=(order=='desc'))
    total_items = len(rows)
    start_i = (page-1)*limit
    page_rows = rows[start_i:start_i+limit]
    return JSONResponse({'data': page_rows, 'pagination': {'page': page, 'limit': limit, 'total': total_items}})


@app.get('/api/agents/{agent_id}/sales')
def agent_sales(agent_id: str, page: int = 1, limit: int = 50):
    db = get_mongo_client().get_database('test')
    apps = list(db['policyapplications'].find({'agentId': agent_id}))
    app_ids = [a.get('_id') for a in apps]
    payments = list(db['paymentorders'].find({'applicationId': {'$in': app_ids}})) if app_ids else []
    rows = []
    for p in payments:
        rows.append({'id': str(p.get('_id')), 'applicationId': str(p.get('applicationId')), 'amount': p.get('amount'), 'status': p.get('status'), 'createdAt': p.get('createdAt')})
    total = len(rows)
    start_i = (page-1)*limit
    return JSONResponse({'data': rows[start_i:start_i+limit], 'pagination': {'page': page, 'limit': limit, 'total': total}})

@app.get("/excel/{product_id}")
def download_product_excel(product_id: str):
    url = f"{BACKEND_URL}/api/products/active"
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(url)
            products = resp.json().get("data", [])
    except Exception as e:
        return JSONResponse({"error": f"Failed to fetch products: {e}"}, status_code=500)
    product = next((p for p in products if p["_id"] == product_id), None)
    if not product:
        return JSONResponse({"error": "Product not found"}, status_code=404)
    columns = [f["fieldId"]["fieldName"] for f in product["fields"]]
    validations = {}
    for idx, f in enumerate(product["fields"]):
        field_type = f["fieldId"].get("dataType", "")
        options = f["fieldId"].get("options", [])
        if field_type in ["dropdown", "radio", "checkbox"] and options:
            validations[idx] = options
    sample_row = []
    for idx, f in enumerate(product["fields"]):
        field_type = f["fieldId"].get("dataType", "")
        options = f["fieldId"].get("options", [])
        if field_type in ["dropdown", "radio", "checkbox"] and options:
            sample_row.append(options[0])
        elif field_type == "number":
            sample_row.append(12345)
        elif field_type == "adharCard":
            sample_row.append("123412341234")
        elif field_type == "panCard":
            sample_row.append("ABCDE1234F")
        elif field_type == "email":
            sample_row.append("sample@email.com")
        else:
            sample_row.append("Sample Value")
    df = pd.DataFrame([sample_row], columns=columns)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False)
        workbook = writer.book
        worksheet = writer.sheets["Sheet1"]
        for col_idx, opts in validations.items():
            col_letter = chr(ord('A') + col_idx)
            worksheet.data_validation(f'{col_letter}2:{col_letter}101', {
                'validate': 'list',
                'source': opts,
                'input_message': 'Select from dropdown',
            })
    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={product['name'].replace(' ', '_')}.xlsx"
        },
    )

@app.post("/validate-excel/{product_id}")
async def validate_excel(product_id: str, file: UploadFile = File(...)):
    url = f"{BACKEND_URL}/api/products/active"
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(url)
            products = resp.json().get("data", [])
    except Exception as e:
        return JSONResponse({"error": f"Failed to fetch products: {e}"}, status_code=500)
    product = next((p for p in products if p["_id"] == product_id), None)
    if not product:
        return JSONResponse({"error": "Product not found"}, status_code=404)
    contents = await file.read()
    df = pd.read_excel(io.BytesIO(contents))
    field_rules = []
    for f in product["fields"]:
        field_type = f["fieldId"].get("dataType", "text")
        options = f["fieldId"].get("options", [])
        field_rules.append({
            "type": field_type,
            "options": options
        })
    errors = []
    for idx, row in df.iloc[1:].iterrows(): 
        for col_idx, value in enumerate(row):
            rule = field_rules[col_idx]
            t = rule["type"]
            opts = rule["options"]
            val = str(value).strip() if pd.notnull(value) else ""
            if t == "number":
                if val and not val.isdigit():
                    errors.append(f"Row {idx+2}, Column {col_idx+1}: Not a valid number")
            elif t == "email":
                if val and not re.match(r"^[\w\.-]+@[\w\.-]+\.\w+$", val):
                    errors.append(f"Row {idx+2}, Column {col_idx+1}: Not a valid email")
            elif t == "adharCard":
                val_no_space = val.replace(' ', '')
                if val and not re.match(r"^\d{12}$", val_no_space):
                    errors.append(f"Row {idx+2}, Column {col_idx+1}: Not a valid Aadhaar (12 digits, spaces ignored)")
            elif t == "panCard":
                if val and not re.match(r"^[A-Z]{5}\d{4}[A-Z]$", val, re.I):
                    errors.append(f"Row {idx+2}, Column {col_idx+1}: Not a valid PAN (e.g. ABCDE1234F)")
            elif t == "date":
                if val:
                    try:
                        pd.to_datetime(val)
                    except Exception:
                        errors.append(f"Row {idx+2}, Column {col_idx+1}: Not a valid date")
            elif t in ["dropdown", "radio", "checkbox"]:
                if val and val not in opts:
                    errors.append(f"Row {idx+2}, Column {col_idx+1}: Value '{val}' not in allowed options")
            # text: always valid
    if errors:
        return JSONResponse({"valid": False, "errors": errors})
    return JSONResponse({"valid": True, "message": "Excel is valid."})
