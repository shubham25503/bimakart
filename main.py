
import os
import io
import re
import httpx
import pandas as pd
from fastapi import FastAPI, Response, UploadFile, File
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()
BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8080")

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
