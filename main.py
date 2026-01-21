
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse
import httpx
import io
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8080")
print(BACKEND_URL)
app = FastAPI()

# Allow CORS for all origins (for development)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
@app.get("/")
def serve_index():
    index_path = os.path.join(os.path.dirname(__file__), "index.html")
    return FileResponse(index_path, media_type="text/html")




@app.get("/api/products/active")
def get_active_products():
    # Proxy the request to the staging backend
    url = f"{BACKEND_URL}/api/products/active"
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(url)
            return JSONResponse(resp.json(), status_code=resp.status_code)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/excel/{product_id}")
def download_product_excel(product_id: str):
    # Fetch product list from backend
    url = f"{BACKEND_URL}/api/products/active"
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(url)
            products = resp.json().get("data", [])
    except Exception as e:
        return JSONResponse({"error": f"Failed to fetch products: {e}"}, status_code=500)
    # Find product
    product = next((p for p in products if p["_id"] == product_id), None)
    if not product:
        return JSONResponse({"error": "Product not found"}, status_code=404)
    # Prepare columns from fields
    columns = [f["fieldId"]["fieldName"] for f in product["fields"]]
    # Create empty DataFrame with columns
    df = pd.DataFrame(columns=columns)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={product['name'].replace(' ', '_')}.xlsx"
        },
    )
