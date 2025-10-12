import os
import uuid
from datetime import datetime
from typing import List

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, text

# --- Database Connection ---
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
engine = create_engine(DB_URL)

# --- FastAPI App Setup ---
# This line creates the 'app' variable that uvicorn is looking for.
app = FastAPI(title="iCash - Cash Register")
templates = Jinja2Templates(directory="templates")

# --- API Endpoints ---
@app.get("/", response_class=HTMLResponse)
async def get_cash_register_ui(request: Request):
    """Serves the main HTML page for the cash register UI."""
    with engine.connect() as conn:
        # Use a more robust way to fetch results into a list of dictionaries
        products_result = conn.execute(text("SELECT * FROM products ORDER BY product_id;")).mappings().all()
        return templates.TemplateResponse(
            "index.html",
            {"request": request, "products": products_result, "supermarkets": ["SMKT001", "SMKT002", "SMKT003"]}
        )

@app.post("/submit_purchase")
async def submit_purchase(
    request: Request,
    supermarket_id: str = Form(...),
    user_id: str = Form(...),
    is_new_customer: bool = Form(False),
    items: List[int] = Form(...)
):
    """Receives purchase data from the form, calculates total, and saves to the DB."""
    # 1. Determine User ID
    customer_id = str(uuid.uuid4()) if is_new_customer or not user_id else user_id

    with engine.connect() as conn:
        # 2. Begin a transaction
        with conn.begin() as transaction:
            try:
                # 3. Insert the main purchase record
                purchase_time = datetime.now()
                result = conn.execute(text("""
                    INSERT INTO purchases (supermarket_id, timestamp, user_id)
                    VALUES (:supermarket_id, :timestamp, :user_id) RETURNING purchase_id;
                """), {
                    "supermarket_id": supermarket_id,
                    "timestamp": purchase_time,
                    "user_id": customer_id
                })
                purchase_id = result.scalar()

                # 4. Insert each item from the purchase into the junction table
                for item_id in items:
                    conn.execute(text("""
                        INSERT INTO purchase_items (purchase_id, product_id)
                        VALUES (:purchase_id, :product_id);
                    """), { "purchase_id": purchase_id, "product_id": item_id })
                
                print(f"✅ Successfully recorded purchase {purchase_id} for user {customer_id}.")

            except Exception as e:
                print(f"❌ Error during purchase submission: {e}")
                transaction.rollback() # Rollback on error
                products_result = conn.execute(text("SELECT * FROM products ORDER BY product_id;")).mappings().all()
                return templates.TemplateResponse(
                    "index.html",
                    {
                        "request": request, 
                        "products": products_result, 
                        "supermarkets": ["SMKT001", "SMKT002", "SMKT003"],
                        "error": "Failed to process purchase."
                    },
                    status_code=500
                )

    # Re-fetch products to render the page again
    with engine.connect() as conn:
        products_result = conn.execute(text("SELECT * FROM products ORDER BY product_id;")).mappings().all()
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "products": products_result,
                "supermarkets": ["SMKT001", "SMKT002", "SMKT003"],
                "success_message": f"Purchase recorded for user {customer_id}!"
            }
        )