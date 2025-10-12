import os
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
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
app = FastAPI(title="iCash - Owner's Dashboard")
templates = Jinja2Templates(directory="templates")


# --- API Endpoint to Fetch Stats ---
@app.get("/api/stats")
async def get_stats():
    """Fetches all required statistics from the database and returns as JSON."""
    stats = {}
    with engine.connect() as conn:
        # 1. Number of unique shoppers
        unique_shoppers_query = text("SELECT COUNT(DISTINCT user_id) FROM purchases;")
        stats['unique_shoppers'] = conn.execute(unique_shoppers_query).scalar()

        # 2. List of "loyal" shoppers (>= 3 purchases)
        loyal_shoppers_query = text("""
            SELECT user_id, COUNT(purchase_id) as purchase_count
            FROM purchases
            GROUP BY user_id
            HAVING COUNT(purchase_id) >= 3
            ORDER BY purchase_count DESC;
        """)
        loyal_shoppers = conn.execute(loyal_shoppers_query).fetchall()
        # Convert list of tuples to list of dicts for easier JSON handling
        stats['loyal_shoppers'] = [row._asdict() for row in loyal_shoppers]


        # 3. List of the 3 best-selling products of all time
        top_products_query = text("""
            SELECT p.product_name, COUNT(pi.product_id) as sales_count
            FROM purchase_items pi
            JOIN products p ON pi.product_id = p.product_id
            GROUP BY p.product_name
            ORDER BY sales_count DESC;
        """)
        all_products = conn.execute(top_products_query).fetchall()

        # Logic to handle ties for the 3rd spot
        top_products = []
        if all_products:
            third_place_sales_count = 0
            if len(all_products) >= 3:
                third_place_sales_count = all_products[2][1] # Get sales count of the 3rd item
            
            for product in all_products:
                if len(top_products) < 3 or product[1] >= third_place_sales_count:
                    top_products.append({"name": product[0], "sales": product[1]})
                else:
                    break
        
        stats['top_products'] = top_products

    return JSONResponse(content=stats)


# --- HTML Frontend ---
@app.get("/", response_class=HTMLResponse)
async def get_dashboard_ui(request: Request):
    """Serves the main HTML page for the dashboard UI."""
    return templates.TemplateResponse("index.html", {"request": request})