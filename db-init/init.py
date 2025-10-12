import os
import time
import pandas as pd
from sqlalchemy import create_engine, text

# --- Database Connection Details ---
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"

# --- Helper function to wait for the database to be ready ---
def wait_for_db():
    """Waits for the database to become available."""
    max_retries = 10
    retry_delay = 5
    for i in range(max_retries):
        try:
            engine = create_engine(DB_URL)
            with engine.connect():
                print("✅ Database connection successful!")
                return engine
        except Exception:
            print(f"⏳ Database not ready yet (attempt {i+1}/{max_retries})... Retrying in {retry_delay}s.")
            time.sleep(retry_delay)
    print("❌ Could not connect to the database. Exiting.")
    exit(1)

# --- Main Initialization Logic ---
def initialize_database(engine):
    """Creates tables and loads data from CSV files if tables are empty."""
    with engine.connect() as conn:
        print("🚀 Starting database initialization...")

        # --- Create tables in the correct order ---
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS products (
                product_id SERIAL PRIMARY KEY,
                product_name VARCHAR(255) NOT NULL UNIQUE,
                unit_price NUMERIC(10, 2) NOT NULL
            );
        """))
        print("✔️ 'products' table created.")

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS purchases (
            purchase_id SERIAL PRIMARY KEY,
            supermarket_id VARCHAR(255) NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            user_id VARCHAR(255) NOT NULL
        );
    """))
        print("✔️ 'purchases' table created.")

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS purchase_items (
                purchase_id INT REFERENCES purchases(purchase_id),
                product_id INT REFERENCES products(product_id),
                PRIMARY KEY (purchase_id, product_id)
            );
        """))
        print("✔️ 'purchase_items' table created.")
        
        conn.commit()

        # --- Data Loading ---
        with conn.begin() as transaction:
            # Load products if the table is empty
            if conn.execute(text("SELECT COUNT(*) FROM products;")).scalar() == 0:
                print("📂 'products' table is empty. Loading data...")
                products_df = pd.read_csv("data/products_list.csv")
                products_df.to_sql('products', conn, if_exists='append', index=False)
                print(f"✅ Loaded {len(products_df)} products.")
            else:
                print("ℹ️ 'products' table already has data.")

            # After loading, create a map of product_name -> product_id
            product_map_result = conn.execute(text("SELECT product_name, product_id FROM products;")).fetchall()
            name_to_id_map = {name: pid for name, pid in product_map_result}
            print(f"🗺️ Created product name-to-ID map: {name_to_id_map}")

            # Load historical purchases if the table is empty
            if conn.execute(text("SELECT COUNT(*) FROM purchases;")).scalar() == 0:
                print("📂 'purchases' table is empty. Loading historical data...")
                purchases_df = pd.read_csv("data/purchases.csv")

                for index, row in purchases_df.iterrows():
                    # Insert the main purchase record
                    purchase_insert = conn.execute(text("""
                        INSERT INTO purchases (supermarket_id, timestamp, user_id)
                        VALUES (:sid, :ts, :uid) RETURNING purchase_id;
                    """), {"sid": row['supermarket_id'], "ts": row['timestamp'], "uid": row['user_id']})
                    
                    new_purchase_id = purchase_insert.scalar()

                    # Use the map to find IDs for product names
                    product_names = [name.strip() for name in str(row['items_list']).split(',')]
                    for name in product_names:
                        product_id = name_to_id_map.get(name)
                        if product_id:
                            conn.execute(text("""
                                INSERT INTO purchase_items (purchase_id, product_id)
                                VALUES (:pid, :prod_id);
                            """), {"pid": new_purchase_id, "prod_id": product_id})
                        else:
                            print(f"⚠️ Warning: Product '{name}' from Purchases.csv not found in product list.")
                print(f"✅ Loaded {len(purchases_df)} historical purchases.")
            else:
                print("ℹ️ 'purchases' table already has data.")

        print("🎉 Database initialization complete!")

if __name__ == "__main__":
    db_engine = wait_for_db()
    if db_engine:
        initialize_database(db_engine)