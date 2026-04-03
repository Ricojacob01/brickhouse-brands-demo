# Databricks notebook source
# MAGIC %md
# MAGIC # Brickhouse Brands - Lakebase Database Seed Notebook
# MAGIC
# MAGIC This notebook seeds the Lakebase database for the Brickhouse Brands demo app.
# MAGIC It connects to the Lakebase instance and populates products, stores, users, inventory, and orders.

# COMMAND ----------

# Install required packages
%pip install psycopg2-binary python-dateutil numpy tqdm

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Generate a Lakebase database credential using the Databricks SDK
from databricks.sdk import WorkspaceClient
import uuid

w = WorkspaceClient()

# Generate credential for the Lakebase instance
resp = w.api_client.do(
    "POST",
    "/api/2.0/database/credential",
    body={
        "request_id": str(uuid.uuid4()),
        "instance_names": ["brickhouse-brands-v2"],
    },
)

db_token = resp["token"]
db_expiration = resp["expiration_time"]
print(f"Credential generated, expires: {db_expiration}")

# Get current user for DB connection
me = w.current_user.me()
db_user = me.user_name
print(f"DB user: {db_user}")

# COMMAND ----------

# Database connection config
DB_HOST = "ep-delicate-dust-d2rwrvq1.database.us-east-1.cloud.databricks.com"
DB_PORT = 5432
DB_NAME = "databricks_postgres"
DB_USER = db_user
DB_PASS = db_token

# COMMAND ----------

import psycopg2
from psycopg2.extras import execute_batch
import random
from datetime import datetime, timedelta

random.seed(42)

def get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS, sslmode="require"
    )

# Test connection
conn = get_connection()
cur = conn.cursor()
cur.execute("SELECT 1")
print("Connection successful:", cur.fetchone())
cur.close()
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schema

# COMMAND ----------

conn = get_connection()
cur = conn.cursor()

# Create tables
cur.execute("""
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    brand VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    package_size VARCHAR(50) NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS stores (
    store_id SERIAL PRIMARY KEY,
    store_name VARCHAR(255) NOT NULL,
    store_code VARCHAR(20) UNIQUE NOT NULL,
    address TEXT NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(2) NOT NULL,
    zip_code VARCHAR(10) NOT NULL,
    region VARCHAR(50) NOT NULL,
    store_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    role VARCHAR(50) NOT NULL CHECK (role IN ('store_manager', 'regional_manager')),
    store_id INTEGER REFERENCES stores(store_id),
    region VARCHAR(50),
    avatar_url VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS inventory (
    inventory_id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    store_id INTEGER REFERENCES stores(store_id),
    quantity_cases INTEGER NOT NULL DEFAULT 0,
    reserved_cases INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    version INTEGER DEFAULT 1,
    UNIQUE(product_id, store_id)
);
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    order_number VARCHAR(50) UNIQUE NOT NULL,
    from_store_id INTEGER REFERENCES stores(store_id),
    to_store_id INTEGER NOT NULL REFERENCES stores(store_id),
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    quantity_cases INTEGER NOT NULL,
    order_status VARCHAR(50) NOT NULL DEFAULT 'pending_review'
        CHECK (order_status IN ('pending_review', 'approved', 'fulfilled', 'cancelled')),
    requested_by INTEGER NOT NULL REFERENCES users(user_id),
    approved_by INTEGER REFERENCES users(user_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    approved_date TIMESTAMP,
    fulfilled_date TIMESTAMP,
    notes TEXT,
    version INTEGER DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_inventory_store_product ON inventory(store_id, product_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(order_status);
CREATE INDEX IF NOT EXISTS idx_orders_store ON orders(to_store_id);
""")
conn.commit()
cur.close()
conn.close()
print("Schema created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Populate Static Data (Products, Stores, Users)

# COMMAND ----------

conn = get_connection()
cur = conn.cursor()

# Clear existing data
cur.execute("TRUNCATE TABLE orders, inventory, users, stores, products RESTART IDENTITY CASCADE;")

# Products data (156 beverage products)
products_data = [
    ("Fizzy Classic Cola", "BubbleCorp", "Cola", "24x12oz cans", 18.99),
    ("Thunder Cola", "Lightning Beverages", "Cola", "24x12oz cans", 18.49),
    ("Zero Splash Diet Cola", "BubbleCorp", "Cola", "24x12oz cans", 18.99),
    ("Lightning Zero Sugar", "Lightning Beverages", "Cola", "24x12oz cans", 18.49),
    ("Cherry Burst Cola", "BubbleCorp", "Cola", "24x12oz cans", 19.49),
    ("Professor Fizz", "Vintage Soda Co", "Cola", "24x12oz cans", 18.79),
    ("Vanilla Dream Cola", "BubbleCorp", "Cola", "24x12oz cans", 19.29),
    ("Retro Classic Cola", "Vintage Soda Co", "Cola", "24x12oz cans", 17.99),
    ("Max Power Cola", "Lightning Beverages", "Cola", "24x12oz cans", 19.99),
    ("Royal Crown Cola", "Premium Sodas", "Cola", "24x12oz cans", 20.49),
    ("Crystal Lime", "BubbleCorp", "Citrus", "24x12oz cans", 17.99),
    ("Lucky Lemon", "Vintage Soda Co", "Citrus", "24x12oz cans", 17.79),
    ("Peak Citrus Rush", "Lightning Beverages", "Citrus", "24x12oz cans", 19.49),
    ("Misty Lime Splash", "Lightning Beverages", "Citrus", "24x12oz cans", 17.99),
    ("Golden Orange Burst", "Vintage Soda Co", "Citrus", "24x12oz cans", 18.29),
    ("Zesty Grapefruit", "BubbleCorp", "Citrus", "24x12oz cans", 18.49),
    ("Tropical Citrus Blend", "Lightning Beverages", "Citrus", "24x12oz cans", 19.79),
    ("Lemon Lime Twist", "Fresh Fizz Co", "Citrus", "24x12oz cans", 17.89),
    ("Blood Orange Sensation", "Premium Sodas", "Citrus", "24x12oz cans", 20.99),
    ("Lime Mint Fusion", "BubbleCorp", "Citrus", "24x12oz cans", 18.79),
    ("Root Beer Classic", "BubbleCorp", "Soda", "24x12oz cans", 18.49),
    ("Cream Soda Delight", "Vintage Soda Co", "Soda", "24x12oz cans", 18.79),
    ("Ginger Ale Supreme", "Lightning Beverages", "Soda", "24x12oz cans", 17.99),
    ("Orange Soda Pop", "BubbleCorp", "Soda", "24x12oz cans", 17.49),
    ("Grape Soda Fizz", "Fruit Fizz Co", "Soda", "24x12oz cans", 17.29),
    ("Black Cherry Soda", "Premium Sodas", "Soda", "24x12oz cans", 19.49),
    ("Strawberry Cream Soda", "Vintage Soda Co", "Soda", "24x12oz cans", 18.99),
    ("Pineapple Soda", "Tropical Drinks Co", "Soda", "24x12oz cans", 18.29),
    ("Dr. Pepper Style", "Unique Sodas", "Soda", "24x12oz cans", 18.89),
    ("Birch Beer Original", "Craft Soda Co", "Soda", "24x12oz cans", 19.99),
    ("Pure Stream Water", "BubbleCorp", "Water", "24x16.9oz bottles", 12.99),
    ("Aqua Fresh Water", "Lightning Beverages", "Water", "24x16.9oz bottles", 12.79),
    ("Crystal Smart Water", "BubbleCorp", "Water", "24x16.9oz bottles", 24.99),
    ("Mountain Spring Water", "Alpine Waters", "Water", "24x16.9oz bottles", 32.99),
    ("Glacier Pure Water", "Arctic Springs", "Water", "24x16.9oz bottles", 28.99),
    ("Alkaline Spring Water", "Pure Life Co", "Water", "24x16.9oz bottles", 35.99),
    ("Artesian Well Water", "Premium Waters", "Water", "24x16.9oz bottles", 38.99),
    ("Electrolyte Enhanced Water", "BubbleCorp", "Water", "24x16.9oz bottles", 29.99),
    ("Mineral Rich Water", "Natural Springs", "Water", "24x16.9oz bottles", 31.99),
    ("Purified Drinking Water", "Hydro Pure", "Water", "24x16.9oz bottles", 11.99),
    ("Lemon Cucumber Water", "BubbleCorp", "Flavored Water", "24x16.9oz bottles", 19.99),
    ("Berry Mint Infusion", "Lightning Beverages", "Flavored Water", "24x16.9oz bottles", 20.49),
    ("Watermelon Basil Water", "Fresh Infusions", "Flavored Water", "24x16.9oz bottles", 21.99),
    ("Peach Ginger Water", "Natural Flavor Co", "Flavored Water", "24x16.9oz bottles", 20.99),
    ("Lime Coconut Water", "Tropical Waters", "Flavored Water", "24x16.9oz bottles", 22.49),
    ("Strawberry Lemon Water", "BubbleCorp", "Flavored Water", "24x16.9oz bottles", 19.79),
    ("Raspberry Lime Water", "Berry Fresh Co", "Flavored Water", "24x16.9oz bottles", 21.29),
    ("Mango Passion Water", "Exotic Waters", "Flavored Water", "24x16.9oz bottles", 23.99),
    ("Classic Sparkling Water", "BubbleCorp", "Sparkling Water", "24x12oz cans", 16.99),
    ("Lime Sparkling Water", "Lightning Beverages", "Sparkling Water", "24x12oz cans", 17.49),
    ("Grapefruit Sparkling Water", "Citrus Bubbles", "Sparkling Water", "24x12oz cans", 17.99),
    ("Berry Sparkling Water", "Fresh Bubbles", "Sparkling Water", "24x12oz cans", 18.29),
    ("Orange Sparkling Water", "BubbleCorp", "Sparkling Water", "24x12oz cans", 17.79),
    ("Cucumber Mint Sparkling", "Premium Bubbles", "Sparkling Water", "24x12oz cans", 19.49),
    ("Watermelon Sparkling Water", "Summer Fizz Co", "Sparkling Water", "24x12oz cans", 18.99),
    ("Peach Sparkling Water", "Orchard Bubbles", "Sparkling Water", "24x12oz cans", 18.49),
    ("Hydro Punch", "Lightning Beverages", "Sports Drink", "12x32oz bottles", 24.99),
    ("Hydro Lemon-Lime", "Lightning Beverages", "Sports Drink", "12x32oz bottles", 24.99),
    ("Energy Wave Blue", "BubbleCorp", "Sports Drink", "12x32oz bottles", 23.99),
    ("Energy Wave Red", "BubbleCorp", "Sports Drink", "12x32oz bottles", 23.99),
    ("Muscle Fuel Strawberry", "BubbleCorp", "Sports Drink", "12x16oz bottles", 26.99),
    ("Electro Orange", "Athletic Performance", "Sports Drink", "12x32oz bottles", 25.49),
    ("Power Grape", "Lightning Beverages", "Sports Drink", "12x32oz bottles", 24.79),
    ("Tropical Punch Sport", "Island Athletics", "Sports Drink", "12x32oz bottles", 25.99),
    ("Coconut Water Sport", "Natural Athletes", "Sports Drink", "12x16oz bottles", 29.99),
    ("Recovery Formula", "Pro Sports Nutrition", "Sports Drink", "12x20oz bottles", 31.99),
    ("Thunder Bolt Energy", "Storm Drinks", "Energy Drink", "24x8.4oz cans", 45.99),
    ("Beast Mode Energy", "Wild Energy Co", "Energy Drink", "24x16oz cans", 52.99),
    ("Rocket Fuel Energy", "Lightning Beverages", "Energy Drink", "24x16oz cans", 48.99),
    ("Blast Energy Blue Razz", "Explosive Drinks", "Energy Drink", "12x16oz cans", 34.99),
    ("Voltage Energy", "Electric Drinks", "Energy Drink", "24x8.4oz cans", 46.99),
    ("Adrenaline Rush", "Extreme Energy", "Energy Drink", "24x16oz cans", 54.99),
    ("Zero Sugar Lightning", "Lightning Beverages", "Energy Drink", "24x16oz cans", 49.99),
    ("Natural Energy Boost", "Organic Energy Co", "Energy Drink", "12x12oz cans", 38.99),
    ("Tropical Energy Blast", "Island Energy", "Energy Drink", "24x8.4oz cans", 47.99),
    ("Coffee Energy Fusion", "Cafe Energy", "Energy Drink", "12x16oz cans", 36.99),
    ("Sunrise Orange", "BubbleCorp", "Juice", "12x59oz bottles", 28.99),
    ("Tropical Burst Orange", "Lightning Beverages", "Juice", "12x59oz bottles", 29.99),
    ("Simply Fresh Orange", "BubbleCorp", "Juice", "12x52oz bottles", 32.99),
    ("Berry Coast Cranberry", "Coastal Juices", "Juice", "12x64oz bottles", 35.99),
    ("Apple Orchard Fresh", "Farm Fresh Juices", "Juice", "12x64oz bottles", 33.99),
    ("Grape Vineyard", "Premium Juices", "Juice", "12x59oz bottles", 36.99),
    ("Pineapple Paradise", "Tropical Juices", "Juice", "12x46oz bottles", 31.99),
    ("Pomegranate Power", "Antioxidant Juices", "Juice", "12x32oz bottles", 42.99),
    ("Mango Madness", "Exotic Juices", "Juice", "12x46oz bottles", 34.99),
    ("Mixed Berry Blend", "Berry Best Juices", "Juice", "12x59oz bottles", 37.99),
    ("Grapefruit Fresh", "Citrus Grove", "Juice", "12x52oz bottles", 30.99),
    ("Watermelon Fresh", "Summer Harvest", "Juice", "12x46oz bottles", 29.99),
    ("Cafe Chill Frappuccino", "Coffee House Co", "Coffee", "12x13.7oz bottles", 24.99),
    ("Morning Rush Cold Brew", "Donut Cafe", "Coffee", "12x13.7oz bottles", 22.99),
    ("Vanilla Latte RTD", "Premium Coffee Co", "Coffee", "12x13.7oz bottles", 26.99),
    ("Mocha Madness", "Chocolate Coffee Co", "Coffee", "12x13.7oz bottles", 25.99),
    ("Caramel Macchiato", "Sweet Coffee Co", "Coffee", "12x13.7oz bottles", 27.49),
    ("Iced Coffee Classic", "BubbleCorp", "Coffee", "12x16oz bottles", 23.99),
    ("Espresso Shot RTD", "Intense Coffee Co", "Coffee", "12x8oz bottles", 29.99),
    ("Nitro Cold Brew", "Craft Coffee Co", "Coffee", "12x11oz cans", 31.99),
    ("Protein Coffee Shake", "Fitness Coffee Co", "Coffee", "12x14oz bottles", 34.99),
    ("Coconut Coffee Cooler", "Tropical Coffee Co", "Coffee", "12x13.7oz bottles", 28.99),
    ("Golden Leaf Sweet Tea", "BubbleCorp", "Tea", "12x18.5oz bottles", 19.99),
    ("Nature Brew Green Tea", "Lightning Beverages", "Tea", "12x18.5oz bottles", 20.99),
    ("Chai Spice Latte", "Spice Tea Co", "Tea", "12x13.7oz bottles", 23.99),
    ("Matcha Green Tea", "Zen Tea Co", "Tea", "12x16oz bottles", 27.99),
    ("Peach Iced Tea", "Southern Tea Co", "Tea", "12x18.5oz bottles", 21.99),
    ("Raspberry Iced Tea", "Berry Tea Co", "Tea", "12x18.5oz bottles", 22.49),
    ("White Tea Antioxidant", "Pure Tea Co", "Tea", "12x16oz bottles", 25.99),
    ("Hibiscus Herbal Tea", "Herbal Blends Co", "Tea", "12x16oz bottles", 24.99),
    ("Earl Grey Iced Tea", "Classic Tea Co", "Tea", "12x18.5oz bottles", 23.49),
    ("Lemon Ginger Tea", "Wellness Tea Co", "Tea", "12x16oz bottles", 26.49),
    ("Ginger Lemon Kombucha", "Probiotic Brew Co", "Kombucha", "12x16oz bottles", 35.99),
    ("Berry Blast Kombucha", "Fermented Fresh", "Kombucha", "12x16oz bottles", 36.99),
    ("Green Tea Kombucha", "Zen Brew Co", "Kombucha", "12x16oz bottles", 34.99),
    ("Hibiscus Rose Kombucha", "Floral Ferments", "Kombucha", "12x16oz bottles", 38.99),
    ("Pineapple Turmeric Kombucha", "Tropical Cultures", "Kombucha", "12x16oz bottles", 37.99),
    ("Original GT Kombucha", "GT Brewing Co", "Kombucha", "12x16oz bottles", 39.99),
    ("Lavender Kombucha", "Artisan Ferments", "Kombucha", "12x16oz bottles", 41.99),
    ("Watermelon Mint Kombucha", "Summer Cultures", "Kombucha", "12x16oz bottles", 36.49),
    ("Almond Milk Original", "Nut Milk Co", "Plant Milk", "12x32oz cartons", 41.99),
    ("Oat Milk Vanilla", "Grain Beverages", "Plant Milk", "12x32oz cartons", 43.99),
    ("Coconut Milk Unsweetened", "Tropical Milk Co", "Plant Milk", "12x32oz cartons", 39.99),
    ("Soy Milk Chocolate", "Bean Beverages", "Plant Milk", "12x32oz cartons", 38.99),
    ("Cashew Milk Original", "Premium Nut Co", "Plant Milk", "12x32oz cartons", 45.99),
    ("Rice Milk Vanilla", "Grain Goodness Co", "Plant Milk", "12x32oz cartons", 37.99),
    ("Hemp Milk Original", "Hemp Harvest Co", "Plant Milk", "12x32oz cartons", 47.99),
    ("Pea Protein Milk", "Plant Protein Co", "Plant Milk", "12x32oz cartons", 49.99),
    ("Immune Boost Citrus", "Wellness Drinks", "Functional", "12x16oz bottles", 31.99),
    ("Probiotic Berry Blend", "Gut Health Co", "Functional", "12x16oz bottles", 33.99),
    ("Collagen Beauty Water", "Beauty Beverages", "Functional", "12x16oz bottles", 45.99),
    ("Brain Boost Blueberry", "Cognitive Drinks", "Functional", "12x16oz bottles", 38.99),
    ("Detox Green Juice", "Cleanse Co", "Functional", "12x16oz bottles", 42.99),
    ("Energy & Focus", "Mental Performance", "Functional", "12x16oz bottles", 39.99),
    ("Sleep Support Chamomile", "Rest Beverages", "Functional", "12x16oz bottles", 34.99),
    ("Hydration Plus", "Electrolyte Plus Co", "Functional", "12x16oz bottles", 29.99),
    ("Chocolate Protein Shake", "Muscle Nutrition", "Protein", "12x14oz bottles", 44.99),
    ("Vanilla Protein Smoothie", "Fit Beverages", "Protein", "12x14oz bottles", 43.99),
    ("Strawberry Protein Drink", "Athletic Nutrition", "Protein", "12x14oz bottles", 44.49),
    ("Cookies & Cream Protein", "Dessert Nutrition", "Protein", "12x14oz bottles", 46.99),
    ("Coffee Protein Fusion", "Caffeine Gains", "Protein", "12x14oz bottles", 47.99),
    ("Plant Protein Vanilla", "Vegan Gains Co", "Protein", "12x14oz bottles", 48.99),
    ("Banana Protein Smoothie", "Tropical Gains", "Protein", "12x14oz bottles", 45.49),
    ("Chocolate Peanut Protein", "Nut Gains Co", "Protein", "12x14oz bottles", 49.99),
    ("Vitamin C Orange", "Vitamin Waters Co", "Vitamin Water", "24x20oz bottles", 39.99),
    ("B-Complex Berry", "Energy Vitamins", "Vitamin Water", "24x20oz bottles", 41.99),
    ("Vitamin D Lemon", "Sunshine Vitamins", "Vitamin Water", "24x20oz bottles", 40.99),
    ("Multi-Vitamin Tropical", "Complete Nutrition", "Vitamin Water", "24x20oz bottles", 43.99),
    ("Antioxidant Pomegranate", "Antioxidant Waters", "Vitamin Water", "24x20oz bottles", 44.99),
    ("Calcium Citrus", "Bone Health Co", "Vitamin Water", "24x20oz bottles", 42.99),
    ("Iron Plus Cherry", "Mineral Waters Co", "Vitamin Water", "24x20oz bottles", 41.49),
    ("Magnesium Mint", "Relaxation Waters", "Vitamin Water", "24x20oz bottles", 40.49),
    ("Virgin Mojito", "Mocktail Masters", "Mocktail", "12x12oz bottles", 27.99),
    ("Sparkling Cranberry", "Celebration Drinks", "Mocktail", "12x12oz bottles", 25.99),
    ("Ginger Moscow Mule", "Premium Mocktails", "Mocktail", "12x12oz bottles", 29.99),
    ("Virgin Pina Colada", "Tropical Mocktails", "Mocktail", "12x12oz bottles", 31.99),
    ("Sparkling Grape Juice", "Festive Beverages", "Mocktail", "12x12oz bottles", 24.99),
    ("Virgin Bloody Mary", "Savory Mocktails", "Mocktail", "12x12oz bottles", 28.99),
    ("Cucumber Gin & Tonic", "Garden Mocktails", "Mocktail", "12x12oz bottles", 30.99),
    ("Elderflower Spritz", "Floral Mocktails", "Mocktail", "12x12oz bottles", 32.99),
]

cur.executemany(
    "INSERT INTO products (product_name, brand, category, package_size, unit_price) VALUES (%s, %s, %s, %s, %s)",
    products_data,
)

# Stores data (26 locations)
stores_data = [
    ("Headquarters Warehouse", "HQ000", "1000 Corporate Blvd", "Atlanta", "GA", "30328", "Southeast", "Warehouse"),
    ("Downtown Chicago Store", "CHI001", "123 Michigan Ave", "Chicago", "IL", "60601", "Midwest", "Urban"),
    ("Suburban Dallas Store", "DAL002", "456 Preston Rd", "Dallas", "TX", "75201", "South", "Suburban"),
    ("Los Angeles Metro", "LAX003", "789 Sunset Blvd", "Los Angeles", "CA", "90028", "West", "Urban"),
    ("Miami Beach Store", "MIA004", "321 Ocean Dr", "Miami", "FL", "33139", "Southeast", "Tourist"),
    ("Seattle Downtown", "SEA005", "654 Pine St", "Seattle", "WA", "98101", "Northwest", "Urban"),
    ("Atlanta Midtown", "ATL006", "987 Peachtree St", "Atlanta", "GA", "30309", "Southeast", "Urban"),
    ("Phoenix Central", "PHX007", "147 Central Ave", "Phoenix", "AZ", "85004", "Southwest", "Urban"),
    ("Boston Commons", "BOS008", "258 Tremont St", "Boston", "MA", "02116", "Northeast", "Urban"),
    ("Denver Tech Center", "DEN009", "369 Tech Center Dr", "Denver", "CO", "80237", "Mountain", "Business"),
    ("Nashville Music Row", "NSH010", "741 Music Square", "Nashville", "TN", "37203", "South", "Entertainment"),
    ("Houston Galleria", "HOU011", "5085 Westheimer Rd", "Houston", "TX", "77056", "South", "Shopping"),
    ("Las Vegas Strip", "LAS012", "3200 Las Vegas Blvd", "Las Vegas", "NV", "89109", "West", "Tourist"),
    ("Orlando Theme Park", "ORL013", "1234 International Dr", "Orlando", "FL", "32819", "Southeast", "Tourist"),
    ("Minneapolis Skyway", "MSP014", "800 Nicollet Mall", "Minneapolis", "MN", "55402", "Midwest", "Urban"),
    ("Portland Pearl District", "PDX015", "1420 NW Lovejoy St", "Portland", "OR", "97209", "Northwest", "Urban"),
    ("San Francisco Union Square", "SFO016", "345 Stockton St", "San Francisco", "CA", "94108", "West", "Urban"),
    ("New York Times Square", "NYC017", "1500 Broadway", "New York", "NY", "10036", "Northeast", "Tourist"),
    ("Charlotte Uptown", "CLT018", "222 S Tryon St", "Charlotte", "NC", "28202", "Southeast", "Business"),
    ("Kansas City Plaza", "MCI019", "4750 Broadway", "Kansas City", "MO", "64112", "Midwest", "Shopping"),
    ("Salt Lake City Downtown", "SLC020", "50 W Broadway", "Salt Lake City", "UT", "84101", "Mountain", "Urban"),
    ("Tampa Bay Area", "TPA021", "2223 N Westshore Blvd", "Tampa", "FL", "33607", "Southeast", "Business"),
    ("San Diego Gaslamp", "SAN022", "555 Fifth Ave", "San Diego", "CA", "92101", "West", "Urban"),
    ("Philadelphia Center City", "PHL023", "1234 Market St", "Philadelphia", "PA", "19107", "Northeast", "Urban"),
    ("Detroit Renaissance", "DTW024", "400 Renaissance Dr", "Detroit", "MI", "48243", "Midwest", "Urban"),
    ("Austin Downtown", "AUS025", "98 Red River St", "Austin", "TX", "78701", "South", "Entertainment"),
]

cur.executemany(
    "INSERT INTO stores (store_name, store_code, address, city, state, zip_code, region, store_type) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
    stores_data,
)

# Users data (25 store managers + 8 regional managers)
users_data = [
    ("hsmith", "holly.smith@cpg.com", "Holly", "Smith", "store_manager", 1, "Midwest", "https://media.licdn.com/dms/image/v2/D4E03AQHhGDs3Yw0c8w/profile-displayphoto-shrink_400_400/0/1702582166483"),
    ("mjohnson", "mary.johnson@cpg.com", "Mary", "Johnson", "store_manager", 2, "South", "https://randomuser.me/api/portraits/women/2.jpg"),
    ("bwilliams", "bob.williams@cpg.com", "Bob", "Williams", "store_manager", 3, "West", "https://randomuser.me/api/portraits/men/1.jpg"),
    ("sgonzalez", "sofia.gonzalez@cpg.com", "Sofia", "Gonzalez", "store_manager", 4, "Southeast", "https://randomuser.me/api/portraits/women/3.jpg"),
    ("dchen", "david.chen@cpg.com", "David", "Chen", "store_manager", 5, "Northwest", "https://randomuser.me/api/portraits/men/2.jpg"),
    ("lbrown", "lisa.brown@cpg.com", "Lisa", "Brown", "store_manager", 6, "Southeast", "https://randomuser.me/api/portraits/women/4.jpg"),
    ("rgarcia", "robert.garcia@cpg.com", "Robert", "Garcia", "store_manager", 7, "Southwest", "https://randomuser.me/api/portraits/men/3.jpg"),
    ("kmiller", "karen.miller@cpg.com", "Karen", "Miller", "store_manager", 8, "Northeast", "https://randomuser.me/api/portraits/women/5.jpg"),
    ("tdavis", "tom.davis@cpg.com", "Tom", "Davis", "store_manager", 9, "Mountain", "https://randomuser.me/api/portraits/men/4.jpg"),
    ("awilson", "amy.wilson@cpg.com", "Amy", "Wilson", "store_manager", 10, "South", "https://randomuser.me/api/portraits/women/6.jpg"),
    ("crodriguez", "carlos.rodriguez@cpg.com", "Carlos", "Rodriguez", "store_manager", 11, "South", "https://randomuser.me/api/portraits/men/5.jpg"),
    ("jlee", "jessica.lee@cpg.com", "Jessica", "Lee", "store_manager", 12, "West", "https://randomuser.me/api/portraits/women/7.jpg"),
    ("mthomas", "michael.thomas@cpg.com", "Michael", "Thomas", "store_manager", 13, "Southeast", "https://randomuser.me/api/portraits/men/6.jpg"),
    ("swalker", "sarah.walker@cpg.com", "Sarah", "Walker", "store_manager", 14, "Midwest", "https://randomuser.me/api/portraits/women/8.jpg"),
    ("jharris", "james.harris@cpg.com", "James", "Harris", "store_manager", 15, "Northwest", "https://randomuser.me/api/portraits/men/7.jpg"),
    ("emartin", "emily.martin@cpg.com", "Emily", "Martin", "store_manager", 16, "West", "https://randomuser.me/api/portraits/women/9.jpg"),
    ("dclark", "daniel.clark@cpg.com", "Daniel", "Clark", "store_manager", 17, "Northeast", "https://randomuser.me/api/portraits/men/8.jpg"),
    ("alewis", "amanda.lewis@cpg.com", "Amanda", "Lewis", "store_manager", 18, "Southeast", "https://randomuser.me/api/portraits/women/10.jpg"),
    ("rjackson", "ryan.jackson@cpg.com", "Ryan", "Jackson", "store_manager", 19, "Midwest", "https://randomuser.me/api/portraits/men/9.jpg"),
    ("kwhite", "kevin.white@cpg.com", "Kevin", "White", "store_manager", 20, "Mountain", "https://randomuser.me/api/portraits/men/10.jpg"),
    ("lhall", "laura.hall@cpg.com", "Laura", "Hall", "store_manager", 21, "Southeast", "https://randomuser.me/api/portraits/women/11.jpg"),
    ("ballen", "brian.allen@cpg.com", "Brian", "Allen", "store_manager", 22, "West", "https://randomuser.me/api/portraits/men/11.jpg"),
    ("nking", "nicole.king@cpg.com", "Nicole", "King", "store_manager", 23, "Northeast", "https://randomuser.me/api/portraits/women/12.jpg"),
    ("jwright", "jason.wright@cpg.com", "Jason", "Wright", "store_manager", 24, "Midwest", "https://randomuser.me/api/portraits/men/12.jpg"),
    ("mscott", "melissa.scott@cpg.com", "Melissa", "Scott", "store_manager", 25, "South", "https://randomuser.me/api/portraits/women/13.jpg"),
    ("gmoodley", "giran.moodley@cpg.com", "Giran", "Moodley", "regional_manager", None, "Midwest", "https://media.licdn.com/dms/image/v2/C4D03AQEf-2-7ik3cuA/profile-displayphoto-shrink_400_400/0/1586893805650"),
    ("janderson", "jennifer.anderson@cpg.com", "Jennifer", "Anderson", "regional_manager", None, "South", "https://randomuser.me/api/portraits/women/14.jpg"),
    ("slee", "steve.lee@cpg.com", "Steve", "Lee", "regional_manager", None, "West", "https://randomuser.me/api/portraits/men/14.jpg"),
    ("nwhite", "nancy.white@cpg.com", "Nancy", "White", "regional_manager", None, "Southeast", "https://randomuser.me/api/portraits/women/15.jpg"),
    ("pjones", "paul.jones@cpg.com", "Paul", "Jones", "regional_manager", None, "Northeast", "https://randomuser.me/api/portraits/men/15.jpg"),
    ("rtaylor", "rachel.taylor@cpg.com", "Rachel", "Taylor", "regional_manager", None, "Northwest", "https://randomuser.me/api/portraits/women/16.jpg"),
    ("ggreen", "gregory.green@cpg.com", "Gregory", "Green", "regional_manager", None, "Southwest", "https://randomuser.me/api/portraits/men/16.jpg"),
    ("dadams", "diane.adams@cpg.com", "Diane", "Adams", "regional_manager", None, "Mountain", "https://randomuser.me/api/portraits/women/17.jpg"),
]

cur.executemany(
    "INSERT INTO users (username, email, first_name, last_name, role, store_id, region, avatar_url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
    users_data,
)

conn.commit()
cur.close()
conn.close()
print("Static data populated: 156 products, 26 stores, 33 users")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Inventory and Orders

# COMMAND ----------

# Configuration
NUM_PRODUCTS = 156
NUM_STORES = 26
HQ_STORE_ID = 1
FULFILLED_COUNT = 500
PENDING_COUNT = 50
CANCELLED_COUNT = 20
APPROVED_COUNT = 100
AS_OF_DATE = datetime.now()
BACKFILL_START = AS_OF_DATE - timedelta(days=180)

conn = get_connection()
cur = conn.cursor()

# Generate inventory
print("Generating inventory...")
inventory_data = []
product_ids = list(range(1, NUM_PRODUCTS + 1))
retail_store_ids = list(range(2, NUM_STORES + 1))

# HQ inventory
for pid in product_ids:
    inventory_data.append((pid, HQ_STORE_ID, random.randint(500, 3000), 0))

# Retail store inventory
for sid in retail_store_ids:
    for pid in product_ids:
        inventory_data.append((pid, sid, random.randint(20, 150), 0))

execute_batch(
    cur,
    "INSERT INTO inventory (product_id, store_id, quantity_cases, reserved_cases) VALUES (%s, %s, %s, %s)",
    inventory_data,
    page_size=500,
)
conn.commit()
print(f"Inventory generated: {len(inventory_data)} records")

# Generate orders
print("Generating orders...")
regional_manager_ids = list(range(26, 34))
order_counter = 1

def generate_dates(count, status):
    dates = []
    total_days = (AS_OF_DATE - BACKFILL_START).days
    for _ in range(count):
        if status == "fulfilled":
            r = random.betavariate(5, 2)
            days_back = int((1 - r) * total_days)
        elif status == "approved":
            days_back = random.randint(0, min(14, total_days))
        elif status == "pending_review":
            days_back = random.randint(0, min(7, total_days))
        else:
            r = random.betavariate(2, 2)
            days_back = int(r * total_days)
        d = AS_OF_DATE - timedelta(days=days_back)
        d = d.replace(hour=random.randint(6, 22), minute=random.randint(0, 59))
        dates.append(d)
    return sorted(dates)

order_configs = [
    ("fulfilled", FULFILLED_COUNT),
    ("approved", APPROVED_COUNT),
    ("pending_review", PENDING_COUNT),
    ("cancelled", CANCELLED_COUNT),
]

all_orders = []
for status, count in order_configs:
    dates = generate_dates(count, status)
    for order_date in dates:
        order_num = f"ORD{order_counter:06d}"
        to_store = random.choice(retail_store_ids)
        product_id = random.choice(product_ids)
        quantity = random.randint(5, 50)
        requested_by = min(to_store, 25)
        regional_mgr = random.choice(regional_manager_ids)

        approved_by = None
        approved_date = None
        fulfilled_date = None
        notes = None

        if status == "fulfilled":
            approved_date = order_date + timedelta(hours=random.randint(2, 48))
            fulfilled_date = approved_date + timedelta(hours=random.randint(4, 72))
            approved_by = regional_mgr
        elif status == "approved":
            approved_date = order_date + timedelta(hours=random.randint(2, 24))
            approved_by = regional_mgr
        elif status == "cancelled":
            if random.random() < 0.3:
                approved_date = order_date + timedelta(hours=random.randint(2, 48))
                approved_by = regional_mgr
                notes = random.choice(["Insufficient inventory", "Store cancellation", "Product recall"])
            else:
                notes = random.choice(["Insufficient inventory", "Exceeds capacity", "Duplicate order", "Budget constraints"])

        all_orders.append((
            order_num, HQ_STORE_ID, to_store, product_id, quantity, status,
            requested_by, approved_by, order_date, approved_date, fulfilled_date, notes
        ))
        order_counter += 1

execute_batch(
    cur,
    """INSERT INTO orders (order_number, from_store_id, to_store_id, product_id, quantity_cases,
       order_status, requested_by, approved_by, order_date, approved_date, fulfilled_date, notes)
       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
    all_orders,
    page_size=500,
)
conn.commit()
print(f"Orders generated: {len(all_orders)} records")

cur.close()
conn.close()

# COMMAND ----------

# Verify data
conn = get_connection()
cur = conn.cursor()
for table in ["products", "stores", "users", "inventory", "orders"]:
    cur.execute(f"SELECT count(*) FROM {table}")
    print(f"{table}: {cur.fetchone()[0]} rows")

cur.execute("SELECT order_status, count(*) FROM orders GROUP BY order_status ORDER BY count(*) DESC")
print("\nOrders by status:")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]}")

cur.close()
conn.close()
print("\nDatabase seeding complete!")
