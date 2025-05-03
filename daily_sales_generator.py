import pandas as pd
import random
from datetime import datetime
import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# --- CONFIG ---
DAILY_DATE = datetime.now().strftime("%Y-%m-%d")  # Override if needed
NUM_ORDERS = 100  # Adjust volume per day

# --- LOAD MASTERLISTS ---
products_df = pd.read_csv("product_design.csv")
vehicles_df = pd.read_csv("vehicle_master.csv")
cities_df = pd.read_csv("country_region_city.csv")
cities_df = cities_df[cities_df["Country"] == "Australia"]

# --- CONSTANTS ---
discount_codes = ['DISCOUNT10', 'DISCOUNT15', 'DISCOUNT20']
sales_data = []

# --- AGGREGATED COSTS ---
# Summarize material costs by product-location to confirm totals for validation
product_cost_summary = products_df.groupby(['Country', 'Region', 'City', 'Product Name']).agg(
    total_material_cost=('Material Unit Cost', 'sum'),
    total_material_shipping_cost=('Material Shipping Cost', 'sum')
).reset_index()

# Add these summarized costs back to the masterlist_df
products_df = pd.merge(products_df, product_cost_summary, on=['Country', 'Region', 'City', 'Product Name'], how='left')


# --- GENERATE DAILY TRANSACTIONS ---
for _ in range(NUM_ORDERS):
    location = cities_df.sample(1).iloc[0]
    sales_order_number = f"SO_{random.randint(100000, 999999)}"
    sales_type = random.choice(['delivery', 'instore'])
    delivery_fee = 0 if sales_type == 'instore' else round(random.uniform(0, 5), 2)
    delivery_duration = random.randint(5, 45) if sales_type == 'delivery' else None

    vehicles = vehicles_df[
        (vehicles_df['Region'] == location['Region']) &
        (vehicles_df['City'] == location['City'])
    ]['Vehicle Plate'].tolist()
    delivery_vehicle = random.choice(vehicles) if sales_type == 'delivery' else None

    num_products = random.randint(1, 5)
    for _ in range(num_products):
        product = products_df[
            (products_df['Region'] == location['Region']) &
            (products_df['City'] == location['City'])
        ].sample(1).iloc[0]

        quantity = random.randint(1, 10)
        total_product_cost = round(product['Product Price'] * quantity, 2)

        sales_data.append({
            "Country": location['Country'],
            "Region": location['Region'],
            "City": location['City'],
            "Store ID": location['store_id'],
            "Store Name": f"{location['City']} Store",
            "Sales Order Number": sales_order_number,
            "Sales Order Date": DAILY_DATE,
            "Product Name": product['Product Name'],
            "Quantity": quantity,
            "Unit Price": product['Product Price'],
            "Total Product Cost": total_product_cost,
            "Material Cost": product['Material Unit Cost'],
            "Shipping Cost": product['Material Shipping Cost'],
            "Total Cost": product['total_material_cost'] + product['total_material_shipping_cost'],
            "Sales Type": sales_type,
            "Delivery Fee": delivery_fee,
            "Delivery Duration (mins)": delivery_duration,
            "Discount Code": random.choice(discount_codes) if random.random() < 0.5 else None,
            "Discount %": int(random.choice([10, 15, 20])) if random.random() < 0.5 else 0,
            "Delivery vehicle plate": delivery_vehicle
        })

# --- SAVE OUTPUT ---
df = pd.DataFrame(sales_data)
df.to_csv(f"sales_daily_{datetime.now().strftime('%Y%m%d')}.csv", index=False)
print(f"Saved {len(df)} records for {DAILY_DATE}")

import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load connection string
load_dotenv()
connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# Define Blob upload details
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_name = "daily-sales-data"
file_name = f"sales_daily_{datetime.now().strftime('%Y%m%d')}.csv"

# Upload the file
blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
with open(file_name, "rb") as data:
    blob_client.upload_blob(data, overwrite=True)

print(f"✅ Uploaded {file_name} to Azure Blob container: {container_name}")

