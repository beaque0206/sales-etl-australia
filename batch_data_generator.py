import pandas as pd
import random
from datetime import datetime, timedelta

# Load country, region, and city data from the CSV file
cities_df = pd.read_csv("country_region_city.csv")

masterlist_df = pd.read_csv("product_design.csv")
vehicle_df = pd.read_csv("vehicle_master.csv")

# Summarize material costs by product-location to confirm totals for validation
product_cost_summary = masterlist_df.groupby(['Country', 'Region', 'City', 'Product Name']).agg(
    total_material_cost=('Material Unit Cost', 'sum'),
    total_material_shipping_cost=('Material Shipping Cost', 'sum')
).reset_index()

# Add these summarized costs back to the masterlist_df
masterlist_df = pd.merge(masterlist_df, product_cost_summary, on=['Country', 'Region', 'City', 'Product Name'], how='left')

# Generate 1 million sales transactions using the master list as reference
sales_data = []
start_date = datetime(2024, 7, 1)
end_date = datetime(2025, 4, 30)
date_range = pd.date_range(start_date, end_date).tolist()
discount_codes = ['DISCOUNT10', 'DISCOUNT15', 'DISCOUNT20']
vehicle_plates = [f"Plate_{i+1}" for i in range(1, 4)]

cities_df = cities_df[cities_df["Country"]=='Australia']


print('creating transactions')
# Create 1 million random transactions
for _ in range(1000):  # Generate 1 million transactions
    random_location = cities_df.sample(1).iloc[0]
    sales_order_number = f"SO_{random.randint(100000, 999999)}"
    order_date = random.choice(date_range)
    sales_type = random.choice(['delivery', 'instore'])
    delivery_fee = 0 if sales_type == 'instore' else round(random.uniform(0, 5), 2)
    delivery_duration = random.randint(5, 45) if sales_type == 'delivery' else None  # Set delivery duration for delivery orders
    # Get matching vehicles for the location
    matching_vehicles = vehicle_df[
        (vehicle_df['Region'] == random_location['Region']) &
        (vehicle_df['City'] == random_location['City'])
    ]['Vehicle Plate'].tolist()

    delivery_vehicle = random.choice(matching_vehicles) if sales_type == 'delivery' else None
    
    # Determine the number of products for this order (1 to 8)
    num_products_in_order = random.randint(1, 8)
    
    # Generate each product in the order
    for _ in range(num_products_in_order):
        random_product = masterlist_df[
            (masterlist_df['Country'] == random_location['Country']) &
            (masterlist_df['Region'] == random_location['Region']) &
            (masterlist_df['City'] == random_location['City'])
        ].sample(1).iloc[0]

        quantity = random.randint(1, 10)
        total_product_cost = round(random_product['Product Price'] * quantity, 2)
        material_cost = random_product['total_material_cost']
        material_shipping_cost = random_product['total_material_shipping_cost']

        # Append each product in the order to the sales data
        sales_data.append({
            "Country": random_location['Country'],
            "Region": random_location['Region'],
            "City": random_location['City'],
            "Store ID": random_location['store_id'],
            "Store Name": f"{random_location['City']} Store",
            "Sales Order Number": sales_order_number,
            "Sales Order Date": order_date,
            "Product Name": random_product['Product Name'],
            "Quantity": quantity,
            "Unit Price": random_product['Product Price'],
            "Total Product Cost": total_product_cost,
            "Material Cost": material_cost,
            "Shipping Cost": material_shipping_cost,
            "Total Cost": material_cost + material_shipping_cost,
            "Sales Type": sales_type,
            "Delivery Fee": delivery_fee,
            "Delivery Duration (mins)": delivery_duration,  # New field for delivery duration
            "Discount Code": random.choice(discount_codes) if random.random() < 0.5 else None,
            "Discount %": int(random.choice([10, 15, 20])) if random.random() < 0.5 else 0,
            "Delivery vehicle plate": delivery_vehicle
        })

# Convert to DataFrame
sales_df = pd.DataFrame(sales_data)

# Save the final datasets to CSV


sales_df.to_csv("sales_batch.csv", index=False)

