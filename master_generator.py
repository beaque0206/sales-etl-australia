import pandas as pd
import random

# Load country, region, and city data from the CSV file
cities_df = pd.read_csv("country_region_city.csv")

# Generate a list of products and materials with consistent pricing across regions
num_products = 50
num_materials = 50
products = [f"Product_{i+1}" for i in range(num_products)]
materials = [f"MAT_{i:03}" for i in range(1, num_materials + 1)]

# Initialize master list data for products and materials
masterlist_data = []
vehicle_plate_master_list = []

# Create product-material combinations with specified constraints
for _, location_row in cities_df.iterrows():
    country = location_row['Country']
    region = location_row['Region']
    city = location_row['City']

    # Generate 3 random vehicle plates per city
    plates = [f"{region[:2].upper()}{city[:2].upper()}_{i+1}" for i in range(3)]
    for plate in plates:
        vehicle_plate_master_list.append({
            "Country": country,
            "Region": region,
            "City": city,
            "Vehicle Plate": plate
        })
        
    for product in products:
        # Set a single base price for the product, consistent within the store
        product_price = round(random.uniform(20, 100), 2)

        # Set 5 to 8 random materials per product
        num_materials_per_product = random.randint(5, 8)
        selected_materials = random.sample(materials, num_materials_per_product)
        
        # Distribute material cost and shipping constraints for each material
        total_material_cost = round(product_price * random.uniform(0.3, 0.95), 2)
        total_material_shipping_cost = round(total_material_cost * random.uniform(0.05, 0.1), 2)
        
        # Ensure consistent costs across locations with minor variance for region
        for material_code in selected_materials:
            base_material_unit_cost = round(total_material_cost / num_materials_per_product, 2)
            region_adjusted_material_cost = round(base_material_unit_cost * random.uniform(0.97, 1.03), 2)
            material_quantity = random.randint(1, 20)
            material_shipping_cost = round(min(total_material_shipping_cost / num_materials_per_product, region_adjusted_material_cost * 0.1), 2)

            # Append each material to the master list
            masterlist_data.append({
                "Country": country,
                "Region": region,
                "City": city,
                "Product Name": product,
                "Product Price": product_price,
                "Material Code": material_code,
                "Material Quantity": material_quantity,
                "Material Unit Cost": region_adjusted_material_cost,
                "Material Shipping Cost": material_shipping_cost
            })

# Convert master list to DataFrame
masterlist_df = pd.DataFrame(masterlist_data)
vehicle_plate_df = pd.DataFrame(vehicle_plate_master_list)

masterlist_df.to_csv("product_design.csv", index=False)
vehicle_plate_df.to_csv("vehicle_master.csv", index=False)
