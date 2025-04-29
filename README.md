# sales-etl-australia

This project simulates an end-to-end ETL pipeline for sales data generation across Australian cities and regions, using Python and Azure-ready formats. It is designed to demonstrate data engineering, cloud storage, modeling, and Power BI dashboarding.

---

## 📁 Project Structure

| File                     | Description |
|--------------------------|-------------|
| `master_generator.py` | Generates product-material masterlist and delivery vehicle masterlist by city/region |
| `batch_data_generator.py` | Reads the masterlists and generates batch sales transactions |

---

## 📦 Outputs

- `unique_products_with_materials_masterlist.csv`
- `vehicle_plate_masterlist.csv`
- `sales_batch.csv`

---

## ✅ Next Steps

- Automate daily sales generation from May 2025
- Upload outputs to Azure Blob Storage
- Clean and model data in Azure SQL / Databricks
- Visualize KPIs in Power BI

---

## 💡 Goals

This repo demonstrates:
- ETL design and modular script structure
- Cloud-ready output formats
- Git version control and CI/CD principles

