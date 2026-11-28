import csv
from pathlib import Path
from datetime import datetime, date

from openpyxl import load_workbook
from S3_config import create_spark_session


# Input files
WORKSPACE = Path("/workspace")

EXCEL_PATH = WORKSPACE / "Data/processed/Online Retail.xlsx"
INVENTORY_REFERENCE = WORKSPACE / "Data/processed/Online Retail Inventory.csv"

# Generated CSV
OUTPUT_DIR = WORKSPACE / "Data/processed/ingested"
OUTPUT_CSV = OUTPUT_DIR / "Online Retail Inventory.csv"

# SILO output
S3_OUTPUT_PATH = ("s3a://stocksense-data/processed/ingested/Online Retail Inventory")

BASE_COLUMNS = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country"
]

INVENTORY_COLUMNS = [
    "CurrentStock",
    "ReorderLevel",
    "LeadTimeDays",
    "Supplier"
]

OUTPUT_COLUMNS = BASE_COLUMNS + INVENTORY_COLUMNS


def normalize_stock_code(value):
    if value is None:
        return ""

    if isinstance(value, float) and value.is_integer():
        return str(int(value))

    return str(value).strip()


def format_value(value):
    if value is None:
        return ""

    if isinstance(value, datetime):
        return value.strftime("%d/%m/%Y %H:%M")

    if isinstance(value, date):
        return value.strftime("%d/%m/%Y")

    if isinstance(value, float) and value.is_integer():
        return str(int(value))

    return str(value).strip()


def load_inventory_mapping():
    inventory = {}

    with open(INVENTORY_REFERENCE, encoding="utf-8-sig", newline="") as file:

        reader = csv.DictReader(file, delimiter=";")

        for row in reader:
            stock_code = normalize_stock_code(row["StockCode"])

            values = tuple(row[column].strip() for column in INVENTORY_COLUMNS)

            if not any(values):
                continue

            if stock_code in inventory and inventory[stock_code] != values:
                raise ValueError(f"Conflicting inventory data for StockCode: {stock_code}")

            inventory[stock_code] = values

    return inventory


def convert_excel_to_csv(inventory_mapping):
    workbook = load_workbook(EXCEL_PATH,read_only=True,data_only=True)

    worksheet = workbook.active
    rows = worksheet.iter_rows(values_only=True)

    headers = [str(value).strip() if value is not None else "" 
               for value in next(rows)]

    missing_columns = [column for column in BASE_COLUMNS if column not in headers]

    if missing_columns:
        workbook.close()
        raise ValueError(f"Missing Excel columns: {missing_columns}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    matched_products = 0

    try:
        with open(OUTPUT_CSV, "w", encoding="utf-8", newline="",) as file:

            writer = csv.writer(file, delimiter=";")
            writer.writerow(OUTPUT_COLUMNS)

            for excel_row in rows:
                if not any(value is not None for value in excel_row):
                    continue

                row_data = dict(zip(headers, excel_row))

                stock_code = normalize_stock_code(row_data.get("StockCode"))

                inventory_values = inventory_mapping.get(stock_code,("", "", "", ""),)

                if any(inventory_values):
                    matched_products += 1

                base_values = [format_value(row_data.get(column)) for column in BASE_COLUMNS]
                writer.writerow(base_values + list(inventory_values))
                total_rows += 1

    finally:
        workbook.close()

    print("\n========== INGESTION PREPARATION ==========")
    print("Excel rows processed:", total_rows)
    print("Inventory mapping entries:", len(inventory_mapping))
    print("Rows matched with inventory:", matched_products)
    print("Generated CSV:", OUTPUT_CSV)

    return total_rows


def upload_to_silo():
    print("\nUploading enriched CSV to SILO...")

    spark = create_spark_session("StockSense_Prepare_Ingestion")

    try:
        df = (
            spark.read
            .option("header", "true")
            .option("sep", ";")
            .csv(str(OUTPUT_CSV)))

        df.write \
            .mode("overwrite") \
            .option("header", "true") \
            .option("sep", ";") \
            .csv(S3_OUTPUT_PATH)

        print("\n========== SILO UPLOAD ==========")
        print("Saved to:", S3_OUTPUT_PATH)
        print("Uploaded rows:", df.count())

    finally:
        spark.stop()

def main():
    if not EXCEL_PATH.is_file():
        raise FileNotFoundError(f"Excel not found: {EXCEL_PATH}")

    if not INVENTORY_REFERENCE.is_file():
        raise FileNotFoundError(f"Inventory reference not found: {INVENTORY_REFERENCE}")

    inventory_mapping = load_inventory_mapping()
    convert_excel_to_csv(inventory_mapping)
    upload_to_silo()
    print("\nData preparation completed successfully.")


if __name__ == "__main__":
    main()