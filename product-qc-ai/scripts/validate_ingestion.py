import os
from google.cloud import bigquery

PROJECT_ID = "my_project_id"  # <-- Replace with your GCP project ID
DATASET = "product_qc"
TABLE = f"{PROJECT_ID}.{DATASET}.products"

QUERIES = [
    ("Row count", f"SELECT COUNT(*) AS row_count FROM `{TABLE}`"),
    ("Sample join check (images linked)", f"""
        SELECT product_id, ARRAY_LENGTH(image_refs) AS n_images
        FROM `{TABLE}`
        WHERE ARRAY_LENGTH(image_refs) > 0
        ORDER BY n_images DESC
        LIMIT 10;
    """),
    ("Missing descriptions", f"SELECT COUNT(*) AS missing_desc FROM `{TABLE}` WHERE description IS NULL OR description = ''"),
    ("Missing specs", f"SELECT COUNT(*) AS missing_specs FROM `{TABLE}` WHERE specs IS NULL"),
    ("Spot-check random products", f"SELECT * FROM `{TABLE}` ORDER BY RAND() LIMIT 5"),
]

PASS_CRITERIA = {
    "min_row_count": 1,
    "min_image_coverage": 0.7,  # 70% of products should have images
    "min_desc_coverage": 0.7,   # 70% of products should have descriptions
    "min_specs_coverage": 0.7,  # 70% of products should have specs
}

def main():
    client = bigquery.Client(project=PROJECT_ID)
    print(f"\nValidating table: {TABLE}\n")
    # 6.1 Row count
    row_count = client.query(QUERIES[0][1]).result().to_dataframe()["row_count"][0]
    print(f"Row count: {row_count}")
    # 6.2 Sample join check (images linked)
    print("\nSample join check (images linked):")
    print(client.query(QUERIES[1][1]).result().to_dataframe())
    # 6.3 Field sanity
    missing_desc = client.query(QUERIES[2][1]).result().to_dataframe()["missing_desc"][0]
    missing_specs = client.query(QUERIES[3][1]).result().to_dataframe()["missing_specs"][0]
    print(f"\nMissing descriptions: {missing_desc}")
    print(f"Missing specs: {missing_specs}")
    # 6.4 Spot-check random products
    print("\nSpot-check random products:")
    print(client.query(QUERIES[4][1]).result().to_dataframe())
    # Pass/fail criteria
    desc_coverage = 1 - (missing_desc / row_count) if row_count else 0
    specs_coverage = 1 - (missing_specs / row_count) if row_count else 0
    # For image coverage, count products with images
    image_coverage_query = f"SELECT COUNT(*) AS n_with_images FROM `{TABLE}` WHERE ARRAY_LENGTH(image_refs) > 0"
    n_with_images = client.query(image_coverage_query).result().to_dataframe()["n_with_images"][0]
    image_coverage = n_with_images / row_count if row_count else 0
    print(f"\nImage coverage: {image_coverage:.2%}")
    print(f"Description coverage: {desc_coverage:.2%}")
    print(f"Specs coverage: {specs_coverage:.2%}")
    if (row_count >= PASS_CRITERIA["min_row_count"] and
        image_coverage >= PASS_CRITERIA["min_image_coverage"] and
        desc_coverage >= PASS_CRITERIA["min_desc_coverage"] and
        specs_coverage >= PASS_CRITERIA["min_specs_coverage"]):
        print("\nPASS: Ingestion validation criteria met.")
    else:
        print("\nFAIL: Ingestion validation criteria NOT met. Please check your data.")

if __name__ == "__main__":
    main()
