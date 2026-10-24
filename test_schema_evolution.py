from datetime import datetime, timezone, timedelta
import pandas as pd

from main import extract_data


# --------------------------------------------------
# 1. Extract a small recent batch
# --------------------------------------------------

boundary = datetime.now(timezone.utc) - timedelta(hours=2)

pulls_df, reviews_df = extract_data(boundary)

print("\n--- EXTRACTED DATA ---")


print(
    pulls_df[
        ["id", "updated_at", "is_draft"]
    ]
)

print("\n--- PANDAS DTYPES ---")
print(pulls_df.dtypes)


# --------------------------------------------------
# 2. Write the PR data locally as Parquet
# --------------------------------------------------

parquet_path = "schema_evolution_test.parquet"

pulls_df.to_parquet(
    parquet_path,
    index=False
)

print(f"\nParquet written to: {parquet_path}")


# --------------------------------------------------
# 3. Read the Parquet file back
# --------------------------------------------------

parquet_df = pd.read_parquet(parquet_path)

print("\n--- DATA READ BACK FROM PARQUET ---")

print(
    parquet_df[
        ["id", "updated_at", "is_draft"]
    ].head(10)
)


# --------------------------------------------------
# 4. Check datatype after Parquet round trip
# --------------------------------------------------

print("\n--- PARQUET DATAFRAME DTYPES ---")
print(parquet_df.dtypes)

print("\nis_draft dtype:")
print(parquet_df["is_draft"].dtype)