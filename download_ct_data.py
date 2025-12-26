import os
import zipfile
import requests
from datetime import datetime
import re
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
import os

BASE_URL = "https://downloads.climatetrace.org/v5.1.0/sector_packages"

SECTORS = [
    "manufacturing",
    "mineral_extraction",
    "power",
]

EMISSION_TYPES = {
    "co2e_100yr": "co2e_100yr",
    "co2e_20yr": "co2e_20yr",
    "co2": "co2",
    "ch4": "ch4",
    "n2o": "n2o",
    "pm2.5": "pm2_5",
    "vocs": "vocs",
    "co": "co",
    "nh3": "nh3",
    "nox": "nox",
    "so2": "so2",
    "bc": "bc",
    "oc": "oc",
}

DOWNLOAD_DIR = "downloads"
EXTRACT_DIR = "extracted"

FILE_RE = re.compile(r"(.+)_emissions_sources_v5_1_0\.csv$")

# Path to your local Parquet data
PARQUET_BASE = "parquet/emissions"

# Output master file
MASTER_PARQUET = "facility_master.parquet"


os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(EXTRACT_DIR, exist_ok=True)

def log(message):
    ts = datetime.utcnow().isoformat(timespec="seconds")
    print(f"[{ts}] {message}")

def download_zip(url, zip_path):
    r = requests.get(url, stream=True, timeout=60)
    if r.status_code == 200:
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        return "downloaded"
    elif r.status_code == 404:
        return "missing"
    else:
        return f"http_{r.status_code}"

def extract_zip(zip_path, extract_to):
    os.makedirs(extract_to, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(extract_to)

# Download for all sectors and emission types
def download_sectors_emission_types(sectors, emission_types):
    for sector in sectors:
        for emission_name, emission_slug in emission_types.items():

            url = f"{BASE_URL}/{emission_slug}/{sector}.zip"
            zip_filename = f"{sector}__{emission_name}.zip"
            zip_path = os.path.join(DOWNLOAD_DIR, zip_filename)
            extract_path = os.path.join(EXTRACT_DIR, sector, emission_name)

            try:
                status = download_zip(url, zip_path)

                if status == "downloaded":
                    extract_zip(zip_path, extract_path)
                    log(f"OK     | {sector:<18} | {emission_name:<10} | extracted → {extract_path}")
                elif status == "missing":
                    log(f"MISSING| {sector:<18} | {emission_name:<10} | not available")
                else:
                    log(f"ERROR  | {sector:<18} | {emission_name:<10} | {status}")

            except Exception as e:
                log(f"EXCEPT | {sector:<18} | {emission_name:<10} | {e}")

def print_tree(root_path, prefix=""):
    try:
        items = sorted(os.listdir(root_path))
    except PermissionError:
        print(prefix + "└── [Permission Denied]")
        return

    for index, item in enumerate(items):
        path = os.path.join(root_path, item)
        is_last = index == len(items) - 1

        connector = "└── " if is_last else "├── "
        print(prefix + connector + item)

        if os.path.isdir(path):
            extension = "    " if is_last else "│   "
            print_tree(path, prefix + extension)


######################################################################
## Write to parquet files
######################################################################

def normalize_columns(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^\w_]", "", regex=True)
    )
    return df

def process_csv(path, sector, subsector, emission_type, version):
    df = pd.read_csv(path, low_memory=False)

    df = normalize_columns(df)

    # Add explicit metadata columns
    df["sector"] = sector
    df["subsector"] = subsector
    df["emission_type"] = emission_type
    df["source_version"] = version

    return df

def write_parquet(df, sector, subsector, emission_type, out_base):
    out_dir = os.path.join(
        out_base,
        f"sector={sector}",
        f"emission={emission_type}",
        f"subsector={subsector}"
    )
    os.makedirs(out_dir, exist_ok=True)

    table = pa.Table.from_pandas(df, preserve_index=False)

    pq.write_table(
        table,
        os.path.join(out_dir, "part-000.parquet"),
        compression="zstd",
        use_dictionary=True
    )

    print(f"✔ {subsector}: {len(df):,} rows, {df.shape[1]} columns")

for sector in SECTORS:
    for emission_type in EMISSION_TYPES.keys():
        print(f"{sector} - {emission_type}")
        RAW_BASE = f"extracted/{sector}/{emission_type}/DATA"
        OUT_BASE = "parquet/emissions"

        SECTOR = f"{sector}"
        EMISSION_TYPE = emission_type
        VERSION = "v5.1.0"

        for file in sorted(os.listdir(RAW_BASE)):
            match = FILE_RE.match(file)
            if not match:
                print(f"{file} not matched.")
                continue

            subsector = match.group(1)
            csv_path = os.path.join(RAW_BASE, file)

            print(f"Processing {file}")
            df = process_csv(csv_path, subsector)
            write_parquet(df, subsector)


# Initialize DuckDB in-memory connection
con = duckdb.connect()

# Step 1: Find all Parquet files recursively
parquet_files = []
for root, dirs, files in os.walk(PARQUET_BASE):
    for file in files:
        if file.endswith(".parquet"):
            parquet_files.append(os.path.join(root, file))

print(f"Found {len(parquet_files)} Parquet files.")

# Build SQL UNION query over all files
# This keeps all columns, adds partition metadata if missing
queries = []
for f in parquet_files:
    # Extract sector and emission_type from folder structure
    parts = f.split(os.sep)
    # Adjust based on your folder layout
    try:
        sector = [p for p in parts if p.startswith("sector=")][0].split("=")[1]
        emission_type = [p for p in parts if p.startswith("emission=")][0].split("=")[1]
    except IndexError:
        sector = None
        emission_type = None

    q = f"""
    SELECT *, '{sector}' AS sector, '{emission_type}' AS emission_type
    FROM '{f}'
    """
    queries.append(q)

full_query = " UNION ALL ".join(queries)

# Aggregate if needed (optional: here we just combine all rows)
print("Combining all files into master Parquet...")
con.execute(f"""
CREATE TABLE facility_master AS
{full_query}
""")

# Export to Parquet
print(f"Writing master Parquet to {MASTER_PARQUET} ...")
con.execute(f"COPY facility_master TO '{MASTER_PARQUET}' (FORMAT PARQUET);")

print("Done! Master Parquet ready for local analysis or S3 upload.")
