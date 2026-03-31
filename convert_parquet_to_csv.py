from pathlib import Path
import pandas as pd

# I define input/output directories relative to project root
BASE_DIR = Path(__file__).resolve().parent
PARQUET_DIR = BASE_DIR / "data" / "parquets"
CSV_DIR = BASE_DIR / "data" / "csvs"

def convert_all_parquets():
    # I make sure the output directory exists
    CSV_DIR.mkdir(parents=True, exist_ok=True)

    parquet_files = list(PARQUET_DIR.glob("*.parquet"))

    if not parquet_files:
        print("No parquet files found in data/parquets/")
        return

    print(f"Found {len(parquet_files)} parquet files. Converting...\n")

    for pq_file in parquet_files:
        print(f"Processing: {pq_file.name}")

        try:
            df = pd.read_parquet(pq_file)

            output_file = CSV_DIR / f"{pq_file.stem}.csv"
            df.to_csv(output_file, index=False)

            print(f"Saved → {output_file}\n")

        except Exception as e:
            print(f"Failed on {pq_file.name}: {e}\n")

    print("Conversion complete.")

if __name__ == "__main__":
    convert_all_parquets()