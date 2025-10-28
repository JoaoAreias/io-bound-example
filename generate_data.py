from pathlib import Path
import polars as pl
import numpy as np

def generate_data(n_samples: int, seed: int = 42) -> pl.DataFrame:
    np.random.seed(seed)
    city_codes = [
        "LAX",  # Los Angeles
        "JFK",  # New York City (Kennedy)
        "ORD",  # Chicago (O'Hare)
        "LHR",  # London (Heathrow)
        "CDG",  # Paris (Charles de Gaulle)
        "HND",  # Tokyo (Haneda)
        "DXB",  # Dubai
        "ATL",  # Atlanta
        "PEK",  # Beijing
        "SYD"   # Sydney
    ]

    data = {
        "city_code": np.random.choice(city_codes, size=n_samples),
        "temperature": np.random.uniform(-10, 35, size=n_samples).round(2)
    }
    return pl.DataFrame(data)
    

def main():
    n_samples = 100_000
    n_files = 1000

    output_dir = Path("data")
    output_dir.mkdir(parents=True, exist_ok=True)

    for i in range(n_files):
        df = generate_data(n_samples)
        file_path = output_dir / f"data_part_{i+1:03}.csv"
        df.write_csv(file_path)
        print(f"Saved {file_path}")


if __name__ == "__main__":
    main()

