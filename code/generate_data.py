from pathlib import Path
import polars as pl
import numpy as np
from argparse import ArgumentParser

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


def get_random_name(length: int = 8) -> str:
    np.random.seed()
    letters = np.array(list("abcdefghijklmnopqrstuvwxyz"))
    return "".join(np.random.choice(letters, size=length))
    

def main(n_samples: int, n_files: int, verbose: bool = False):
    output_dir = Path(__file__).parent.parent / "data"
    output_dir.mkdir(parents=True, exist_ok=True)

    for _ in range(n_files):
        df = generate_data(n_samples)
        file_name = get_random_name() + ".csv"
        file_path = output_dir / file_name
        if verbose:
            print(f"Writing {file_path} with {n_samples} samples.")
        df.write_csv(file_path)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--n_samples", type=int, default=100_000, help="Number of samples per file")
    parser.add_argument("--n_files", type=int, default=1000, help="Number of files to generate")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    args = parser.parse_args()
    main(args.n_samples, args.n_files, args.verbose)