
import pandas as pd

from pathlib import Path
from typing import Iterable

def read_file(file: Path) -> pd.DataFrame:
    return pd.read_csv(file)

def process_file(data: pd.DataFrame) -> pd.DataFrame:
    result = (
        data.groupby("city_code")
        # Compute sum and count for average calculation
        .agg(
            temperature_sum=("temperature", "sum"),
            temperature_count=("temperature", "count"),
        )
        .reset_index()
    )
    return result
    
def aggregate_results(results: Iterable[pd.DataFrame]) -> pd.DataFrame:
    concatenated_df = pd.concat(results)
    aggregated = (
        concatenated_df.groupby("city_code")
        .agg(
            temperature_sum=("temperature_sum", "sum"),
            temperature_count=("temperature_count", "sum"),
        )
        .reset_index()
    )
    aggregated["average_temperature"] = (
        aggregated["temperature_sum"] / aggregated["temperature_count"]
    )
    aggregated = aggregated.reset_index()[["city_code", "average_temperature"]]
    return aggregated


def main():
    input_dir = Path("data")
    files = input_dir.glob("*.csv")
    output_file = "averages.csv"

    results = []
    for file in files:
        data = read_file(file)
        processed_data = process_file(data)
        results.append(processed_data)

    final_result = aggregate_results(results)

    # Save the final result to a CSV file
    final_result.to_csv(output_file, index=False)


if __name__ == "__main__":
    main()