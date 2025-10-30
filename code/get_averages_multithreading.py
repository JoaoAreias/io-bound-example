import pandas as pd

from pathlib import Path
from queue import Queue
from typing import Iterable
from concurrent.futures import ThreadPoolExecutor

def read_file(file: Path, processing_queue: Queue):
    # Read all CSV files and put them into the processing queue 
    data = pd.read_csv(file)
    processing_queue.put(data)

def process_file(processing_queue: Queue, preprocessed_queue: Queue, n_files: int):
    # Process data from the processing queue and put results into the aggregate queue
    for _ in range(n_files):
        data = processing_queue.get(block=True)
        result = (
            data.groupby("city_code")
            # Compute sum and count for average calculation
            .agg(
                temperature_sum=("temperature", "sum"),
                temperature_count=("temperature", "count"),
            )
            .reset_index()
        )
        preprocessed_queue.put(result)

    
def aggregate_results(preprocessed_queue: Queue, n_files: int) -> pd.DataFrame:
    concatenated = []
    for _ in range(n_files):
        data = preprocessed_queue.get(block=True)
        concatenated.append(data)

    concatenated_df = pd.concat(concatenated)
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
    input_dir = Path(__file__).parent.parent / "data"
    files = list(input_dir.glob("*.csv"))
    
    output_file = "averages.csv"

    # Queues for inter-thread communication
    processing_queue = Queue()
    preprocessed_queue = Queue()


    # Start threads for reading files
    with ThreadPoolExecutor() as executor:
        for file in files:
            executor.submit(read_file, file, processing_queue)

        # Start threads for processing files
        executor.submit(process_file, processing_queue, preprocessed_queue, len(files))

        # Start a thread for aggregating results
        final_result = executor.submit(aggregate_results, preprocessed_queue, len(files)).result()

    # Save the final result to a CSV file
    final_result.to_csv(output_file, index=False)


if __name__ == "__main__":
    main()