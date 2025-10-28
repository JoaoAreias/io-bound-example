import pandas as pd

from pathlib import Path
from queue import Queue
from typing import Iterable
from concurrent.futures import ThreadPoolExecutor

def read_file(files: Iterable[Path], processing_queue: Queue):
    # Read all CSV files and put them into the processing queue
    for file_path in files:
        data = pd.read_csv(file_path)
        processing_queue.put(data)
    # Signal that reading is done
    processing_queue.put(None)

def process_file(processing_queue: Queue, preprocessed_queue: Queue):
    # Process data from the processing queue and put results into the aggregate queue
    while True:
        data = processing_queue.get(block=True)

        # None indicates no more data to process
        if data is None:
            # Signal that processing is done
            preprocessed_queue.put(None)
            break

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

    
def aggregate_results(preprocessed_queue: Queue) -> pd.DataFrame:
    concatenated = []
    while True:
        data = preprocessed_queue.get(block=True)

        # None indicates no more data to aggregate
        if data is None:
            break

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
    input_dir = Path("data")
    files = input_dir.glob("*.csv")
    
    output_file = "averages.csv"

    # Queues for inter-thread communication
    processing_queue = Queue()
    preprocessed_queue = Queue()


    with ThreadPoolExecutor(max_workers=3) as executor:
        # Start the file reading thread
        executor.submit(read_file, files, processing_queue)

        # Start the file processing thread
        executor.submit(process_file, processing_queue, preprocessed_queue)

        # Start the aggregation thread
        future = executor.submit(aggregate_results, preprocessed_queue)

        # Aggregate results in the main thread
        final_result = future.result()

    # Save the final result to a CSV file
    final_result.to_csv(output_file, index=False)


if __name__ == "__main__":
    main()