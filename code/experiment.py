import os
import subprocess
from pathlib import Path
from tqdm import trange

import numpy as np
import pandas as pd

ROOT_PATH = Path(__file__).parent.parent
DATA_PATH = ROOT_PATH / "data"
SCRIPTS = {
    "benchmark": ROOT_PATH / "code" / "get_averages_benchmark.py",
    "multithreading": ROOT_PATH / "code" / "get_averages_multithreading.py",
}


def parse_time_output(stderr: str) -> float:
    real_time_line = next(
        line for line in stderr.splitlines() if "real" in line
    )
    real_time_str = real_time_line.split()[1]

    def parse_time(real_time_str: str) -> float:
        minutes, seconds = real_time_str.split("m")
        total_time = float(minutes) * 60 + float(seconds.strip("s"))
        return total_time

    execution_time = parse_time(real_time_str)
    return execution_time

def call_script(script_path: Path) -> subprocess.CompletedProcess:
    result = subprocess.run(
        f"time python {str(script_path)}",
        capture_output=True,
        text=True,
        shell=True,
        executable="/bin/bash",
    )
    return result

def run_experiment() -> tuple[str, float]:
    # Randomly choose one of the scripts to run
    np.random.seed()
    method = np.random.choice(list(SCRIPTS.keys()))

    # Run the selected script and measure its execution time
    result = call_script(SCRIPTS[method])

    # Parse the time output from stderr
    execution_time = parse_time_output(result.stderr)
    return method, execution_time
    

def main():
    n_experiments = 200  # Number of experiments to run

    results = {
        'method': [],
        'execution_time': [],
    }

    # Dry run to make sure files are cached in both methods
    call_script(SCRIPTS["benchmark"]) 

    for _ in trange(n_experiments):
        method, exec_time = run_experiment()
        results['method'].append(method)
        results['execution_time'].append(exec_time)

    
    results_df = pd.DataFrame(results)
    results_df.to_csv(ROOT_PATH / "experiment_results.csv", index=False)

    print("Experiment results:")
    print(results_df.groupby('method').execution_time.describe())


if __name__ == "__main__":
    main()