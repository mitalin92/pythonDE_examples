from zipfile import ZipFile
import pandas as pd
import json
import sys


def ingest_logs(zip_file: str) -> None:
    rows = []
    malformed_lines = 0

    with ZipFile(zip_file) as archive:
        for filename in archive.namelist():
            with archive.open(filename, "r") as f:
                for line in f:  # line-by-line
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        malformed_lines += 1

    df = pd.DataFrame(rows)

    if malformed_lines > 0:
        print(f"Malformed lines skipped: {malformed_lines}", file=sys.stderr)

    summary = df.groupby("api_method")["latency_ms"].mean().sort_values(ascending=False)
    print(summary.to_string())


if __name__ == "__main__":
    ingest_logs("data/logs.zip")
