from zipfile import ZipFile
import pandas as pd
import json


def main(zip_filename: str) -> None:

    log_lines = []
    malformed_lines = 0
    with ZipFile(zip_filename) as archive:
        df = pd.DataFrame()
        for filename in archive.namelist():
            if not filename.startswith("log"):
                print("Not interested in {filename}")
                continue
            with archive.open(filename, "r") as f:
                for line in f.readlines():
                    try:
                        log_lines.append(json.loads(line))
                    except json.JSONDecodeError:
                        malformed_lines += 1
        
        df = pd.concat([df, pd.DataFrame(log_lines)])

        summary = df.groupby('api_method')['latency_ms'].mean().sort_values(ascending=False)

        print(summary)

if __name__ == "__main__":
    main("data/logs.zip")
