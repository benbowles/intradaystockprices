csvs = """/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/PM-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/PNC-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/PPL-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/PRU-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/PSX-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/PVH-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/PXD-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/PYPL-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/QCOM-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/RCL-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/RF-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/ROCC-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/ROL-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/ROST-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/RTX-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/SBUX-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/SCHD-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/SCHW-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/SEE-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/SKLZ-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/SLB-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/SO-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/SOXS-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/SPGI-2012-05-16-2023-03-23.csv,/Users/bowles/stocks/.temp/vendor/2023-03-23 22:00:45.652105/SPXL-2012-05-16-2023-03-23.csv""".split(',')
import pandas as pd
dfs = [pd.read_csv(csv) for csv in csvs]

import pandas as pd
import multiprocessing as mp

def validate_datetime(data_tuple):
    n, df = data_tuple
    errors = []

    for i, row in df.iterrows():
        try:
            pd.to_datetime(row['Datetime'])
        except:
            errors.append((n, i, row))
            
    return errors

if __name__ == "__main__":

    with mp.Pool(mp.cpu_count()) as pool:
        results = pool.map(validate_datetime, enumerate(dfs))

    errors = []
    for r in results:
        errors.extend(r)

    if errors:
        for error in errors:
            print(f"DataFrame {error[0]}, Row {error[1]}: {error[2]}")
    else:
        print("No errors found.")