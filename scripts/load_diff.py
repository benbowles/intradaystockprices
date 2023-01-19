
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent.resolve()))

import scode

folder_key = str(scode.dt.datetime.now())
folder = '.temp/vendor/' + folder_key + '/'
scode.os.makedirs(folder, exist_ok=True)

print("Starting creating csvs...")
scode.save_all_ticker_csvs_parallel(temp_folder=folder, load_diff_only=True)

print("Done csvs, starting inputting into DB...")
scode.folder_csv_to_delta(folder)