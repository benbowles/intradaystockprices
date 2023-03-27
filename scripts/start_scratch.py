
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent.resolve()))

import scode
#
folder_key = str(scode.dt.datetime.now())
folder_key = "2023-03-23-run1"
folder = '.temp/vendor/' + folder_key + '/'
# scode.os.makedirs(folder, exist_ok=True)
#
# print("Starting creating csvs...")
# scode.save_all_ticker_csvs_parallel(temp_folder='', load_diff_only=False)

print("Done csvs, starting inputting into DB...")
scode.folder_csv_to_delta(folder)

scode.drop_duplicates()