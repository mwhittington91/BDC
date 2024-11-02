import sys
sys.path.append("./")
from src.supabase_db import copy_data_to_postgres, create_table_from_CSV

box_path = "/Users/mwhittington/Library/CloudStorage/Box-Box/BDC 2023-12-31"
csv_path = "exports/bdc_01_Cable_fixed_broadband_D23_14may2024.csv"
csv_path2 = "exports/bdc_01_LBRFixedWireless_fixed_broadband_D23_14may2024.csv"


#df = pd.read_csv(csv_path, encoding="utf-8")

if __name__ == "__main__":

    print(copy_data_to_postgres(csv_path, "bdc_info"))

    # print(create_table_from_CSV(csv_path, "bdc_info"))
