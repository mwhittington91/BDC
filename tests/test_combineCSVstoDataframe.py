import sys
sys.path.append("./")
from src.utils import combineCSVsintoDataFrame

box_path = "/Users/mwhittington/Library/CloudStorage/Box-Box/BDC 2023-12-31"

if __name__ == "__main__":
    df = combineCSVsintoDataFrame()
    print(df.head())
