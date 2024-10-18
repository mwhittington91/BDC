from src.utils import combineCSVstoDataframe

box_path = "/Users/mwhittington/Library/CloudStorage/Box-Box/BDC 2023-12-31"

df = combineCSVstoDataframe(box_path)
print(df.head())
