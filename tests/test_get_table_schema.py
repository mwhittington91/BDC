import sys
sys.path.append("./")
from src.supabase_db import get_table_schema
import pytest

def test_get_table_schema():
    table_name = "bdc_info"
    print(get_table_schema(table_name))
    assert get_table_schema(table_name)[1] == "bdc_info"
    assert get_table_schema(table_name)[0] is not None

if __name__ == "__main__":
    test_get_table_schema()
