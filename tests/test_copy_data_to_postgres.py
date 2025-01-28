from sqlalchemy import create_engine

from db.schema import copy_data_to_postgres


def test_copy_data_to_postgres():
    engine = create_engine("sqlite:///test.db")
    copy_data_to_postgres(
        engine,
        "/Users/mwhittington/Library/CloudStorage/Box-Box/Broadband Data/2023-12-31/bdc_01_Cable_fixed_broadband_D23_14may2024.csv",
        "bdc_info",
    )
    assert True
