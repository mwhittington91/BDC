import os
import sys

import pytest
from sqlalchemy import MetaData, create_engine

sys.path.append("./")
from db.schema import create_bdc_table


def test_create_table():
    engine = create_engine("sqlite:///test.db")
    metadata = MetaData()
    table_name = "bdc_info"
    table = create_bdc_table(table_name, metadata)
    metadata.create_all(engine)
    assert table.name == table_name
    assert len(table.columns) == 12
    assert table.columns[0].name == "frn"
    assert table.columns[1].name == "provider_id"
    assert table.columns[2].name == "brand_name"
    assert table.columns[3].name == "location_id"
    assert table.columns[4].name == "technology"
    assert table.columns[5].name == "max_advertised_download_speed"
    assert table.columns[6].name == "max_advertised_upload_speed"
    assert table.columns[7].name == "low_latency"
    assert table.columns[8].name == "business_residential_code"
    assert table.columns[9].name == "state_usps"
    assert table.columns[10].name == "block_geoid"
    assert table.columns[11].name == "h3_res8_id"


if __name__ == "__main__":
    pytest.main(["-v", __file__])
    os.remove("test.db")
