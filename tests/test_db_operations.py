import pytest
from sqlalchemy import create_engine, MetaData, inspect
from sqlalchemy.engine import Engine
from db.schema import copy_data_to_postgres, create_bdc_table


@pytest.fixture
def test_engine():
    # Create an in-memory SQLite database
    engine = create_engine("sqlite:///:memory:")
    return engine


@pytest.fixture
def test_metadata():
    return MetaData()


@pytest.fixture
def sample_csv_content():
    return """frn,provider_id,brand_name,location_id,technology,max_advertised_download_speed,max_advertised_upload_speed,low_latency,business_residential_code,state_usps,block_geoid,h3_res8_id
123,456,Test Brand,789,Fiber,1000,100,Yes,1,CA,123456789,abc123"""


class TestCreateBDCTable:
    def test_create_bdc_table_success(self, test_engine, test_metadata):
        # Create a test table
        table_name = "test_bdc_table"
        create_bdc_table(table_name, test_metadata)
        test_metadata.create_all(test_engine)

        # Verify the table was created
        inspector = inspect(test_engine)
        assert table_name in inspector.get_table_names()

        # Verify the columns
        columns = inspector.get_columns(table_name)
        column_names = [col["name"] for col in columns]
        expected_columns = [
            "frn",
            "provider_id",
            "brand_name",
            "location_id",
            "technology",
            "max_advertised_download_speed",
            "max_advertised_upload_speed",
            "low_latency",
            "business_residential_code",
            "state_usps",
            "block_geoid",
            "h3_res8_id",
        ]
        assert all(col in column_names for col in expected_columns)

        # Verify column types
        column_types = {col["name"]: str(col["type"]) for col in columns}
        assert "VARCHAR" in column_types["frn"]
        assert "VARCHAR" in column_types["provider_id"]
        assert "VARCHAR" in column_types["brand_name"]
        assert "INTEGER" in column_types["location_id"]
        assert "VARCHAR" in column_types["technology"]
        assert "VARCHAR" in column_types["max_advertised_download_speed"]
        assert "VARCHAR" in column_types["max_advertised_upload_speed"]
        assert "VARCHAR" in column_types["low_latency"]
        assert "VARCHAR" in column_types["business_residential_code"]
        assert "VARCHAR" in column_types["state_usps"]
        assert "INTEGER" in column_types["block_geoid"]
        assert "VARCHAR" in column_types["h3_res8_id"]

    def test_create_bdc_table_duplicate(self, test_engine, test_metadata):
        # Create a test table
        table_name = "test_bdc_table"
        create_bdc_table(table_name, test_metadata)
        test_metadata.create_all(test_engine)

        # Try to create the same table again
        create_bdc_table(table_name, test_metadata)
        test_metadata.create_all(test_engine)

        # Verify only one table exists
        inspector = inspect(test_engine)
        tables = inspector.get_table_names()
        assert len([t for t in tables if t == table_name]) == 1


class TestCopyDataToPostgres:
    def test_copy_data_to_postgres_success(
        self, test_engine, test_metadata, tmp_path, sample_csv_content
    ):
        # Create a test CSV file
        csv_file = tmp_path / "test.csv"
        with open(csv_file, "w") as f:
            f.write(sample_csv_content)

        # Create the table
        table_name = "test_bdc_table"
        create_bdc_table(table_name, test_metadata)
        test_metadata.create_all(test_engine)

        # Copy data to the table
        copy_data_to_postgres(test_engine, str(csv_file), table_name)

        # Verify the data was copied
        with test_engine.connect() as conn:
            result = conn.execute(f"SELECT * FROM {table_name}").fetchall()
            assert len(result) == 1

            # Verify the data
            row = result[0]
            assert row.frn == "123"
            assert row.provider_id == "456"
            assert row.brand_name == "Test Brand"
            assert row.location_id == 789
            assert row.technology == "Fiber"
            assert row.max_advertised_download_speed == "1000"
            assert row.max_advertised_upload_speed == "100"
            assert row.low_latency == "Yes"
            assert row.business_residential_code == "1"
            assert row.state_usps == "CA"
            assert row.block_geoid == 123456789
            assert row.h3_res8_id == "abc123"

    def test_copy_data_to_postgres_empty_file(
        self, test_engine, test_metadata, tmp_path
    ):
        # Create an empty CSV file
        csv_file = tmp_path / "empty.csv"
        csv_file.touch()

        # Create the table
        table_name = "test_bdc_table"
        create_bdc_table(table_name, test_metadata)
        test_metadata.create_all(test_engine)

        # Copy data to the table
        copy_data_to_postgres(test_engine, str(csv_file), table_name)

        # Verify no data was copied
        with test_engine.connect() as conn:
            result = conn.execute(f"SELECT * FROM {table_name}").fetchall()
            assert len(result) == 0

    def test_copy_data_to_postgres_invalid_data(
        self, test_engine, test_metadata, tmp_path
    ):
        # Create a CSV file with invalid data
        csv_file = tmp_path / "invalid.csv"
        with open(csv_file, "w") as f:
            f.write("""frn,provider_id,brand_name,location_id,technology,max_advertised_download_speed,max_advertised_upload_speed,low_latency,business_residential_code,state_usps,block_geoid,h3_res8_id
123,456,Test Brand,invalid,Fiber,1000,100,Yes,1,CA,123456789,abc123""")

        # Create the table
        table_name = "test_bdc_table"
        create_bdc_table(table_name, test_metadata)
        test_metadata.create_all(test_engine)

        # Copy data to the table
        with pytest.raises(Exception):
            copy_data_to_postgres(test_engine, str(csv_file), table_name)

    def test_copy_data_to_postgres_missing_columns(
        self, test_engine, test_metadata, tmp_path
    ):
        # Create a CSV file with missing columns
        csv_file = tmp_path / "missing_columns.csv"
        with open(csv_file, "w") as f:
            f.write("""frn,provider_id,brand_name
123,456,Test Brand""")

        # Create the table
        table_name = "test_bdc_table"
        create_bdc_table(table_name, test_metadata)
        test_metadata.create_all(test_engine)

        # Copy data to the table
        with pytest.raises(Exception):
            copy_data_to_postgres(test_engine, str(csv_file), table_name)

    def test_copy_data_to_postgres_duplicate_data(
        self, test_engine, test_metadata, tmp_path, sample_csv_content
    ):
        # Create a test CSV file
        csv_file = tmp_path / "test.csv"
        with open(csv_file, "w") as f:
            f.write(sample_csv_content)

        # Create the table
        table_name = "test_bdc_table"
        create_bdc_table(table_name, test_metadata)
        test_metadata.create_all(test_engine)

        # Copy data twice
        copy_data_to_postgres(test_engine, str(csv_file), table_name)
        copy_data_to_postgres(test_engine, str(csv_file), table_name)

        # Verify duplicate data was handled
        with test_engine.connect() as conn:
            result = conn.execute(f"SELECT * FROM {table_name}").fetchall()
            assert len(result) == 2  # Should have two rows
