import os
import pytest
import pandas as pd
from io import BytesIO
import zipfile
from src.utils import (
    extractZip,
    upload_file_to_zapier,
    upload_tempfile_to_zapier,
    combineCSVsintoDataFrame,
    zip_exports_if_large,
)


@pytest.fixture
def sample_csv_content():
    return """frn,provider_id,brand_name,location_id,technology,max_advertised_download_speed,max_advertised_upload_speed,low_latency,business_residential_code,state_usps,block_geoid,h3_res8_id
123,456,Test Brand,789,Fiber,1000,100,Yes,1,CA,123456789,abc123"""


@pytest.fixture
def sample_zip_file(tmp_path, sample_csv_content):
    zip_path = tmp_path / "test.zip"
    csv_path = tmp_path / "test.csv"

    # Create a CSV file
    with open(csv_path, "w") as f:
        f.write(sample_csv_content)

    # Create a ZIP file
    with zipfile.ZipFile(zip_path, "w") as zipf:
        zipf.write(csv_path, arcname="test.csv")

    return zip_path, csv_path


@pytest.fixture
def sample_exports_dir(tmp_path, sample_csv_content):
    exports_dir = tmp_path / "exports"
    exports_dir.mkdir()

    # Create multiple CSV files with different content
    for i in range(3):
        csv_path = exports_dir / f"test_{i}.csv"
        with open(csv_path, "w") as f:
            # Modify some values for each file
            modified_content = sample_csv_content.replace("123", str(123 + i)).replace(
                "456", str(456 + i)
            )
            f.write(modified_content)

    return exports_dir


class TestExtractZip:
    def test_extract_zip_success(self, tmp_path, sample_zip_file):
        zip_path, csv_path = sample_zip_file
        exports_dir = tmp_path / "exports"
        exports_dir.mkdir()

        # Create a mock response object
        class MockResponse:
            def __init__(self, content):
                self.content = content

        with open(zip_path, "rb") as f:
            mock_response = MockResponse(f.read())

        # Test the extraction
        extractZip(mock_response, "test")

        # Verify the file was extracted
        assert (exports_dir / "test.csv").exists()

        # Verify the content
        with open(exports_dir / "test.csv", "r") as f:
            content = f.read()
            assert "Test Brand" in content
            assert "Fiber" in content

    def test_extract_zip_invalid_content(self, tmp_path):
        exports_dir = tmp_path / "exports"
        exports_dir.mkdir()

        # Create a mock response with invalid content
        class MockResponse:
            def __init__(self):
                self.content = b"invalid zip content"

        mock_response = MockResponse()

        # Test that it raises an error
        with pytest.raises(zipfile.BadZipFile):
            extractZip(mock_response, "test")


class TestUploadToZapier:
    @pytest.mark.asyncio
    async def test_upload_file_to_zapier_success(self, tmp_path, sample_csv_content):
        # Create a test file
        test_file = tmp_path / "test.csv"
        with open(test_file, "w") as f:
            f.write(sample_csv_content)

        # Mock the httpx client
        with pytest.MonkeyPatch.context() as m:

            async def mock_post(*args, **kwargs):
                class MockResponse:
                    def __init__(self):
                        self.status_code = 200

                return MockResponse()

            m.setattr("httpx.AsyncClient.post", mock_post)

            # Test successful upload
            result = await upload_file_to_zapier(
                str(test_file), "http://test-webhook.com", "test.csv", "test_table"
            )
            assert result is True

    @pytest.mark.asyncio
    async def test_upload_file_to_zapier_file_too_large(self, tmp_path):
        # Create a large test file
        test_file = tmp_path / "large.csv"
        with open(test_file, "w") as f:
            f.write("x" * (101 * 1024 * 1024))  # 101MB file

        # Test upload with file too large
        result = await upload_file_to_zapier(
            str(test_file), "http://test-webhook.com", "large.csv", "test_table"
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_upload_file_to_zapier_network_error(
        self, tmp_path, sample_csv_content
    ):
        # Create a test file
        test_file = tmp_path / "test.csv"
        with open(test_file, "w") as f:
            f.write(sample_csv_content)

        # Mock the httpx client to simulate network error
        with pytest.MonkeyPatch.context() as m:

            async def mock_post(*args, **kwargs):
                raise httpx.RequestError("Network error")

            m.setattr("httpx.AsyncClient.post", mock_post)

            # Test network error handling
            result = await upload_file_to_zapier(
                str(test_file), "http://test-webhook.com", "test.csv", "test_table"
            )
            assert result is False


class TestCombineCSVs:
    def test_combine_csvs_into_dataframe_success(self, sample_exports_dir):
        # Test combining CSV files
        df = combineCSVsintoDataFrame(str(sample_exports_dir))

        # Verify the DataFrame
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3  # Should have 3 rows (one from each CSV)
        assert all(col in df.columns for col in ["frn", "provider_id", "brand_name"])

        # Verify data from different files was combined
        frn_values = df["frn"].unique()
        assert len(frn_values) == 3  # Should have 3 different FRN values

    def test_combine_csvs_into_dataframe_empty_dir(self, tmp_path):
        # Test with empty directory
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        df = combineCSVsintoDataFrame(str(empty_dir))
        assert df is None

    def test_combine_csvs_into_dataframe_invalid_files(self, tmp_path):
        # Create directory with invalid CSV files
        invalid_dir = tmp_path / "invalid"
        invalid_dir.mkdir()

        # Create an invalid CSV file
        with open(invalid_dir / "invalid.csv", "w") as f:
            f.write("invalid,csv,content\n1,2,3")

        # Test with invalid files
        df = combineCSVsintoDataFrame(str(invalid_dir))
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1


class TestZipExports:
    def test_zip_exports_if_large_success(self, sample_exports_dir):
        # Create a large file
        large_file = sample_exports_dir / "large.csv"
        with open(large_file, "w") as f:
            f.write("x" * (101 * 1024 * 1024))  # 101MB file

        # Test zipping
        files_to_upload = zip_exports_if_large(str(sample_exports_dir), max_size_mb=100)

        # Verify the results
        assert len(files_to_upload) > 0
        assert any("large.csv.zip" in str(path) for path, _ in files_to_upload)

        # Verify the zip file exists and is smaller
        zip_file = next(
            path for path, _ in files_to_upload if "large.csv.zip" in str(path)
        )
        assert os.path.exists(zip_file)
        assert os.path.getsize(zip_file) < 101 * 1024 * 1024

    def test_zip_exports_if_large_no_large_files(self, sample_exports_dir):
        # Test with no large files
        files_to_upload = zip_exports_if_large(str(sample_exports_dir), max_size_mb=100)

        # Verify all files are original CSVs
        assert all(not str(path).endswith(".zip") for path, _ in files_to_upload)
        assert len(files_to_upload) == 3  # Should have all 3 original files
