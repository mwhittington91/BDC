import pytest
from unittest.mock import AsyncMock, patch
from src.bdc_api import BDC


@pytest.fixture
def bdc_client():
    return BDC("test_username", "test_api_key")


class TestGetListOfDates:
    @pytest.mark.asyncio
    async def test_getlistofDates_success(self, bdc_client):
        # Mock the response data
        mock_response = {
            "data_type": "State",
            "dates": ["2024-03-13", "2024-03-12", "2024-03-11"],
        }

        # Mock the httpx client
        with patch("httpx.AsyncClient.get") as mock_get:
            mock_get.return_value = AsyncMock(
                status_code=200, json=lambda: mock_response
            )

            # Test the method
            data_type, dates = await bdc_client.getlistofDates()

            # Verify the results
            assert data_type == "State"
            assert len(dates) == 3
            assert "2024-03-13" in dates
            assert "2024-03-12" in dates
            assert "2024-03-11" in dates

    @pytest.mark.asyncio
    async def test_getlistofDates_empty_response(self, bdc_client):
        # Mock empty response
        mock_response = {"data_type": "State", "dates": []}

        with patch("httpx.AsyncClient.get") as mock_get:
            mock_get.return_value = AsyncMock(
                status_code=200, json=lambda: mock_response
            )

            data_type, dates = await bdc_client.getlistofDates()
            assert data_type == "State"
            assert len(dates) == 0


class TestGetDownloadList:
    @pytest.mark.asyncio
    async def test_getDownloadList_success(self, bdc_client):
        # Mock the response data
        mock_response = [
            {"file_id": "123", "file_name": "test_file_1.csv", "file_size": 1000},
            {"file_id": "456", "file_name": "test_file_2.csv", "file_size": 2000},
        ]

        with patch("httpx.AsyncClient.get") as mock_get:
            mock_get.return_value = AsyncMock(
                status_code=200, json=lambda: mock_response
            )

            download_list = await bdc_client.getDownloadList(
                date="2024-03-13", category="State"
            )

            assert len(download_list) == 2
            assert download_list[0]["file_id"] == "123"
            assert download_list[1]["file_id"] == "456"

    @pytest.mark.asyncio
    async def test_getDownloadList_empty_response(self, bdc_client):
        with patch("httpx.AsyncClient.get") as mock_get:
            mock_get.return_value = AsyncMock(status_code=200, json=lambda: [])

            download_list = await bdc_client.getDownloadList(
                date="2024-03-13", category="State"
            )
            assert len(download_list) == 0


class TestGetDownloadFile:
    @pytest.mark.asyncio
    async def test_getDownloadFile_success(self, bdc_client):
        mock_content = b"test,csv,content\n1,2,3"

        with patch("httpx.AsyncClient.get") as mock_get:
            mock_response = AsyncMock(status_code=200, content=mock_content)
            mock_get.return_value = mock_response

            response = await bdc_client.getDownloadFile("123")

            assert response.content == mock_content
            assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_getDownloadFile_large_file(self, bdc_client):
        # Create a large mock content
        mock_content = b"x" * (10 * 1024 * 1024)  # 10MB

        with patch("httpx.AsyncClient.get") as mock_get:
            mock_response = AsyncMock(status_code=200, content=mock_content)
            mock_get.return_value = mock_response

            response = await bdc_client.getDownloadFile("123")

            assert len(response.content) == len(mock_content)
            assert response.status_code == 200


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_api_error_handling(self, bdc_client):
        with patch("httpx.AsyncClient.get") as mock_get:
            mock_get.return_value = AsyncMock(
                status_code=404, json=lambda: {"error": "Not found"}
            )

            # Test error handling for getlistofDates
            with pytest.raises(Exception):
                await bdc_client.getlistofDates()

            # Test error handling for getDownloadList
            with pytest.raises(Exception):
                await bdc_client.getDownloadList(date="2024-03-13", category="State")

            # Test error handling for getDownloadFile
            with pytest.raises(Exception):
                await bdc_client.getDownloadFile("123")

    @pytest.mark.asyncio
    async def test_network_error_handling(self, bdc_client):
        with patch("httpx.AsyncClient.get") as mock_get:
            mock_get.side_effect = Exception("Network error")

            # Test network error handling for all methods
            with pytest.raises(Exception):
                await bdc_client.getlistofDates()

            with pytest.raises(Exception):
                await bdc_client.getDownloadList(date="2024-03-13", category="State")

            with pytest.raises(Exception):
                await bdc_client.getDownloadFile("123")

    @pytest.mark.asyncio
    async def test_invalid_json_response(self, bdc_client):
        with patch("httpx.AsyncClient.get") as mock_get:
            mock_get.return_value = AsyncMock(
                status_code=200, json=lambda: "invalid json"
            )

            # Test invalid JSON handling
            with pytest.raises(Exception):
                await bdc_client.getlistofDates()

            with pytest.raises(Exception):
                await bdc_client.getDownloadList(date="2024-03-13", category="State")
