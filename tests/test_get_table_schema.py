import sys
import os
import pytest
from unittest.mock import patch, MagicMock

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.supabase_db import get_table_schema


@pytest.fixture
def mock_supabase():
    with patch("db.supabase_db.supabase") as mock:
        # Create a mock response
        mock_response = MagicMock()
        mock_response.data = [{"column_name": "test", "data_type": "text"}]
        mock_response.execute.return_value = mock_response

        # Set up the mock chain
        mock.rpc.return_value = mock_response
        yield mock


def test_get_table_schema(mock_supabase):
    table_name = "bdc_info"
    try:
        result = get_table_schema(table_name)
        print(f"Schema result: {result}")
        assert result[1] == "bdc_info"
        assert result[0] is not None

        # Verify the mock was called correctly
        mock_supabase.rpc.assert_called_once_with(
            "get_table_schema", {"p_table_name": table_name}
        )
    except Exception as e:
        print(f"Error getting schema: {e}")
        raise


if __name__ == "__main__":
    try:
        test_get_table_schema()
        print("Test passed!")
    except AssertionError as e:
        print(f"Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error running test: {e}")
        sys.exit(1)
