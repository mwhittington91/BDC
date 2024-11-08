get_table_schema_tool = {
"type": "function",  # Add this line
    "function": {        # Wrap your existing function definition in a "function" key
        "name": "get_table_schema",
        "description": "Retrieves the schema (column names and data types) for a specified table in a PostgreSQL database",
        "parameters": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "The name of the table whose schema you want to retrieve"
                }
            },
            "required": ["table_name"]
    },
    "returns": {
        "type": "string",
        "description": "Returns a formatted string containing the table name and its schema (column names and their corresponding data types), or False if there's an error"
    }
}}
