import duckdb
import sqlite3
import os

def convert_duckdb_to_sqlite(input_path, output_path):
    """
    Convert a DuckDB database to SQLite database.
    
    Parameters:
    - input_path (str): Path to the input DuckDB database file
    - output_path (str): Path where the SQLite database will be created
    
    Returns:
    - bool: True if conversion was successful, False otherwise
    """
    try:
        # Ensure the output directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # Remove the output file if it already exists
        if os.path.exists(output_path):
            os.remove(output_path)
            
        # Connect to DuckDB database
        duck_conn = duckdb.connect(input_path)
        
        # Get list of all tables in DuckDB
        tables = duck_conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'main'
        """).fetchall()
        
        # Connect to SQLite database
        sqlite_conn = sqlite3.connect(output_path)
        
        # Copy each table to SQLite
        for table in tables:
            table_name = table[0]
            print(f"Converting table: {table_name}")
            
            # Get table schema
            schema = duck_conn.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                AND table_schema = 'main'
                ORDER BY ordinal_position
            """).fetchall()
            
            # Map DuckDB types to SQLite types
            type_mapping = {
                'INTEGER': 'INTEGER',
                'BIGINT': 'INTEGER',
                'SMALLINT': 'INTEGER',
                'TINYINT': 'INTEGER',
                'VARCHAR': 'TEXT',
                'CHAR': 'TEXT',
                'TEXT': 'TEXT',
                'DOUBLE': 'REAL',
                'FLOAT': 'REAL',
                'DECIMAL': 'REAL',
                'BOOLEAN': 'INTEGER',
                'DATE': 'TEXT',
                'TIMESTAMP': 'TEXT',
                'TIME': 'TEXT',
            }
            
            # Create table in SQLite
            create_table_sql = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" (\n"
            columns = []
            
            for col_name, col_type in schema:
                sqlite_type = 'TEXT'  # Default type
                for duck_type, sql_type in type_mapping.items():
                    if duck_type in col_type.upper():
                        sqlite_type = sql_type
                        break
                columns.append(f"\"{col_name}\" {sqlite_type}")
            
            create_table_sql += ",\n".join(columns)
            create_table_sql += "\n);"
            
            sqlite_conn.execute(create_table_sql)
            
            # Get data from DuckDB
            data = duck_conn.execute(f'SELECT * FROM "{table_name}"').fetchall()
            
            if data:
                # Prepare INSERT statement
                placeholders = ", ".join(["?" for _ in range(len(schema))])
                insert_sql = f'INSERT INTO "{table_name}" VALUES ({placeholders})'
                
                # Insert data into SQLite
                sqlite_conn.executemany(insert_sql, data)
        
        # Commit changes and close connections
        sqlite_conn.commit()
        sqlite_conn.close()
        duck_conn.close()
        
        print(f"✅ Successfully converted DuckDB database to SQLite: {output_path}")
        return True
        
    except Exception as e:
        print(f"❌ Error converting DuckDB to SQLite: {str(e)}")
        return False
