import psycopg2
from psycopg2 import sql

class PostgreSQL:
    def __init__(self, conn_params: dict):
        self.conn = psycopg2.connect(**conn_params)
        self.cursor = self.conn.cursor()
    
    def __check_database_exist(self, db_name: str):
        self.cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname=%s", (db_name,))
        return self.cursor.fetchone() is not None
    
    def create_database(self, db_name: str):
        if not self.__check_database_exist(db_name):
            self.cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(db_name)))
            self.conn.commit()
        else:
            print(f"Database {db_name} đã tồn tại")

    def __check_table_exist(self, table_name: str):
        self.cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name=%s
            )
        """, (table_name,))
        return self.cursor.fetchone()[0]

    def create_table(self, table_name: str, columns: list):
        """Create a table in PostgreSQL."""
        # Construct the SQL query for creating a table
        create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({});").format(
            sql.Identifier(table_name),  # Use table name
            sql.SQL(', ').join(sql.SQL(column) for column in columns)  # Join column definitions
        )
        
        # Execute the query
        self.cursor.execute(create_table_query)
        self.conn.commit()

    def insert_many(self, data, table, columns):
        # Construct the SQL query to insert multiple rows
        insert_query = sql.SQL("""
        INSERT INTO {} ({}) VALUES ({}) 
        ON CONFLICT (id) DO UPDATE SET {}
                """).format(
                            sql.Identifier(table),  # Table name
                            sql.SQL(', ').join(map(sql.Identifier, columns)),  # Column names
                            sql.SQL(', ').join(sql.Placeholder() for _ in columns),  # Placeholders for values
                            sql.SQL(', ').join([sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col)) for col in columns])
                            )

        # Convert the data into a list of tuples for the placeholders
        # values = [tuple(row[column] for column in columns) for row in data]
        values = []
        for row in data:
            value_tuple = []
            for column in columns:
                value = row[column]
                # If the value is a dict, extract the relevant data
                if isinstance(value, dict):
                    # Assuming you want a specific key from the dict, adjust as necessary
                    if 'spotify' in value:
                        value_tuple.append(value['spotify'])  # or whatever key you want to extract
                    elif 'total' in value:
                        value_tuple.append(value['total'])  # adjust as necessary
                    else:
                        value_tuple.append(None)  # or handle missing keys accordingly
                else:
                    value_tuple.append(value)
            values.append(tuple(value_tuple))

        # Execute the query and pass the values
        self.cursor.executemany(insert_query, values)
        self.conn.commit()


    def insert_one(self, data, table_name: str, columns: str):
        insert_query = sql.SQL("INSERT INTO {} {} VALUES {}").format(
            sql.Identifier(table_name),
            # sql.SQL(columns),
            sql.SQL('(' + ', '.join(['%s'] * len(data)) + ')')
        )
        self.cursor.execute(insert_query, data)
        self.conn.commit()

    def insert_with_existing_fk_ids(self, fk_table_name, table_name, fk_col, data, columns):
        fk_ids = [row[f'{fk_col}'] for row in data]
        existing_fk_ids = self.get_existing_artist_ids_from_db(fk_table_name, fk_ids)
        valid_data = [record for record in data if record[f'{fk_col}'] in existing_fk_ids]

        if valid_data:
            try:
                self.insert_many(valid_data, table_name, columns)
            except Exception as e:
                print(f"Error inserting data into {table_name}: {str(e)}")
                raise
        else:
            print(f"No valid data to insert for {table_name}. Ensure that {fk_col} exists in the table.")

    def get_existing_artist_ids_from_db(self, table_name, fk_ids):
        query = sql.SQL("SELECT id FROM {} WHERE id = ANY(%s)").format(
            sql.Identifier(table_name)
        )
        self.cursor.execute(query, (fk_ids,))
        result = self.cursor.fetchall()
        return [row[0] for row in result]


    def close(self):
        """Đóng kết nối với database"""
        self.cursor.close()
        self.conn.close()

        