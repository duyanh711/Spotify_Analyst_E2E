import psycopg2
import pandas as pd

def extract_data_from_db(tables, connection_params, output_path):
    try:
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        for table in tables:
            query = f"SELECT * FROM {table}"
            cursor.execute(query)
            cols_name = [desc[0] for desc in cursor.description]
            records = cursor.fetchall()
            df = pd.DataFrame(records, columns=cols_name)

            df.to_csv(f"{output_path}/{table}.csv", index=False)

        cursor.close()
        conn.close()
    except Exception as e:
        raise(e)

# Test
if __name__ == "__main__":
    connection_params = {
        'dbname': 'spotify',
        'user': 'spotify',
        'password': 'spotify',
        'host': 'localhost',
        'port': '5432'
    }
    output_directory = "./data"
    tables = ['artists', 'albums', 'tracks', 'audio_features']
    extract_data_from_db(tables, connection_params, output_directory)