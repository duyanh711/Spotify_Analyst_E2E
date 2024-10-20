from scrap_artists_name import ScrapArtistName
from extract_data import extract_data
from spotify_api_auth import SpotifyAuth
from spotify_scrapper import SpotifyCrawler
from postgre_process import PostgreSQL


def main():
    # Step 1: Scrape artist names and save to a file
    scrap = ScrapArtistName()
    scrap.artists_crawler()  # This will scrape and store artist names in the file
    final_artists_information, final_albums_information, \
        final_tracks_information, final_tracks_features_information = \
        extract_data()
    # Step 4: Load the data into PostgreSQL
    db_config = {
        'dbname': 'spotify',
        'user': 'spotify',
        'password': 'spotify',
        'host': 'localhost',
        'port': '5432'
    }

    # Connect to PostgreSQL
    postgres_db = PostgreSQL(db_config)

    # Insert the data into PostgreSQL
    cols = ['id', 'external_urls', 'followers', 'name', 'popularity']
    cols_to_create = [
    "id VARCHAR(255) NOT NULL",
    "external_urls VARCHAR(255)",
    "followers INTEGER",
    "name VARCHAR(255)",
    "popularity INTEGER"
    ]
    postgres_db.create_table('artists', cols_to_create)
    postgres_db.insert_many(final_artists_information, 'artists', cols)
    # postgres_db.insert_many(final_albums_information, 'albums')
    # postgres_db.insert_many(final_tracks_information, 'tracks')
    # postgres_db.insert_many(final_tracks_features_information, 'audio_features')

    print("Data successfully loaded into PostgreSQL!")

if __name__ == "__main__":
    main()