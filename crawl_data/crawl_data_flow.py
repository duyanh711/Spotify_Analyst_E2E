from crawl_data.scrap_artists_name import ScrapArtistName
from crawl_data.crawl_full_data import extract_data
from crawl_data.postgre_process import PostgreSQL


def scrape_data_from_API():
    scrap = ScrapArtistName()
    scrap.artists_crawler() 

def load_data_to_db():
    final_artists_information, final_albums_information, \
        final_tracks_information, final_tracks_features_information = \
        extract_data()
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
    cols_artists = ['id', 'external_urls', 'followers', 'name', 'popularity']
    cols_to_create_artists = [
        "id VARCHAR(255) PRIMARY KEY",
        "external_urls VARCHAR(255)",
        "followers INTEGER",
        "name VARCHAR(255)",
        "popularity INTEGER"
    ]

    cols_to_create_albums = [
        "id VARCHAR(255) PRIMARY KEY",
        "artist_id VARCHAR(255) REFERENCES artists(id)",
        "name VARCHAR(255)",
        "album_type VARCHAR(255)",
        "external_urls VARCHAR(255)",
        "label VARCHAR(255)",
        "popularity INT",
        "release_date VARCHAR(255)",
        "release_date_precision VARCHAR(255)",
        "total_tracks INT"
    ]
    cols_albums = ["id", "artist_id", "name", "album_type", "external_urls", "label", "popularity",\
                    "release_date", "release_date_precision", "total_tracks"]

    cols_to_create_tracks = [
        "id VARCHAR PRIMARY KEY",
        "artist_id VARCHAR REFERENCES artists(id)",
        "album_id VARCHAR REFERENCES albums(id)",
        "disc_number INT",
        "explicit BOOLEAN",
        "name VARCHAR",
        "external_urls VARCHAR",
        "track_number INT",
        "popularity INT",
        "duration_ms INT"
        ]
    cols_tracks = ["id", "artist_id", "album_id", "disc_number", "explicit", "name", "external_urls",\
                    "track_number", "popularity", "duration_ms"]

    cols_to_create_tracks_features = [
        "id VARCHAR REFERENCES tracks(id)",
        "danceability DOUBLE PRECISION",
        "energy DOUBLE PRECISION",
        "key INT",
        "loudness DOUBLE PRECISION",
        "mode INT",
        "speechiness DOUBLE PRECISION",
        "acousticness DOUBLE PRECISION",
        "instrumentalness DOUBLE PRECISION",
        "liveness DOUBLE PRECISION",
        "valence DOUBLE PRECISION",
        "tempo DOUBLE PRECISION",
        "duration_ms INT",
        "time_signature INT",
        "PRIMARY KEY (id)"
        ]
    cols_tracks_features = ["id", "danceability", "energy", "key", "loudness", "mode","speechiness",\
                            "acousticness", "instrumentalness", "liveness", "valence", "tempo", "duration_ms", "time_signature"]

    postgres_db.create_table('artists', cols_to_create_artists)
    postgres_db.create_table('albums', cols_to_create_albums)
    postgres_db.create_table('tracks', cols_to_create_tracks)
    postgres_db.create_table('audio_features', cols_to_create_tracks_features)
    postgres_db.insert_many(final_artists_information, 'artists', cols_artists)
    postgres_db.insert_many(final_albums_information, 'albums', cols_albums)
    postgres_db.insert_with_existing_fk_ids('artists','tracks', 'artist_id', final_tracks_information, cols_tracks)
    postgres_db.insert_with_existing_fk_ids('tracks', 'audio_features', 'id', final_tracks_features_information, cols_tracks_features)

    print("Data successfully loaded into PostgreSQL!")

if __name__ == "__main__":
    scrape_data_from_API()
    load_data_to_db()