from spotify_api_auth import SpotifyAuth
from spotify_scrapper import SpotifyCrawler
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def spotify_crawler(artists_name, start_index=0, end_index=20):
    print("Start crawling ...")
    try:
        client_id = os.getenv("SPOTIFY_CLIENT_ID")
        client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
        sa = SpotifyAuth(client_id, client_secret)
    except Exception:
        raise Exception("Invalid Token")
    # Initialize Spotify Scrapper
    sc = SpotifyCrawler(sa)

    if start_index < 0 or start_index >= len(artists_name):
        raise Exception("Invalid start index")
    elif start_index > end_index:
        raise Exception("Invalid start and end index")
    elif end_index > len(artists_name):
        raise Exception("Invalid end index")
    else:
        try:
            final_artists_information, final_albums_information, final_tracks_information, final_tracks_features_information = sc.get_all_information_from_artists(
                artists_name[start_index:end_index])
        except RuntimeError:
            raise Exception("Max retry attempts reached!")
    return final_artists_information, final_albums_information, final_tracks_information, final_tracks_features_information


def read_artists_from_txt(file_path):
    """Reads artist names from a text file and stores them in a list"""
    with open(file_path, 'r', encoding='utf-8') as file:
        # Read all lines from the file and strip any trailing newlines or spaces
        artists_list = [line.strip() for line in file.readlines()]
    return artists_list

def extract_data():
    artists_name = read_artists_from_txt("./data/artists_name.txt")
    final_artists_information,\
        final_albums_information, \
        final_tracks_information, \
        final_tracks_features_information = spotify_crawler(artists_name=artists_name, start_index=0, end_index=20)
    return final_artists_information, \
            final_albums_information, \
            final_tracks_information, \
            final_tracks_features_information

if __name__ == "__main__":
    # Testing
    artists_name = read_artists_from_txt("./data/artists_name.txt")
    spotify_crawler(artists_name=artists_name, start_index=0, end_index=1)