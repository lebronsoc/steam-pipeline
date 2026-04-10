import requests
from datetime import datetime
from supabase import create_client, Client
import os

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "steam_games"
APP_IDS = [730, 570, 440, 578080]

def get_current_players(app_id):
    url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={app_id}"
    r = requests.get(url)
    return r.json().get("response", {}).get("player_count")

def get_game_data(app_id):
    url = f"https://store.steampowered.com/api/appdetails?appids={app_id}"
    r = requests.get(url)
    data = r.json()

    if not data[str(app_id)]["success"]:
        return None

    game = data[str(app_id)]["data"]

    genres = game.get("genres")
    genres_clean = ",".join([g["description"] for g in genres]) if genres else None

    return {
        "app_id": app_id,
        "name": game.get("name"),
        "release_date": game.get("release_date", {}).get("date"),
        "genres": genres_clean,
        "current_players": get_current_players(app_id),
        "last_updated": datetime.utcnow().isoformat()
    }

all_data = []

for app_id in APP_IDS:
    try:
        game = get_game_data(app_id)
        if game:
            all_data.append(game)
            print(game["name"], game["current_players"])
    except Exception as e:
        print("Error:", app_id, e)

if all_data:
    supabase.table(TABLE_NAME).upsert(all_data).execute()
    print("Updated Supabase:", len(all_data))
