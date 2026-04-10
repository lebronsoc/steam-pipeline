import requests
from datetime import datetime
from supabase import create_client
import os

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ------------------------
# GET ALL STEAM APPS
# ------------------------
def get_app_list():
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    return requests.get(url).json()["applist"]["apps"]

# ------------------------
# GET PLAYER COUNT
# ------------------------
def get_players(app_id):
    url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={app_id}"
    return requests.get(url).json().get("response", {}).get("player_count")

# ------------------------
# BASIC FILTER (removes junk apps)
# ------------------------
def is_valid(app_id):
    url = f"https://store.steampowered.com/api/appdetails?appids={app_id}"
    r = requests.get(url).json()
    return r.get(str(app_id), {}).get("success", False)

apps = get_app_list()

print("Total apps:", len(apps))

count = 0

for app in apps[:200]:  # start small so it doesn't break
    app_id = app["appid"]
    name = app["name"]

    try:
        if not is_valid(app_id):
            continue

        players = get_players(app_id)

        print(name, players)

        supabase.table("steam_games").upsert({
            "app_id": app_id,
            "name": name,
            "last_updated": datetime.utcnow().isoformat()
        }).execute()

        supabase.table("steam_player_history").insert({
            "app_id": app_id,
            "name": name,
            "current_players": players,
            "recorded_at": datetime.utcnow().isoformat()
        }).execute()

        count += 1

    except Exception as e:
        print("Error:", app_id, e)

print("DONE:", count)