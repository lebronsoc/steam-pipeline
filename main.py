import os
import requests
from datetime import datetime
from supabase import create_client

# ------------------------
# SUPABASE SETUP
# ------------------------
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ------------------------
# GET STEAM APP LIST (SAFE)
# ------------------------
def get_app_list():
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    try:
        r = requests.get(url, headers=headers, timeout=30)

        print("Steam status:", r.status_code)

        if r.status_code != 200:
            print("Steam API failed:", r.text[:200])
            return []

        data = r.json()
        return data.get("applist", {}).get("apps", [])

    except Exception as e:
        print("Error fetching app list:", e)
        return []

# ------------------------
# GET PLAYER COUNT
# ------------------------
def get_players(app_id):
    url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={app_id}"

    try:
        r = requests.get(url, timeout=15)
        data = r.json()
        return data.get("response", {}).get("player_count")
    except:
        return None

# ------------------------
# VALIDATE GAME
# ------------------------
def is_valid_game(app_id):
    url = f"https://store.steampowered.com/api/appdetails?appids={app_id}"

    try:
        r = requests.get(url, timeout=15)
        data = r.json()
        return data.get(str(app_id), {}).get("success", False)
    except:
        return False

# ------------------------
# MAIN PIPELINE
# ------------------------
apps = get_app_list()

print("Total apps fetched:", len(apps))

count = 0

# limit for safety
for app in apps[:200]:

    app_id = app.get("appid")
    name = app.get("name")

    if not app_id or not name:
        continue

    try:
        # filter real games
        if not is_valid_game(app_id):
            continue

        players = get_players(app_id)

        print(name, "->", players)

        # ------------------------
        # UPSERT GAME
        # ------------------------
        supabase.table("steam_games").upsert({
            "app_id": app_id,
            "name": name,
            "last_updated": datetime.utcnow().isoformat()
        }).execute()

        # ------------------------
        # INSERT HISTORY
        # ------------------------
        supabase.table("steam_player_history").insert({
            "app_id": app_id,
            "name": name,
            "current_players": players,
            "recorded_at": datetime.utcnow().isoformat()
        }).execute()

        count += 1

    except Exception as e:
        print("Error processing", app_id, ":", e)

print("DONE. Processed:", count)