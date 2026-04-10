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
# GET APP LIST (STABLE + FALLBACK)
# ------------------------
def get_app_list():
    headers = {"User-Agent": "Mozilla/5.0"}

    # ---- Primary Steam endpoint ----
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"

    try:
        r = requests.get(url, headers=headers, timeout=30)
        print("Steam AppList status:", r.status_code)

        if r.status_code == 200:
            data = r.json()
            return data.get("applist", {}).get("apps", [])

        print("Primary failed, switching fallback...")

    except Exception as e:
        print("Primary error:", e)

    # ---- Fallback (SteamSpy mirror) ----
    try:
        fallback_url = "https://steamspy.com/api.php?request=all"
        r = requests.get(fallback_url, timeout=30)

        print("Fallback status:", r.status_code)

        data = r.json()

        # Convert SteamSpy format → same structure
        return [
            {"appid": int(k), "name": v.get("name", "Unknown")}
            for k, v in data.items()
            if k.isdigit()
        ]

    except Exception as e:
        print("Fallback failed:", e)
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

print("Total apps loaded:", len(apps))

if not apps:
    print("No apps fetched. Exiting.")
    exit()

count = 0

for app in apps[:200]:  # keep safe for GitHub Actions

    app_id = app.get("appid")
    name = app.get("name")

    if not app_id or not name:
        continue

    try:
        if not is_valid_game(app_id):
            continue

        players = get_players(app_id)

        print(name, "->", players)

        # ------------------------
        # UPSERT MASTER TABLE
        # ------------------------
        supabase.table("steam_games").upsert({
            "app_id": app_id,
            "name": name,
            "last_updated": datetime.utcnow().isoformat()
        }).execute()

        # ------------------------
        # INSERT HISTORY TABLE
        # ------------------------
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