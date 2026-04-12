import os
import requests
from datetime import datetime, timezone
from supabase import create_client

# ------------------------
# CONFIG
# ------------------------
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

APP_LIST_URL = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
PLAYER_URL = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={appid}"
STORE_URL = "https://store.steampowered.com/api/appdetails?appids={appid}"

BATCH_SIZE = 200
REQUEST_TIMEOUT = 30
USER_AGENT = {"User-Agent": "Mozilla/5.0"}


# ------------------------
# HELPERS
# ------------------------
def utc_now():
    return datetime.now(timezone.utc).isoformat()


def get_app_list():
    """
    Primary Steam AppList endpoint with SteamSpy fallback.
    Returns a list of dicts with keys: appid, name.
    """
    try:
        r = requests.get(APP_LIST_URL, headers=USER_AGENT, timeout=REQUEST_TIMEOUT)
        print("Steam AppList status:", r.status_code)

        if r.status_code == 200:
            data = r.json()
            apps = data.get("applist", {}).get("apps", [])
            if apps:
                return apps
    except Exception as e:
        print("Primary AppList error:", e)

    # Fallback
    try:
        fallback_url = "https://steamspy.com/api.php?request=all"
        r = requests.get(fallback_url, timeout=REQUEST_TIMEOUT)
        print("Fallback status:", r.status_code)

        if r.status_code == 200:
            data = r.json()
            apps = []
            for appid_str, info in data.items():
                if appid_str.isdigit():
                    apps.append({
                        "appid": int(appid_str),
                        "name": info.get("name", "Unknown")
                    })
            return apps
    except Exception as e:
        print("Fallback AppList error:", e)

    return []


def get_players(app_id):
    """
    Live concurrent players for one app.
    """
    try:
        url = PLAYER_URL.format(appid=app_id)
        r = requests.get(url, headers=USER_AGENT, timeout=REQUEST_TIMEOUT)
        data = r.json()
        return data.get("response", {}).get("player_count")
    except Exception:
        return None


def is_valid_game(app_id):
    """
    Filters out junk apps, demos, tools, etc.
    """
    try:
        url = STORE_URL.format(appid=app_id)
        r = requests.get(url, headers=USER_AGENT, timeout=REQUEST_TIMEOUT)
        data = r.json()
        return data.get(str(app_id), {}).get("success", False)
    except Exception:
        return False


def get_state():
    """
    Reads pipeline position from Supabase.
    If missing, starts at 0.
    """
    try:
        res = supabase.table("pipeline_state").select("*").eq("id", 1).execute()
        if res.data and len(res.data) > 0:
            return int(res.data[0].get("last_index", 0))
    except Exception as e:
        print("State read error:", e)

    # Create default state if missing
    try:
        supabase.table("pipeline_state").upsert({
            "id": 1,
            "last_index": 0
        }).execute()
    except Exception as e:
        print("State init error:", e)

    return 0


def save_state(last_index):
    """
    Saves pipeline position back to Supabase.
    """
    try:
        supabase.table("pipeline_state").upsert({
            "id": 1,
            "last_index": int(last_index)
        }).execute()
    except Exception as e:
        print("State save error:", e)


# ------------------------
# MAIN
# ------------------------
def main():
    apps = get_app_list()
    print("Total apps loaded:", len(apps))

    if not apps:
        print("No apps fetched. Exiting.")
        return

    last_index = get_state()
    print("Starting from index:", last_index)

    if last_index >= len(apps):
        last_index = 0

    next_index = min(last_index + BATCH_SIZE, len(apps))
    subset = apps[last_index:next_index]

    processed = 0
    added_or_updated = 0

    for app in subset:
        app_id = app.get("appid")
        name = app.get("name")

        if not app_id or not name:
            continue

        try:
            # Keep the list clean
            if not is_valid_game(app_id):
                continue

            players = get_players(app_id)

            print(f"{name} -> {players}")

            # Master table: one row per game
            supabase.table("steam_games").upsert({
                "app_id": app_id,
                "name": name,
                "last_updated": utc_now()
            }).execute()

            # History table: one row per run
            supabase.table("steam_player_history").insert({
                "app_id": app_id,
                "name": name,
                "current_players": players,
                "recorded_at": utc_now()
            }).execute()

            processed += 1
            added_or_updated += 1

        except Exception as e:
            print("Error processing", app_id, e)

    # Move forward for the next GitHub Actions run
    if next_index >= len(apps):
        next_index = 0

    save_state(next_index)

    print("DONE:", processed)
    print("Next index:", next_index)
    print("Updated records:", added_or_updated)


if __name__ == "__main__":
    main()
