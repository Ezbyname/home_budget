import requests
from packaging.version import Version

GITHUB_REPO = "ezbyname/home_budget"

def check_for_updates(current_version):
    """Returns dict with latest version and download_url, or None if already up to date."""
    try:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
        response = requests.get(url, timeout=5, headers={"User-Agent": "home-budget-app"})
        if response.status_code != 200:
            return None
        release = response.json()
        latest = release["tag_name"].lstrip("v")
        if Version(latest) > Version(current_version):
            assets = release.get("assets", [])
            download_url = assets[0]["browser_download_url"] if assets else release["html_url"]
            return {"version": latest, "download_url": download_url}
        return None
    except Exception:
        return None
