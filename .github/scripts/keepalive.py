"""Keep the shop-facing app awake and prove it is up.

Streamlit Community Cloud parks an app after an idle stretch; the next visitor
then sees a generic "Zzzz ... get this app back up" screen -- inside FAVI's
onboarding iframe. This script opens the app in a real (headless) browser
session like a viewer would, clicks the wake button if the app is asleep, and
exits non-zero if the app does not come up within five minutes. Run on a
schedule it prevents sleeping; on failure GitHub notifies the repo owner, which
makes it a free uptime check for the class of outage the in-app Slack reporter
can never see (dead container, platform outage).
"""
import os
import sys
import time

from playwright.sync_api import sync_playwright

URL = os.environ.get("APP_URL", "https://favi-shops-feedchecker.streamlit.app/")
READY = 'input[aria-label="Feed URL"]'
WAKE_BUTTON = "Yes, get this app back up!"
TIMEOUT_S = 300


def app_ready(page) -> bool:
    for frame in page.frames:  # cloud shell wraps the app in a nested frame
        try:
            if frame.locator(READY).count():
                return True
        except Exception:
            pass
    return False


def try_wake(page) -> bool:
    for frame in page.frames:
        try:
            button = frame.get_by_role("button", name=WAKE_BUTTON)
            if button.count():
                button.first.click()
                return True
        except Exception:
            pass
    return False


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        started = time.time()
        page.goto(URL, timeout=90_000)
        woke = False
        while time.time() - started < TIMEOUT_S:
            if app_ready(page):
                state = "was asleep, woken" if woke else "already awake"
                print(f"OK: {URL} is up ({state}) after {time.time() - started:.0f}s")
                browser.close()
                return 0
            if not woke and try_wake(page):
                woke = True
                print("app was asleep -- wake requested")
            time.sleep(3)
        print(f"ERROR: {URL} did not come up within {TIMEOUT_S}s")
        browser.close()
        return 1


if __name__ == "__main__":
    sys.exit(main())
