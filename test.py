import logging
import os
import time
import json
from pocketoptionapi.stable_api import PocketOption

logging.basicConfig(level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S")


def process_order_result(result):
    logging.info(f"Order result: {json.dumps(result, indent=4)}")
    return result


def main():
    ssid = os.environ.get("PO_SSID")
    if not ssid:
        logging.error(
            "Missing PO_SSID. Export your Socket.IO auth string from the browser, e.g. "
            'export PO_SSID=\'42["auth",{"session":"...","isDemo":1,"uid":...}]\''
        )
        return

    api = PocketOption(ssid)
    ok, err = api.connect()

    if not ok:
        logging.error(f"Connection failed: {err}")
        return

    while not (api.check_connect() and api.is_time_synced()):
        time.sleep(0.1)

    logging.info(f"Connected. Balance: {api.get_balance()}")
    logging.info(f"Assets loaded: {len(api.get_assets())}")

    asset = "EURUSD_otc"
    payout = api.get_payout(asset)
    logging.info(f"Payout for {asset}: {payout}%")

    api.subscribe(asset, 60)
    time.sleep(2)

    sentiment = api.get_sentiment(asset)
    logging.info(f"Sentiment for {asset}: {sentiment}%")

    ticks = api.get_realtime_ticks(asset, limit=5)
    logging.info(f"Recent ticks: {ticks}")

    while True:
        try:
            server_time = api.get_server_datetime()
            print(server_time.strftime("%Y-%m-%d %H:%M:%S"))
            time.sleep(1)
        except KeyboardInterrupt:
            logging.info("User interrupted")
            break
        except Exception as e:
            logging.error(f"Error: {e}")
            time.sleep(1)


if __name__ == "__main__":
    main()
