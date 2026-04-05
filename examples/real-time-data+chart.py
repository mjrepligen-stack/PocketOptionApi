import logging
import pandas as pd
import time
from lightweight_charts import Chart
from pocketoptionapi.stable_api import PocketOption

logging.basicConfig(level=logging.INFO)
pd.set_option('display.float_format', lambda x: '%.5f' % x)


def ticks_to_ohlc(ticks, period=10):
    """Converts (timestamp, price) ticks into OHLC candle dicts."""
    if not ticks:
        return []
    candles = {}
    for ts, price in sorted(ticks):
        ct = int(ts // period) * period
        if ct not in candles:
            candles[ct] = {"date": pd.Timestamp(ct, unit="s"), "open": price, "high": price, "low": price, "close": price}
        else:
            c = candles[ct]
            c["high"] = max(c["high"], price)
            c["low"] = min(c["low"], price)
            c["close"] = price
    return sorted(candles.values(), key=lambda c: c["date"])


def main():
    try:
        chart = Chart(toolbox=True)
        chart.legend(visible=True, font_size=14)
        chart.precision(5)
        chart.candle_style(up_color='#32CD32', down_color='#FF0000')

        ssid = r"""42["auth",{"session":"YOUR_SESSION","isDemo":1,"uid":0,"platform":2}]"""
        api = PocketOption(ssid)
        api.connect()

        while not (api.check_connect() and api.is_time_synced()):
            continue

        asset = "AUDCAD_otc"
        period = 10

        api.subscribe(asset, period)

        chart_set = False

        while True:
            try:
                ticks = api.get_realtime_ticks(asset, limit=500)
                candles = ticks_to_ohlc(ticks, period)
                if candles:
                    df = pd.DataFrame(candles)
                    df = df[['date', 'open', 'high', 'low', 'close']]

                    if not chart_set:
                        chart.set(df)
                        chart.show(block=False)
                        chart_set = True
                    else:
                        chart.update(df.iloc[-1])
            except Exception as e:
                logging.error(f"Error: {e}")

            time.sleep(0.03)

    except Exception as e:
        logging.error(f"Error: {e}")
    except KeyboardInterrupt:
        logging.info("User interrupt")


if __name__ == "__main__":
    main()
