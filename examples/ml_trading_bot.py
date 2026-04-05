import logging
import time
from typing import Tuple

import pandas as pd
import numpy as np
from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

from pocketoptionapi.stable_api import PocketOption


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute basic features for the ML model."""
    df = df.copy()
    df["return"] = df["close"].pct_change()
    df["ma5"] = df["close"].rolling(window=5).mean()
    df["ma10"] = df["close"].rolling(window=10).mean()
    df["ma5_diff"] = df["close"] - df["ma5"]
    df["ma10_diff"] = df["close"] - df["ma10"]
    df["volatility"] = df["return"].rolling(window=5).std()
    df["target"] = (df["close"].shift(-1) > df["close"]).astype(int)
    df.dropna(inplace=True)
    return df


def train_model(df: pd.DataFrame) -> Tuple[SGDClassifier, float]:
    """Train a simple logistic regression model using SGD."""
    features = df[["return", "ma5_diff", "ma10_diff", "volatility"]]
    target = df["target"]

    X_train, X_test, y_train, y_test = train_test_split(
        features, target, test_size=0.3, random_state=42
    )

    model = SGDClassifier(loss="log_loss", max_iter=1000, tol=1e-3)
    model.fit(X_train, y_train)
    predictions = model.predict(X_test)
    acc = accuracy_score(y_test, predictions)
    return model, acc


def main():
    logging.basicConfig(level=logging.INFO)
    ssid = ""  # Replace with your SSID

    asset = "EURUSD_otc"
    period = 60
    candles_to_train = 1000

    api = PocketOption(ssid)
    api.connect()

    while not (api.check_connect() and api.is_time_synced()):
        time.sleep(0.5)

    api.subscribe(asset, period)
    time.sleep(1)

    pages = candles_to_train // 150 + 1
    raw = api.get_historical_candles(asset, period, offset=45000, count_request=pages)
    if not raw:
        logging.error("Could not fetch training data")
        return

    df = api.process_candles_data(raw, period)
    if df is None or df.empty:
        logging.error("Could not process candle data")
        return

    data = compute_features(df)
    model, acc = train_model(data)
    logging.info(f"Initial model accuracy: {acc:.2f}")

    amount = 1

    try:
        while True:
            server_time = api.get_server_datetime()
            if server_time.second == 0:
                raw_new = api.get_historical_candles(asset, period, offset=9000, count_request=1)
                if not raw_new:
                    time.sleep(1)
                    continue

                df_new = api.process_candles_data(raw_new, period)
                if df_new is None or df_new.empty:
                    time.sleep(1)
                    continue

                data = compute_features(df_new)
                if data.empty:
                    time.sleep(1)
                    continue

                latest = data.iloc[-1]
                X_pred = latest[["return", "ma5_diff", "ma10_diff", "volatility"]].values.reshape(1, -1)
                prob = model.predict_proba(X_pred)[0]
                direction = "call" if prob[1] > 0.5 else "put"

                if max(prob) > 0.55:
                    order_data, status = api.buy(amount, asset, direction, period)
                    if status:
                        api.check_win(order_data.get("id"), callback=lambda r: logging.info(f"Result: {r}"))

                model.partial_fit(
                    X_pred, np.array([latest["target"]]), classes=[0, 1]
                )

            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("User interrupt")


if __name__ == "__main__":
    main()
