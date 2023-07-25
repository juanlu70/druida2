import os
import time
import logging
import libstats


sts = libstats.LibStats()


class Utils:
    """ Function library for common utilities on Druida """
    def __init__(self, db):
        self.last_day_values = {
            'open': 0.00,
            'high': 0.00,
            'low': 0.00,
            'close': 0.00
        }
        self.db = db

        return

    # -- get initial values from last day --
    def get_init_values(self, argums: dict) -> dict:
        todaysecs = sts.get_secs(argums['date'] + " 00:00:00")
        yesterday = time.strftime("%Y-%m-%d", time.localtime(todaysecs - 86400))

        params = ["fecha='" + yesterday + "'"]
        daily = self.db.select(params, argums['ticker'], "diario")

        self.last_day_values['open'] = daily[0]['open']
        self.last_day_values['high'] = daily[0]['high']
        self.last_day_values['low'] = daily[0]['low']
        self.last_day_values['close'] = daily[0]['close']

        return self.last_day_values

    # -- get day data --
    def day_data(self, argums: dict) -> list:
        database = self.db.get_database(argums['date'])
        today = time.strftime("%Y-%m-%d", time.localtime())
        if argums['date'] == today:
            table = "quoteday"
        else:
            table = "quotes1"

        params = [
            "ticker='" + argums['ticker'] + "'",
            "fecha='" + argums['date'] + "'",
            "ORDER BY secs"
        ]
        day_data = self.db.select(params, table, database)

        return day_data

    # -- get real ticker --
    def get_real_ticker(self, ticker: str) -> list:
        newticker = ticker
        alterticker = ticker

        if ticker == "^FXEURO":
            newticker = "EURUSD"
            alterticker = ticker
        if ticker == "^COMORO":
            newticker = "XAUUSD"
            alterticker = ticker

        return [newticker, alterticker]

    # -- price multiplier --
    def get_multiplier(self, ticker, price, head) -> int:
        finalprice = price
        multiplier = 1

        if ticker == "EURUSD":
            multiplier = 10000
        if ticker == "SP500" \
                or ticker == "^GSPCMF" \
                or ticker == "BS-BTCUSD" \
                or ticker == "KR-XBTUSD" \
                or ticker == "XAUUSD":
            multiplier = 100

        if head == 1:
            finalprice = int(price * multiplier)
        if head == -1:
            finalprice = float(price / float(multiplier))

        return finalprice

    # -- start logging --
    def start_log(self, log_file: str) -> None:
        if os.path.isfile(log_file):
            os.remove(log_file)

        logging.basicConfig(filename=log_file, level=logging.INFO)

        return
