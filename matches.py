#!/usr/bin/env python3

import logging
import libdb_mysql
import libstats
import utils


db = libdb_mysql.LibDbMysql()
sts = libstats.LibStats()
ut = utils.Utils(db)


class Matches:
    """ Class to include matching processing into Druida """
    def __init__(self):
        self.last_day = {
            'last_close': 0.00,
            'last_maximum': 0.00,
            'last_minimum': 0.00,
            'last_open': 0.00
        }
        self.last_close = 0.00
        self.sum = 0
        self.sub = 0
        self.curr_high = 0.00
        self.curr_low = 0.00

        return

    # -- generic matching function --
    def get_matches(self, argums: dict, data: dict, field: str, last_value: float, table: str) -> list:
        matchestable = []
        matchespct = []
        fecha = str(data['fecha'])
        hora = str(data['hora'])

        if field == "":
            field = "close"

        # print("DATA:")
        # print(data)

        # -- get real ticker --
        [ticker, ticker1] = ut.get_real_ticker(argums['ticker'])

        # -- get processed values --
        dbclose = sts.get_original(ticker, ticker1, data[field], 1)
        close = sts.get_multiplier(ticker, dbclose, 1)
        dblast_value = sts.get_original(ticker, ticker1, last_value, 1)
        last_value = sts.get_multiplier(ticker, dblast_value, 1)

        # -- get levels --
        trend = close - last_value
        # print("TABLE: ")
        # print(table)
        # print("CLOSE /LAST_VALUE:")
        # print(str(close)+" / "+str(last_value))

        if close != last_value:
            tmppcttrend = (((close - last_value) * 100.00) / last_value) * 100
            pcttrend = int(tmppcttrend)

            daysecs = sts.get_day_secs(fecha+" "+hora)

            params = [
                "ticker='"+argums['ticker']+"'",
                "daysecs="+str(daysecs),
                "fecha<'"+argums['date']+"'",
                "trend="+str(trend)
            ]
            matches = db.select(params, table, "statsrt")

            params = [
                "ticker='"+argums['ticker']+"'",
                "daysecs="+str(daysecs),
                "fecha<'"+argums['date']+"'",
                "trend="+str(trend)
            ]
            matchestable = db.select(params, table, "statsrt")

            if len(matchestable) > 0:
                line = "---> "+table.upper()+" TREND MATCHES: "+str(len(matchestable))
                logging.info(line)
                print(line)

            params = [
                "ticker='"+ticker+"'",
                "daysecs="+str(daysecs),
                "fecha<'"+argums['date']+"'",
                "pcttrend="+str(pcttrend),
            ]
            matchespct = db.select(params, table, "statsrt")

        # -- make a unified list with all matches --
        if len(matchespct) > 0:
            line = "---> "+table.upper()+" PCTTREND MATCHES: "+str(len(matchespct))
            logging.info(line)
            print(line)

        # -- group matches --
        matches = matchestable
        matches.extend(matchespct)

        return matches

    # -- get PRICE matches on historical data for current tick --
    def get_price_matches(self, argums: dict, data: dict, last_close: float) -> list:
        price_matches = self.get_matches(argums, data, "close", last_close, "prices")

        return price_matches

    # -- get MAXIMUM matches on historical for one tick --
    def get_maximum_matches(self, argums: dict, data: dict, last_high: float) -> list:
        high_matches = self.get_matches(argums, data, "high", last_high, "pricesmax")

        return high_matches

    # -- get MINIMUM matches on historical for one tick --
    def get_minimum_matches(self, argums: dict, data: dict, last_low: float) -> list:
        low_matches = self.get_matches(argums, data, "low", last_low, "pricesmin")

        return low_matches

    # -- get OPEN matches on historical for one tick --
    def get_open_matches(self, argums: dict, data: dict, last_open: float) -> list:
        open_matches = self.get_matches(argums, data, "open", last_open, "pricesopen")

        return open_matches

    # -- get UP matches on historical for one tick --
    def get_up_matches(self, argums: dict, data: dict, last_close: float) -> list:
        if data['close'] > last_close:
            self.sum += 1

        up_matches = self.get_matches(argums, data, "close", last_close, "pricesup")

        return up_matches

    # -- get DOWN matches on historical for one tick --
    def get_dn_matches(self, argums: dict, data: dict, last_close: float) -> list:
        # print("--> DN LAST_CLOSE:")
        # print(last_close)
        if data['close'] < last_close:
            self.sub += 1

        dn_matches = self.get_matches(argums, data, "close", last_close, "pricesdown")

        return dn_matches

    # -- get CURRENT HIGH on historical for one tick --
    def get_curr_high_matches(self, argums: dict, data: dict) -> list:
        if self.curr_high != data['daymax']:
            self.curr_high = data['daymax']

        curr_high_matches = self.get_matches(argums, data, "daymax", self.curr_high, "pricescurrhigh")

        return curr_high_matches

    # -- get CURRENT LOW on historical for one tick --
    def get_curr_low_matches(self, argums: dict, data: dict) -> list:
        if self.curr_low != data['daymin']:
            self.curr_low = data['daymin']

        curr_low_matches = self.get_matches(argums, data, "daymin", self.curr_low, "pricescurrlow")

        return curr_low_matches

    # def get_maximum_matches(self, argums: dict, data: dict, last_maximum: float, last_price: float) -> list:
    #     matches = self.get_price_matches(argums, data, "high", last_maximum, "pricesmax")

    #    return matches

    # -- get PRICE matches on historical for one tick --
    # def get_price_matches(self, argums: dict, data: dict, last_close: float, last_price: float) -> list:
        # matches = []
        # fecha = str(data['fecha'])
        # hora = str(data['hora'])

        # -- get real ticker --
        # [ticker, ticker1] = get_real_ticker(argums['ticker'])

        # -- get processed values --
        # dbclose = sts.get_original(ticker, ticker1, data['close'], 1)
        # close = sts.get_multiplier(ticker, dbclose, 1)

        # -- get levels --
        # trend = close - last_close
        # # pct = (((close - last_price) * 100.00) / last_close) * 100
        # tmppcttrend = (((close - last_close) * 100.00) / last_close) * 100
        # pcttrend = int(tmppcttrend)

        # daysecs = sts.get_day_secs(fecha+" "+hora)

        # params = [
        #     "ticker='"+argums['ticker']+"'",
        #     "daysecs="+str(daysecs),
        #     "fecha<'"+argums['date']+"'",
        #     "trend="+str(trend)
        # ]
        # matchesprice = db.select(params, "prices", "statsrt")
        # if len(matchesprice) > 0:
        #    line = "---> TREND MATCHES: "+str(len(matchesprice))
        #     logging.info(line)
        #     print(line)

        # params = [
        #     "ticker='"+ticker+"'",
        #     "daysecs="+str(daysecs),
        #     "fecha<'"+argums['date']+"'",
        #     "pcttrend="+str(pcttrend),
        # ]
        # matchespct = db.select(params, "prices", "statsrt")
        # if len(matchespct) > 0:
        #     line = "---> PCTTREND MATCHES: "+str(len(matchespct))
        #     logging.info(line)
        #     print(line)

        # -- group matches --
        # for row in matchesprice:
        #     matches.append(row)
        # for row in matchespct:
        #     matches.append(row)

        # return matches

    # -- get MAXIMUM matches on historical for one tick --
    # def get_maximum_matches(self, argums: dict, data: dict, last_maximum: float, last_price: float) -> list:
    #     matches = []
    #     fecha = str(data['fecha'])
    #     hora = str(data['hora'])

    #     # -- get real ticker --
    #     [ticker, ticker1] = get_real_ticker(argums['ticker'])

    #     # -- get levels --
    #     dbclose = sts.get_original(ticker, ticker1, data['close'], 1)
    #     close = sts.get_multiplier(ticker, dbclose, 1)

    #     trend = close - last_maximum
    #     # pct = (((close - last_price) * 100.00) / last_maximum) * 100
    #     tmppcttrend = (((close - last_maximum) * 100.00) / last_maximum) * 100
    #     pcttrend = int(tmppcttrend)

    #     daysecs = sts.get_day_secs(fecha+" "+hora)

    #     params = [
    #         "ticker='"+argums['ticker']+"'",
    #         "daysecs="+str(daysecs),
    #         "fecha<'"+argums['date']+"'",
    #         "trend="+str(trend)
    #     ]
    #     matchesmaximum = db.select(params, "pricesmax", "statsrt")
    #     if len(matchesmaximum) > 0:
    #         line = "---> MAXIMUM TREND MATCHES: "+str(len(matchesmaximum))
    #         logging.info(line)
    #         print(line)

    #     params = [
    #         "ticker='"+ticker+"'",
    #         "daysecs="+str(daysecs),
    #         "fecha<'"+argums['date']+"'",
    #         "pcttrend="+str(pcttrend),
    #     ]
    #     matchesmax = db.select(params, "pricesmax", "statsrt")
    #     if len(matchesmax) > 0:
    #         line = "---> MAXIMUM PCTTREND MATCHES: "+str(len(matchesmax))
    #         logging.info(line)
    #         print(line)

    #     # -- group matches --
    #     for row in matchesmaximum:
    #         matches.append(row)
    #     for row in matchesmax:
    #         matches.append(row)

    #     print("PRICEMAX matches: "+str(matches))

    #     return matches

    # -- get MINIMUM matches on historical for one tick --
    # def get_minimum_matches(self, argums: dict, data: dict, last_minimum: float, last_price: float) -> list:
    #     matches = []
    #     fecha = str(data['fecha'])
    #     hora = str(data['hora'])

    #     # -- get real ticker --
    #     ticker, ticker1 = get_real_ticker(argums['ticker'])

    #     # -- get levels --
    #     dbclose = sts.get_original(ticker, ticker1, data['close'], 1)
    #    close = sts.get_multiplier(ticker, dbclose, 1)

    #     trend = close - last_minimum
    #     # pct = (((close - last_price) * 100.00) / last_minimum) * 100
    #     tmppcttrend = (((close - last_minimum) * 100.00) / last_minimum) * 100
    #     pcttrend = int(tmppcttrend)

    #    daysecs = sts.get_day_secs(fecha+" "+hora)

    #    params = [
    #        "ticker='"+argums['ticker']+"'",
    #        "daysecs="+str(daysecs),
    #        "fecha<'"+argums['date']+"'",
    #        "trend="+str(trend)
    #    ]
    #    matchesminimum = db.select(params, "pricesmin", "statsrt")
    #    if len(matchesminimum) > 0:
    #        line = "---> MINIMUM TREND MATCHES: "+str(len(matchesminimum))
    #        logging.info(line)
    #        print(line)

    #    params = [
    #        "ticker='"+ticker+"'",
    #        "daysecs="+str(daysecs),
    #        "fecha<'"+argums['date']+"'",
    #        "pcttrend="+str(pcttrend),
    #    ]
    #    matchesminpct = db.select(params, "pricesmin", "statsrt")
    #    if len(matchesminpct) > 0:
    #        line = "---> MINIMUM PCTTREND MATCHES: "+str(len(matchesminpct))
    #        logging.info(line)
    #        print(line)

    #    # -- group matches --
    #    for row in matchesminimum:
    #        matches.append(row)
    #    for row in matchesminpct:
    #        matches.append(row)

    #    return matches

    # -- get OPEN matches on historical for one tick --
    # def get_open_matches(self, argums: dict, data: dict, last_open: float, last_price: float) -> list:
    #     matches = []
    #     fecha = str(data['fecha'])
    #     hora = str(data['hora'])

        # -- get real ticker --
    #     ticker, ticker1 = get_real_ticker(argums['ticker'])

        # -- get levels --
    #     dbclose = sts.get_original(ticker, ticker1, data['close'], 1)
    #     close = sts.get_multiplier(ticker, dbclose, 1)

    #     trend = close - last_open
    #    pct = (((close - last_price) * 100.00) / last_open) * 100
    #    tmppcttrend = (((close - last_open) * 100.00) / last_open) * 100
    #     pcttrend = int(tmppcttrend)

    #     daysecs = sts.get_day_secs(fecha+" "+hora)

    #    params = [
    #         "ticker='"+argums['ticker']+"'",
    #         "daysecs="+str(daysecs),
    #         "fecha<'"+argums['date']+"'",
    #         "trend="+str(trend)
    #     ]
    #     matchesopen = db.select(params, "pricesopen", "statsrt")
    #     if len(matchesopen) > 0:
    #         line = "---> OPEN TREND MATCHES: "+str(len(matchesopen))
    #         logging.info(line)
    #         print(line)

    #     params = [
    #         "ticker='"+ticker+"'",
    #         "daysecs="+str(daysecs),
    #         "fecha<'"+argums['date']+"'",
    #         "pcttrend="+str(pcttrend),
    #     ]
    #     matchesopenpct = db.select(params, "pricesopen", "statsrt")
    #     if len(matchesopenpct) > 0:
    #         line = "---> OPEN PCTTREND MATCHES: "+str(len(matchesopenpct))
    #         logging.info(line)
    #         print(line)

        # -- group matches --
    #     for row in matchesopen:
    #         matches.append(row)
    #     for row in matchesopenpct:
    #         matches.append(row)

    #     return matches

    # -- get UP matches on historical for one tick --
    # def get_up_matches(self, argums, data, last_open, last_price) -> list:
    #     matches = []
    #     fecha = str(data['fecha'])
    #     hora = str(data['hora'])

    #     # -- get real ticker --
    #    ticker, ticker1 = get_real_ticker(argums['ticker'])

    #     # -- get levels --
    #     dbclose = sts.get_original(ticker, ticker1, data['close'], 1)
    #     close = sts.get_multiplier(ticker, dbclose, 1)
    #    trend = close - last_open
    #     if trend > 0:
    #         self.sum += 1

    #     pct = (((close - last_price) * 100.00) / last_open) * 100
    #     tmppcttrend = (((close - last_open) * 100.00) / last_open) * 100
    #     pcttrend = int(tmppcttrend)

    #     daysecs = sts.get_day_secs(fecha+" "+hora)

    #     params = [
    #         "ticker='"+argums['ticker']+"'",
    #         "daysecs="+str(daysecs),
    #         "fecha<'"+argums['date']+"'",
    #         "trend="+str(self.sum)
    #     ]
    #     matchesup = db.select(params, "pricesup", "statsrt")
    #     if len(matchesup) > 0:
    #         line = "---> UP TREND MATCHES: "+str(len(matchesup))
    #         logging.info(line)
    #         print(line)

    #     params = [
    #         "ticker='"+ticker+"'",
    #         "daysecs="+str(daysecs),
    #         "fecha<'"+argums['date']+"'",
    #         "pcttrend="+str(pcttrend),
    #     ]
    #     matchesuppct = db.select(params, "pricesup", "statsrt")
    #     if len(matchesuppct) > 0:
    #         line = "---> UP PCTTREND MATCHES: "+str(len(matchesuppct))
    #         logging.info(line)
    #         print(line)

    #     # -- group matches --
    #     for row in matchesup:
    #         matches.append(row)
    #     for row in matchesuppct:
    #         matches.append(row)

    #     return matches

    # -- get DOWN matches on historical for one tick --
    # def get_down_matches(self, argums, data, last_open, last_price) -> list:
    #     matches = []
    #    fecha = str(data['fecha'])
    #    hora = str(data['hora'])

    #     # -- get real ticker --
    #     ticker, ticker1 = get_real_ticker(argums['ticker'])

    #     # -- get levels --
    #     dbclose = sts.get_original(ticker, ticker1, data['close'], 1)
    #     db_day_maximum = sts.get_original(ticker, ticker1, data['daymax'], 1)
    #     close = sts.get_multiplier(ticker, dbclose, 1)
    #     trend = dbclose - last_open
    #     if trend < 0:
    #         self.sub += 1

    #     pct = (((close - last_price) * 100.00) / last_open) * 100
    #     tmppcttrend = (((close - last_open) * 100.00) / last_open) * 100
    #     pcttrend = int(tmppcttrend)

    #     daysecs = sts.get_day_secs(fecha + " " + hora)

    #     params = [
    #         "ticker='" + argums['ticker'] + "'",
    #         "daysecs=" + str(daysecs),
    #         "fecha<'" + argums['date'] + "'",
    #         "trend=" + str(self.sub)
    #     ]
    #     matchesdown = db.select(params, "pricesdown", "statsrt")
    #     if len(matchesdown) > 0:
    #         line = "---> DOWN TREND MATCHES: " + str(len(matchesdown))
    #         logging.info(line)
    #         print(line)

    #     params = [
    #         "ticker='" + ticker + "'",
    #         "daysecs=" + str(daysecs),
    #         "fecha<'" + argums['date'] + "'",
    #         "pcttrend=" + str(pcttrend),
    #     ]
    #     matchesdownpct = db.select(params, "pricesdown", "statsrt")
    #     if len(matchesdownpct) > 0:
    #         line = "---> DOWN PCTTREND MATCHES: " + str(len(matchesdownpct))
    #         logging.info(line)
    #         print(line)

    #     # -- group matches --
    #     for row in matchesdown:
    #         matches.append(row)
    #     for row in matchesdownpct:
    #         matches.append(row)

    #     return matches

    # -- get CURRENT HIGH matches on historical for one tick --
    # def get_curr_high_matches(self, argums, data, last_price):
    #     matches = []
    #     fecha = str(data['fecha'])
    #     hora = str(data['hora'])

    #     # -- get real ticker --
    #     ticker, ticker1 = get_real_ticker(argums['ticker'])

    #     # -- get levels --
    #     dbclose = sts.get_original(ticker, ticker1, data['close'], 1)
    #     db_day_maximum = sts.get_original(ticker, ticker1, data['daymax'],  1)
    #     close = sts.get_multiplier(ticker, dbclose, 1)
    #     day_maximum = sts.get_multiplier(ticker, db_day_maximum, 1)
    #     trend = day_maximum - close
    #     if trend < 0:
    #         self.sub += 1

    #     # pct = (((day_maximum - close) * 100.00) / day_maximum) * 100
    #     tmppcttrend = (((day_maximum - close) * 100.00) / day_maximum) * 100
    #     pcttrend = int(tmppcttrend)

    #     daysecs = sts.get_day_secs(fecha + " " + hora)

    #     params = [
    #         "ticker='" + argums['ticker'] + "'",
    #         "daysecs=" + str(daysecs),
    #         "fecha<'" + argums['date'] + "'",
    #         "trend=" + str(self.sub)
    #     ]
    #     matchesdaymax = db.select(params, "pricesdaymax", "statsrt")
    #     if len(matchesdaymax) > 0:
    #         line = "---> DAY MAX. TREND MATCHES: " + str(len(matchesdaymax))
    #         logging.info(line)
    #         print(line)

    #     params = [
    #         "ticker='" + ticker + "'",
    #         "daysecs=" + str(daysecs),
    #         "fecha<'" + argums['date'] + "'",
    #         "pcttrend=" + str(pcttrend),
    #     ]
    #     matchesdaymaxpct = db.select(params, "pricesdaymax", "statsrt")
    #     if len(matchesdaymaxpct) > 0:
    #         line = "---> DAY MAX. PCTTREND MATCHES: " + str(len(matchesdaymaxpct))
    #         logging.info(line)
    #         print(line)

    #     # -- group matches --
    #     for item in matchesdaymax:
    #         matches.append(item)
    #     for item in matchesdaymaxpct:
    #         matches.append(item)

    #     return matches
