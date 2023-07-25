#!/usr/bin/env python3

import argparse
import time
import logging

import libdb_mysql
import matches
import libstats
import utils
import levelstat


mtch = matches.Matches()
db = libdb_mysql.LibDbMysql()
sts = libstats.LibStats()
ut = utils.Utils(db)
ls = levelstat.LevelStat()


class Druida:
    """ Druida base class """
    def __init__(self):
        self.last_day_values = {
            'open': 0.00,
            'high': 0.00,
            'low': 0.00,
            'close': 0.00
        }
        self.last_close = 0.00
        self.data = None

        return

    # -- process arguments --
    def arguments(self) -> dict:
        parser = argparse.ArgumentParser()
        parser.add_argument("-t", "--ticker", type=str, default='KR-XBTUSD', help="Ticker symbol")
        parser.add_argument("-d", "--date", type=str, default=time.strftime("%Y-%m-%d", time.localtime()), help="Date")
        parser.add_argument("-g", "--trading", action="count", default=1, help="Disable trading mode")
        parser.add_argument("-l", "--log_file", type=str, default="", help="Specify log file")
        parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase verbosity level")
        args = parser.parse_args()

        # -- Create a dictionary with the specified values --
        tokens = {
            "ticker": args.ticker,
            "date": args.date,
            "trading": args.trading,
            "log_file": args.log_file,
            "verbose": args.verbose
        }
        if tokens['log_file'] == "":
            tokens['log_file'] = "log-NEW-"+tokens['ticker']+"-"+tokens['date']+".log"
        if tokens['trading'] > 0:
            tokens['trading'] = 0

        if tokens['verbose'] >= 1:
            print("TOKENS:")
            print(tokens)

        return tokens


# ###########################################################################
# #  MAIN

drd = Druida()

closes = []
dates = []
maxlevels = []
minlevels = []
oldmaxmin = {}
allmatches = 0

# -- process arguments --
argums = drd.arguments()
print("ARGUMS: "+str(argums))
db.debug_mode = argums['verbose']

# -- start logging --
ut.start_log(argums['log_file'])

# -- get last day values --
drd.last_day_values = ut.get_init_values(argums)
# print("DRD.LAST_DAY_VALUES:")
# print(drd.last_day_values)

# -- get last close price --
day_data = ut.day_data(argums)
drd.last_close = day_data[0]['close']
# print("DRD.LAST_CLOSE:")
# print(drd.last_close)

# -- loop to get max/min of past data --
if len(day_data) > 0:
    for row in day_data:
        orig_ticker = argums['ticker']
        argums['ticker'] = ut.get_real_ticker(argums['ticker'])[0]

        matches = mtch.get_price_matches(argums, row, drd.last_day_values['close'])
        matches.extend(mtch.get_maximum_matches(argums, row, drd.last_day_values['high']))
        matches.extend(mtch.get_minimum_matches(argums, row, drd.last_day_values['low']))
        matches.extend(mtch.get_open_matches(argums, row, drd.last_day_values['open']))

        matches.extend(mtch.get_up_matches(argums, row, drd.last_close))
        matches.extend(mtch.get_dn_matches(argums, row, drd.last_close))
        matches.extend(mtch.get_curr_high_matches(argums, row))
        matches.extend(mtch.get_curr_low_matches(argums, row))

        allmatches += len(matches)

        argums['ticker'] = orig_ticker
        high, low = ls.get_levels(argums, matches, row, drd.last_close)
        ls.update_burn_levels(argums, row, drd.last_close)
        max_price, min_price = ls.get_pairs(argums, high, low, row, drd.last_close)

        # -- get record levels and show results --
        strlevelhigh = "--- "
        if high['level'] != 0.00:
            strlevelhigh = str(high['level'])

        strlevellow = " ---"
        if low['level'] != 0.00:
            strlevellow = str(low['level'])

        strlevelshl = strlevelhigh+"-"+strlevellow+" ("+str(high['num'])+"-"+str(low['num'])+")"

        strfecha = str(row['fecha'])
        if len(strfecha) == 7:
            strfecha = "0"+strfecha

        strmaxminprice = "Matchs: "+str(allmatches)+"  "
        strmaxminprice += "Ext: "+str(max_price['all'])+"-"+str(min_price['all'])+" ("+str(max_price['allnum'])+"-" \
                          + str(min_price['allnum'])+")  "
        strmaxminprice += "Tou: "+str(max_price['touched'])+"-"+str(min_price['touched'])+"  "
        strmaxminprice += "Max: "+str(max_price['maxnum'])+"-" \
                          + str(min_price['maxnum'])+" (" \
                          + str(max_price['num'])+"-" + str(min_price['num']) + ") (" \
                          + str(round(max_price['maxnum'] - min_price['maxnum']))+")  "
        strmaxminprice += "Cur: "+strlevelhigh+"-"+strlevellow+" (" \
                          + str(high['num'])+"-"+str(low['num'])+") (" \
                          + str(round(high['level'] - low['level'], 2))+")"

        line = argums['ticker']+" 1D "+strfecha+" "+str(row['hora'])+"  " + str(row['close'])+"  "+strmaxminprice
        logging.info(line)
        print(line)

        # -- if new values different from current max min levels then store it --
        current_maxmin = {
            'high': max_price['maxnum'],
            'numhigh': max_price['num'],
            'low': min_price['maxnum'],
            'numlow': min_price['num']
        }
        if current_maxmin != oldmaxmin:
            if current_maxmin['numhigh'] > 1 and current_maxmin['numlow'] > 1:
                histo = {
                    'ticker': argums['ticker'],
                    'fecha': row['fecha'],
                    'hora': row['hora'],
                    'secs': sts.get_secs(strfecha+" "+str(row['hora'])),
                    'high': max_price['maxnum'],
                    'numhigh': max_price['num'],
                    'low': min_price['maxnum'],
                    'numlow': min_price['num']
                }
                db.insert(histo, "histo_levels_1D_3S", "statsrt")

        # -- get old values for next loop --
        oldmaxmin = current_maxmin
        drd.last_close = row['close']

        time.sleep(0.0001)

# -- real time prices and graph update --
if argums['trading'] == 1:
    line = "-----------------------------------------------"
    line += "Past data processed, now entering real time...."
    line += "-----------------------------------------------"
    logging.info(line)
    print(line)

    # -- get last id --
    if len(drd.data) > 0:
        last_id = drd.data[-1]['id']
    else:
        database = db.get_database(argums['date'])
        table = "quotes1"
        if argums['date'] == time.strftime("%Y-%m-%d", time.localtime()):
            database = "bolsa"
            table = "quoteday"

        params = [
            "SELECT MAX(id)",
            "ticker='"+argums['ticker']+"'"
        ]
        tmp = db.select(params, table, database)
        last_id = tmp[0]['MAX(id)']

    # -- loop for trading --
    while True:
        # -- get current data --
        database = db.get_database(argums['date'])
        table = "quotes1"
        if argums['date'] == time.strftime("%Y-%m-%d", time.localtime()):
            database = "bolsa"
            table = "quoteday"

        params = [
            "ticker='"+argums['ticker']+"'",
            "id>"+str(last_id)
        ]
        last_data = db.select(params, table, database)

        # -- make analysis --
        if len(last_data) > 0:
            for row in last_data:
                orig_ticker = argums['ticker']
                argums['ticker'] = ut.get_real_ticker(argums['ticker'])[0]

                matches = mtch.get_price_matches(argums, row, drd.last_day_values['close'])
                matches.extend(mtch.get_maximum_matches(argums, row, drd.last_day_values['high']))
                matches.extend(mtch.get_minimum_matches(argums, row, drd.last_day_values['low']))
                matches.extend(mtch.get_open_matches(argums, row, drd.last_day_values['open']))
                matches.extend(mtch.get_up_matches(argums, row, drd.last_close))
                matches.extend(mtch.get_dn_matches(argums, row, drd.last_close))

                allmatches += len(matches)

                argums['ticker'] = orig_ticker
                high, low = ls.get_levels(argums, matches, row, drd.last_close)
                ls.update_burn_levels(argums, row, drd.last_close)
                max_price, min_price = ls.get_pairs(argums, high, low, row, drd.last_close)

                # -- show results --
                strlevelhigh = "--- "
                if high['level'] != 0.00:
                    strlevelhigh = str(high['level'])

                strlevellow = " ---"
                if low['level'] != 0.00:
                    strlevellow = str(low['level'])

                strfecha = str(row['fecha'])
                if len(strfecha) == 7:
                    strfecha = "0"+strfecha

                strmaxminprice = "Matchs: "+str(allmatches)+"  "
                strmaxminprice += "Ext: "+str(max_price['all'])+"-"+str(min_price['all'])+" (" \
                                  + str(max_price['allnum']) + "-"+str(min_price['allnum'])+")  "
                strmaxminprice += "Tou: "+str(max_price['touched'])+"-"+str(min_price['touched'])+"  "
                strmaxminprice += "Max: "+str(max_price['maxnum'])+"-"+str(min_price['maxnum'])+" (" \
                                  + str(max_price['num'])+"-"+str(min_price['num'])+")  (" \
                                  + str(round(max_price['maxnum'] - min_price['maxnum']))+")  "
                strmaxminprice += "Cur: "+strlevelhigh+"-"+strlevellow+" ("+str(high['num'])+"-"+str(low['num']) \
                                  + ") ("+str(round(high['level'] - low['level'], 2))+")"

                line = argums['ticker']+" 1D "+strfecha+" "+str(row['hora'])+"  "+str(row['close'])+"  "+strmaxminprice
                logging.info(line)
                print(line)

                # -- if new values different from current max min levels then store it --
                current_maxmin = {
                    'high': max_price['maxnum'],
                    'numhigh': max_price['num'],
                    'low': min_price['maxnum'],
                    'numlow': min_price['num']
                }
                if current_maxmin != oldmaxmin:
                    if current_maxmin['numhigh'] > 1 and current_maxmin['numlow'] > 1:
                        histo = {
                            'ticker': argums['ticker'],
                            'fecha': row['fecha'],
                            'hora': row['hora'],
                            'secs': sts.get_secs(strfecha+" "+str(row['hora'])),
                            'high': max_price['maxnum'],
                            'numhigh': max_price['num'],
                            'low': min_price['maxnum'],
                            'numlow': min_price['num']
                        }
                        db.insert(histo, "histo_levels_1D_3S", "statsrt")

                # -- get old values for next loop --
                oldmaxmin = current_maxmin
                drd.last_close = row['close']

                time.sleep(0.0001)
            last_id = last_data[-1]['id']

        time.sleep(1)
