import time
import libdb_mysql
import libstats
import utils


db = libdb_mysql.LibDbMysql()
sts = libstats.LibStats()
utl = utils.Utils(db)


class LevelStat:
    """ Class for statistical level sorting """
    def __init__(self):
        self.maxlevels = None
        self.minlevels = None

        return

    # -- get levels from matches --
    def get_levels(self, argums: dict, matches: list, data: dict, last_price: float) -> list:
        # db.debug_mode = 2
        maxlevel = last_price
        minlevel = last_price

        # -- get real ticker --
        ticker, ticker1 = utl.get_real_ticker(argums['ticker'])
        close = sts.get_multiplier(ticker, data['close'], 1)

        high = {'level': 0.00, 'num': 0, 'highlow': 0}
        low = {'level': 0.00, 'num': 0, 'highlow': 0}

        # -- loop for result matches --
        for item in matches:
            params = [
                "SELECT MAX(trend), MIN(trend)",
                "ticker='" + item['ticker'] + "'",
                "fecha='" + str(item['fecha']) + "'",
                "daysecs>=" + str(item['daysecs'])
            ]
            tmp = db.select(params, "prices", "statsrt")

            maxday = tmp[0]['MAX(trend)']
            minday = tmp[0]['MIN(trend)']

            if maxday is not None:
                diffmax = maxday - item['trend']
                maxlevel = close + diffmax
            if minday is not None:
                diffmin = item['trend'] - minday
                minlevel = close - diffmin

            if maxday is None:
                maxlevel = 0
            if minday is None:
                minlevel = 0

            # line = "---> NEW MAX.LEVEL: "+str(sts.get_multiplier(data['ticker'], maxlevel, -1))+" - NEW MIN.LEVEL: "
            # +str(sts.get_multiplier(data['ticker'], minlevel, -1))
            # logging.info(line)
            # print(line)

            # -- update DB levels --
            self.update_levels(argums, maxlevel, 1, data['secs'])
            self.update_levels(argums, minlevel, -1, data['secs'])

            time.sleep(0.0001)

        db.debug_mode = 1
        # -- sort levels --
        params = [
            "ticker='" + argums['ticker'] + "'",
            "fecha='" + argums['date'] + "'",
            "level>" + str(close),
            "burned=0",
            "ORDER BY num ASC, secs ASC"
        ]
        self.maxlevels = db.select(params, "levels", "statsrt")
        if len(self.maxlevels) > 0:
            high = self.maxlevels[-1]
            high['level'] = sts.get_multiplier(data['ticker'], self.maxlevels[-1]['level'], -1)

        params = [
            "ticker='" + argums['ticker'] + "'",
            "fecha='" + argums['date'] + "'",
            "level<" + str(close),
            "burned=0",
            "ORDER BY num ASC, secs ASC"
        ]
        self.minlevels = db.select(params, "levels", "statsrt")
        if len(self.minlevels) > 0:
            low = self.minlevels[-1]
            low['level'] = sts.get_multiplier(data['ticker'], self.minlevels[-1]['level'], -1)

        return [high, low]

    # -- function to update levels --
    def update_levels(self, argums: dict, level: float, maxmin: float, secs: int) -> None:
        # -- add or update levels --
        params = [
            "ticker='" + argums['ticker'] + "'",
            "fecha='" + argums['date'] + "'",
            "level=" + str(level)
        ]
        tmp = db.select(params, "levels", "statsrt")
        if len(tmp) == 0:
            new_level = {
                'ticker': argums['ticker'],
                'fecha': argums['date'],
                'highlow': maxmin,
                'level': str(level),
                'num': 1,
                'secs': secs,
                'burned': 0
            }
            db.insert(new_level, "levels", "statsrt")
        else:
            tmp[0]['num'] += 1
            params1 = {
                'num': tmp[0]['num'],
                'secs': secs
            }
            params2 = ["id=" + str(tmp[0]['id'])]
            db.update(params1, params2, "levels", "statsrt")

        return

    # -- function for update burnt levels --
    def update_burn_levels(self, argums: dict, data: dict, last_price: float) -> None:
        # -- get levels --
        params = [
            "ticker='" + argums['ticker'] + "'",
            "fecha='" + argums['date'] + "'",
            "burned=0"
        ]
        levels = db.select(params, "levels", "statsrt")

        # -- check levels --
        for i in levels:
            level_burnt = 0
            level = sts.get_multiplier(data['ticker'], i['level'], -1)

            # -- price touch levels --
            if last_price <= data['close']:
                if last_price <= level <= data['close']:
                    level_burnt = 1
            if last_price > data['close']:
                if last_price >= level >= data['close']:
                    level_burnt = 1

            # -- burn levels alive with more than 1 hour ago --
            hour_ago = data['secs'] - 3600
            if i['secs'] < hour_ago:
                level_burnt = 1

            if level_burnt == 1:
                params1 = {
                    'burned': 1,
                    'num': 0
                }
                params2 = ["id=" + str(i['id'])]
                del i['id']
                db.update(params1, params2, "levels", "statsrt")

        return

    # -- get pairs --
    def get_pairs(self, argums: dict, high: list, low: list, data: dict, last_price: float) -> list:
        max_price = {'all': 0.00, 'allnum': 0, 'touched': 0.00, 'maxnum': 0.00, 'num': 0}
        min_price = {'all': 0.00, 'allnum': 0, 'touched': 0.00, 'maxnum': 0.00, 'num': 0}

        # -- check past pairs --
        params = [
            "ticker='" + argums['ticker'] + "'",
            "fecha='" + argums['date'] + "'"
        ]
        pairs = db.select(params, "pairs", "statsrt")

        for i in pairs:
            touched = 0
            curr_max_price = sts.get_multiplier(argums['ticker'], i['max_price'], -1)
            curr_min_price = sts.get_multiplier(argums['ticker'], i['min_price'], -1)

            if data['close'] >= last_price:
                if last_price <= curr_max_price <= data['close']:
                    touched = -1
                if last_price <= curr_min_price <= data['close']:
                    touched = -1
            if data['close'] < last_price:
                if data['close'] <= curr_max_price <= last_price:
                    touched = 1
                if data['close'] <= curr_min_price <= last_price:
                    touched = 1

            if touched == -1 or touched == 1:
                if i['first_mm'] == 0:
                    params1 = {
                        'first_mm': touched,
                        'op_secs': data['secs'],
                    }
                    params2 = ["id=" + str(i['id'])]
                    db.update(params1, params2, "pairs", "statsrt")
                if i['first_mm'] == -1 and touched == 1:
                    params1 = {
                        'op_finish': 1,
                        'op_secs': data['secs'] - i['op_secs']
                    }
                    params2 = ["id=" + str(i['id'])]
                    db.update(params1, params2, "pairs", "statsrt")

                if i['first_mm'] == 1 and touched == -1:
                    params1 = {
                        'op_finish': 1,
                        'op_secs': data['secs'] - i['op_secs']
                    }
                    params2 = ["id=" + str(i['id'])]
                    db.update(params1, params2, "pairs", "statsrt")

        # -- add new pairs --
        high_level = sts.get_multiplier(argums['ticker'], high['level'], 1)
        low_level = sts.get_multiplier(argums['ticker'], low['level'], 1)

        if high_level != 0 and low_level != 0 and high['num'] > 1 and low['num'] > 1:
            params = [
                "ticker='" + argums['ticker'] + "'",
                "fecha='" + argums['date'] + "'",
                "max_price=" + str(high_level),
                "min_price=" + str(low_level)
            ]
            tmp = db.select(params, "pairs", "statsrt")
            if len(tmp) == 0:
                secs_now = int(time.time())
                hora = time.strftime("%H:%M:%S", time.localtime(secs_now))

                newpair = {
                    'ticker': argums['ticker'],
                    'fecha': argums['date'],
                    'hora': hora,
                    'secs': secs_now,
                    'max_price': high_level,
                    'max_num': high['num'],
                    'min_price': low_level,
                    'min_num': low['num'],
                    'duration': 1,
                    'first_mm': 0,
                    'op_finish': 0,
                    'op_secs': 0,
                    'points': high['level'] - low['level']
                }
                db.insert(newpair, "pairs", "statsrt")
            else:
                # -- add duration --
                params1 = {
                    'duration': tmp[0]['duration'] + 1,
                    'max_num': high['num'],
                    'min_num': low['num']
                }
                params2 = ["id=" + str(tmp[0]['id'])]
                db.update(params1, params2, "pairs", "statsrt")

        # -- get most extreme max. pairs levels untouched (all) --
        params = [
            "ticker='" + argums['ticker'] + "'",
            "fecha='" + argums['date'] + "'",
            "duration>1",
            "op_finish=0"
        ]
        tmp = db.select(params, "pairs", "statsrt")
        if len(tmp) > 0:
            max_price['all'] = tmp[0]['max_price']
            max_price['allnum'] = tmp[0]['max_num']

            for item in tmp:
                if item['max_price'] > max_price['all']:
                    max_price['all'] = item['max_price']
                    max_price['allnum'] = item['max_num']

            max_price['all'] = sts.get_multiplier(argums['ticker'], max_price['all'], -1)

        # -- get most extreme min. pairs levels untouched (all) --
        params = [
            "SELECT MIN(min_price)",
            "ticker='" + argums['ticker'] + "'",
            "fecha='" + argums['date'] + "'",
            "duration>1",
            "op_finish=0"
        ]
        tmp = db.select(params, "pairs", "statsrt")
        if len(tmp) > 0:
            if tmp[0]['MIN(min_price)'] is not None:
                min_price['all'] = sts.get_multiplier(argums['ticker'], tmp[0]['MIN(min_price)'], -1)

        # -- get most extreme max. pairs levels touched (touched) --
        params = [
            "SELECT MAX(max_price)",
            "ticker='" + argums['ticker'] + "'",
            "fecha='" + argums['date'] + "'",
            "duration>1",
            "op_finish=0",
            "(first_mm=1 OR first_mm=-1)"
        ]
        tmp = db.select(params, "pairs", "statsrt")

        if len(tmp) > 0:
            if tmp[0]['MAX(max_price)'] is not None:
                max_price['touched'] = sts.get_multiplier(argums['ticker'], tmp[0]['MAX(max_price)'], -1)

        # -- get most extreme min. pairs levels touched (touched) --
        params = [
            "ticker='" + argums['ticker'] + "'",
            "fecha='" + argums['date'] + "'",
            "duration>1",
            "op_finish=0",
            "(first_mm=1 OR first_mm=-1)"
        ]
        tmp = db.select(params, "pairs", "statsrt")
        if len(tmp) > 0:
            min_price['all'] = tmp[0]['min_price']
            min_price['allnum'] = tmp[0]['min_num']

            for item in tmp:
                if item['min_price'] < min_price['all']:
                    min_price['all'] = item['min_price']
                    min_price['allnum'] = item['min_num']

            min_price['all'] = sts.get_multiplier(argums['ticker'], min_price['all'], -1)

        # -- get most extreme max. pairs levels by max num (maxnum) --
        params = [
            "ticker='" + argums['ticker'] + "'",
            "fecha='" + argums['date'] + "'",
            "duration>1",
            "op_finish=0"
        ]
        pairs = db.select(params, "pairs", "statsrt")

        if pairs is None:
            return [max_price, min_price]

        if len(pairs) == 0:
            return [max_price, min_price]

        sum_num = 0
        maxprice = 0.00
        maxnum = 0
        minprice = 0.00
        minnum = 0
        for item in pairs:
            sumpairs = item['max_num'] + item['min_num']
            if sumpairs > sum_num:
                sum_num = sumpairs
                maxprice = item['max_price']
                maxnum = item['max_num']
                minprice = item['min_price']
                minnum = item['min_num']

        max_price['maxnum'] = sts.get_multiplier(argums['ticker'], maxprice, -1)
        max_price['num'] = maxnum
        min_price['maxnum'] = sts.get_multiplier(argums['ticker'], minprice, -1)
        min_price['num'] = minnum

        return [max_price, min_price]
