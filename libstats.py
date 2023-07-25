import sys
import time
import libdb_mysql


db = libdb_mysql.LibDbMysql()


class LibStats:
	""" Class for manage predictions """

	# -- constructor --
	def __init__(self):
		self.debug_mode = 1

		return

	# -- convert date into seconds --
	def get_secs(self, datehour):
		tmp = datehour.split(" ")
		date = tmp[0]
		hour = tmp[1]
		if len(hour) < 8:
			hour = "0"+hour

		tmpsecs = time.strptime(date+" "+hour, "%Y-%m-%d %H:%M:%S")
		secs = time.mktime(tmpsecs)

		return secs

	# -- get daily seconds --
	def get_day_secs(self, datehour):
		tmp = datehour.split(" ")
		date = tmp[0]
		hourini = "00:00:00"
		hour = tmp[1]
		if len(hour) < 8:
			hour = "0"+hour

		tmpsecs = time.strptime(date+" "+hour, "%Y-%m-%d %H:%M:%S")
		normalsecs = time.mktime(tmpsecs)
		tmpsecs = time.strptime(date+" "+hourini, "%Y-%m-%d %H:%M:%S")
		inisecs = time.mktime(tmpsecs)
		daysecs = normalsecs - inisecs

		return daysecs

	# -- get weekly seconds --
	def get_week_secs(self, datehour):
		tmp = datehour.split(" ")
		date = tmp[0]
		hourini = "00:00:00"
		hour = tmp[1]
		if len(hour) < 8:
			hour = "0"+hour

		tmpsecs = time.strptime(date+" "+hour, "%Y-%m-%d %H:%M:%S")
		curr_secs = int(time.mktime(tmpsecs))

		weekday_num = int(time.strftime("%w", time.localtime(curr_secs)))

		tmp_ini_week_secs = curr_secs - (86400 * (weekday_num - 1))

		ini_week_date = str(time.strftime("%Y-%m-%d", time.localtime(tmp_ini_week_secs)))
		tmpsecs = time.strptime(ini_week_date+" 00:00:00", "%Y-%m-%d %H:%M:%S")
		ini_secs = time.mktime(tmpsecs)

		weeksecs = curr_secs - ini_secs

		return weeksecs

	# -- get all week times --
	def get_week_times(self, curr_secs):
		week = {}

		week['weeknum'] = time.strftime("%U", time.localtime(curr_secs))
		week['weekday'] = int(time.strftime("%w", time.localtime(curr_secs)))

		tmp_ini_week_secs = curr_secs - (86400 * (week['weekday'] - 1))
		tmp_end_week_secs = curr_secs + (86400 * (5 - week['weekday']))

		week['ini_week_date'] = time.strftime("%Y-%m-%d", time.localtime(tmp_ini_week_secs))
		tmpsecs = time.strptime(week['ini_week_date']+" 00:00:00", "%Y-%m-%d %H:%M:%S")
		week['ini_week_secs'] = time.mktime(tmpsecs)

		week['end_week_date'] = time.strftime("%Y-%m-%d", time.localtime(tmp_end_week_secs))
		tmpsecs = time.strptime(week['end_week_date']+" 23:59:59", "%Y-%m-%d %H:%M:%S")
		week['end_week_secs'] = time.mktime(tmpsecs)

		return week

	# -- price multiplier --
	def get_multiplier(self, ticker, price, head):
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


	# -- put price in original format --
	def get_original(self, ticker, ticker1, price, head):
		finalprice = price
		divisor = 1

		if ticker1 == "^FXEURO":
			divisor = 10000

		if head == 1:
			finalprice = price / float(divisor)
		if head == -1:
			finalprice = price * divisor

		return finalprice


	#### MAIN FUNCTIONS ####

	# -- make a prediction for certain time --
	def make_prediction(self, data):
		daysecs = self.get_day_secs(str(data['pdate'])+" "+str(data['ptime']))

		params = [
			"ticker='"+data['ticker']+"'",
			"daysecs="+str(daysecs),
			"pdate<'"+str(data['pdate'])+"'",
			"trend="+str(data['trend']),
			#"pct="+str(data['pct'])
		]
		data = db.select(params, "prices")

		return data



