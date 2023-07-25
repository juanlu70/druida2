import time
import pymysql


class LibDbMysql:
	""" Class for manage mysql database queries """

	# -- constructor --
	def __init__(self):
		self.debug_mode = 1
		self.transaction_on = 0
		self.transaction = ""

		return

	# -- open database --
	def open_db(self, database):
		user = ""
		password = ""

		bbdd = pymysql.connect(host="localhost", user=user, passwd=password, db=database)
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		# bbdd = MySQLdb.connect(host="localhost", user=user, password=password, db=database)
		# sql = bbdd.cursor

		return bbdd, sql

	# -- insert quotes data on db --
	def row_insert(self, data, table, database):
		# -- open database --
		bbdd, sql = self.open_db(database)

		# -- get and filter data --
		for item in data:
			query = "INSERT INTO `"+table+"` ("
			for n in item.keys():
				query += n+","
			query = query[0:-1]

			query += ") VALUES ("
			for name in item.keys():
				query += "'"+str(item[name])+"',"

			query = query[0:-1]
			query += ");"

			if self.debug_mode > 1:
				print("("+database+") "+query)

			# -- execute query --
			sql.execute(query)
		bbdd.commit()
		bbdd.close()

		return

	# -- make a generic insert with one row --
	def insert(self, data, table, database):
		bbdd, sql = self.open_db(database)

		query = "INSERT INTO `"+table+"` ("

		for item in data.keys():
			query += "`"+item+"`, "
		query = query[0:-2]

		query += ") VALUES ("

		for name in data.keys():
			query += "'"+str(data[name])+"', "
		query = query[0:-2]

		query += ");"
		if self.debug_mode > 1:
			print("("+database+") "+query)

		# -- execute query --
		sql.execute(query)
		bbdd.commit()

		bbdd.close()

		return

	# -- make generic select defined by data content --
	def select(self, data, table, database):
		tuples = []
		bbdd, sql = self.open_db(database)

		query = "SELECT * FROM `"+table+"` WHERE "

		for item in data:
			if item.upper().find("SELECT") == 0:
				query = item+" FROM `"+table+"` WHERE "

			if item.find("SELECT") < 0 and item.find("ORDER BY") < 0:
				query += item+" AND "

			if item.find("ORDER BY") == 0:
				if query[-5:] == " AND ":
					query = query[0:-5]
				if query[-7:] == " WHERE ":
					query = query[0:-7]
				query += " " + item

		if query[-5:] == " AND ":
			query = query[0:-5]
		if query[-7:] == " WHERE ":
			query = query[0:-7]

		query += ";"
		if self.debug_mode > 1:
			print("("+database+") "+query)

		# -- execute query --
		sql.execute(query)
		tuples = sql.fetchall()

		bbdd.close()

		if tuples is None:
			return []
		if len(tuples) == 0:
			return []

		return tuples

	# -- make a generic delete defined by data content --
	def delete(self, data, table, database):
		bbdd, sql = self.open_db(database)

		query = "DELETE FROM `"+table+"`"

		if len(data) > 0:
			query += " WHERE "

			for item in data:
				query += item+" AND "
			query = query[0:-5]

		query += ";"
		if self.debug_mode > 1:
			print("("+database+") "+query)

		# -- execute query --
		sql.execute(query)
		bbdd.commit()
		bbdd.close()

		return

	# -- make a generic update defined by two data content --
	def update(self, data1, data2, table, database):
		bbdd, sql = self.open_db(database)

		# -- compose query --
		query = "UPDATE `"+table+"`"

		if len(data1) > 0:
			query += " SET "

			for item1 in data1.keys():
				query += "`"+item1+"`='"+str(data1[item1])+"', "
			query = query[0:-2]

		if len(data2) > 0:
			query += " WHERE "

			for item2 in data2:
				query += item2+" AND "
			if query[-5:] == " AND ":
				query = query[0:-5]

		query += ";"
		if self.debug_mode > 1:
			print("("+database+") "+query)

		# -- execute query --
		sql.execute(query)
		bbdd.commit()
		bbdd.close()

		return

	# -- make a generic query --
	def query(self, params: list, database: str) -> list:
		tuples = []
		bbdd, sql = self.open_db(database)

		# -- compose query --
		for item in params:
			query = item

			if query[-1] != ";":
				query += query+";"

			if self.debug_mode > 1:
				print("("+database+") "+query)

			# -- execute query --
			sql.execute(query)
			tuples = sql.fetchall()
			bbdd.commit()

		bbdd.close()

		return tuples

	# -- get database --
	def get_database(self, data: int) -> str:
		database = "bolsa"
		now = int(time.time())

		if type(data) == str:
			secs = self.get_secs(data+" 00:00:00") + 604800
		else:
			secs = int(data)

		if secs >= 820450800 and secs < 978217200:
			database = "bolsa19962000"

		if secs >= 978303600 and secs < 1135983600:
			database = "bolsa20012005"

		if secs >= 1136070000 and secs < 1293836400:
			database = "bolsa20062010"

		if secs >= 1293836400 and secs < 1451602800:
			database = "bolsa20112015"

		if secs >= 1451602800 and secs < 1609455600:
			database = "bolsa20162020"

		if secs >= 1609455600 and secs <= now:
			database = "bolsa20212025"

		if self.debug_mode > 1:
			print("DATABASE selected:")
			print(database)

		return database

	# -- convert date into seconds --
	def get_secs(self, date_hour):
		tmp = date_hour.split(" ")
		date = tmp[0]
		hour = tmp[1]
		if len(hour) < 8:
			hour = "0"+hour

		tmpsecs = time.strptime(date+" "+hour, "%Y-%m-%d %H:%M:%S")
		secs = time.mktime(tmpsecs)

		return secs
