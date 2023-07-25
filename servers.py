#!/usr/bin/env python3
# vim: set fileencoding=utf-8 :
# -*- coding: utf-8 -*-


#import MySQLdb
import pymysql


# -- database process class --
class servers():
	"Database servers definition"

	# -- constructor --
	def __init__(self):
		self.debug_mode = 1

		return

	# -- abrir db bolsart --
	def db_bolsart(self):
		bbdd = pymysql.connect(host = "localhost", user = "juanlu", passwd = "", db = "bolsart")
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		curr_db = "bolsart"

		return [sql, bbdd, curr_db]

	# -- abrir db bolsa --
	def db_bolsa(self):
		bbdd = pymysql.connect(host = "localhost", user = "juanlu", passwd = "", db = "bolsa")
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		curr_db = "bolsa"

		return [sql, bbdd, curr_db]

	# -- abrir db bolsa 2010 --
	def db_bolsa2010(self):
		bbdd = pymysql.connect(host = "localhost", user = "juanlu", passwd = "", db = "bolsa2010")
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		curr_db = "bolsa2010"

		return [sql, bbdd, curr_db]

	# -- abrir db de bolsa19962000 --
	def db_bolsa19962000(self):
		bbdd = pymysql.connect(host = "localhost", user = "juanlu", passwd = "", db = "bolsa19962000")
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		curr_db = "bolsa19962000"

		return [sql, bbdd, curr_db]

	# -- abrir db de bolsa20002005 --
	def db_bolsa20012005(self):
		bbdd = pymysql.connect(host = "localhost", user = "juanlu", passwd = "", db = "bolsa20012005")
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		curr_db = "bolsa20012005"

		return [sql, bbdd, curr_db]

	# -- abrir db de bolsa20062010 --
	def db_bolsa20062010(self):
		bbdd = pymysql.connect(host = "localhost", user = "juanlu", passwd = "", db = "bolsa20062010")
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		curr_db = "bolsa20062010"

		return [sql, bbdd, curr_db]

	# -- abrir db de bolsa20112015 --
	def db_bolsa20112015(self):
		bbdd = pymysql.connect(host = "localhost", user = "juanlu", passwd = "", db = "bolsa20112015")
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		curr_db = "bolsa20112015"

		return [sql, bbdd, curr_db]

	# -- abrir db de bolsa20162020 --
	def db_bolsa20162020(self):
		bbdd = pymysql.connect(host = "localhost", user = "juanlu", passwd = "", db = "bolsa20162020")
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		curr_db = "bolsa20162020"

		return [sql, bbdd, curr_db]

	# -- abrir db de bolsa20212025 --
	def db_bolsa20212025(self):
		bbdd = pymysql.connect(host = "localhost", user = "juanlu", passwd = "", db = "bolsa20212025")
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		curr_db = "bolsa20212025"

		return [sql, bbdd, curr_db]

	# -- abrir db de statsrt --
	def db_statsrt(self):
		bbdd = pymysql.connect(host = "localhost", user = "juanlu", passwd = "", db = "statsrt")
		#bbdd = pymysql.connect(host = "localhost", user = "juanlu", passwd = "", db = "stats1")
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		curr_db = "statsrt"

		return [sql, bbdd, curr_db]

	# -- abrir db de panoramix (se usa para pruebas reales desde idefix) --
	def db_panoramix(self):
		bbdd = pymysql.connect(host = "192.168.1.15", user = "juanlu", passwd = "", db = "bolsart")
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		curr_db = "panoramix"

		return [sql, bbdd, curr_db]

	# -- abrir db de asterix fuera de la red local --
	def db_remote(self):
		bbdd = pymysql.connect(host = "mulderbot.no-ip.org", user = "juanlu", passwd = "", db = "bolsart")
		sql = bbdd.cursor(pymysql.cursors.DictCursor)
		curr_db = "remote"

		return [sql, bbdd, curr_db]


