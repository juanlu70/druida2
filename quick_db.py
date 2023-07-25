import libdb_mysql


db = libdb_mysql.LibDbMysql()

db.debug_mode = 2
database = "diario"

ticker = 'KR-XBTUSD'

params = [
    "SELECT close",
    "fecha='2023-01-01'",
    "ORDER BY secs DESC LIMIT 1 OFFSET 0"
]
result = db.select(params, "KR-XBTUSD", "diario")
print("RESULT:")
print(result)

params = [
    "SELECT high",
    "fecha='2023-01-01'",
    "ORDER BY secs DESC LIMIT 1 OFFSET 0"
]
result = db.select(params, "KR-XBTUSD", "diario")
print("RESULT:")
print(result)

params = [
    "SELECT low",
    "fecha='2023-01-01'",
    "ORDER BY secs DESC LIMIT 1 OFFSET 0"
]
result = db.select(params, "KR-XBTUSD", "diario")
print("RESULT:")
print(result)

params = [
    "SELECT open",
    "fecha='2023-01-01'",
    "ORDER BY secs DESC LIMIT 1 OFFSET 0"
]
result = db.select(params, "KR-XBTUSD", "diario")
print("RESULT:")
print(result)

params = [
    "ticker='KR-XBTUSD'",
    "fecha='2023-01-01'",
    "ORDER BY secs"
]
result = db.select(params, "quotes1", "bolsa20212025")
print("RESULT:")
print(result)

