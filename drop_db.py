import sqlite3, config

connection = sqlite3.connect(config.DB_FILE)

cursor = connection.cursor()

# cursor.execute("""DROP TABLE stock_price""")
#
# cursor.execute("""DROP TABLE stock""")
#
# cursor.execute("""DROP TABLE stock_strategy""")
#
# cursor.execute("""DROP TABLE strategy""")

# cursor.execute("""DROP TABLE income_annual_general""")
#
# cursor.execute("""DROP TABLE balance_annual_general""")
#
# cursor.execute("""DROP TABLE cashflow_annual_general""")
#

# cursor.execute("""DROP TABLE income_quarter_general""")
#
# cursor.execute("""DROP TABLE balance_quarter_general""")
#
# cursor.execute("""DROP TABLE cashflow_quarter_general""")

cursor.execute("""DROP TABLE simfin_stock_price""")




connection.commit()