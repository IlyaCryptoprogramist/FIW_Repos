import sqlite3
conn = sqlite3.connect(r'D:\git\FIW_Repos\funding_data.db')
c = conn.cursor()
c.execute("SELECT exchange, COUNT(*) FROM funding_rates GROUP BY exchange")
for row in c.fetchall():
    print(f"{row[0]}: {row[1]} записей")
conn.close()