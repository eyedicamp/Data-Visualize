import psycopg2

# Connect postgreSQL DB
conn = psycopg2.connect(
    dbname="postgres",
    user="postgre_user",
    password="popo0403",
    host="localhost",
    port="5432"
)

print("Connecting to Database")

cur = conn.cursor()

# Select all data
cur.execute("SELECT * FROM completion_history_date_yes")

# Select specific data
# cur.execute("SELECT * FROM completion_history WHERE area_paint_1yard_paint > 3000;")
while True:
    result_one = cur.fetchone()
    if result_one is None:
        break
    
    print(result_one)

# Update DB
cur.execute("UPDATE completion_history_date_yes SET area_paint_1yard_paint = area_paint_1yard_paint + 1234 WHERE area_paint_1yard_paint < 50000;")
conn.commit()

cur.close()
conn.close()