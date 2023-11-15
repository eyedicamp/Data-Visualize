import psycopg2
import os
import glob

# connect postgreSQL DB
conn = psycopg2.connect(
    dbname="postgres",
    user="postgre_user",
    password="popo0403",
    host="localhost",
    port="5432"
)

print("Connecting to Database")

cur = conn.cursor()

# Loop through each CSV
for filename in glob.glob(os.path.join('.', "*.csv")):
    # Create a table name
    tablename = filename.replace(".csv", "").replace(".\\", "")

    # Open file
    fileInput = open(filename, "r")

    # Extract first line of file
    firstLine = fileInput.readline().strip()
    
    # Type check를 위한 secondLine Input 받기
    secondLine = fileInput.readline().strip()

    # Split columns into an array [...]
    columns = firstLine.split(",")
    typecheck = secondLine.split(",")
    columnType = ['BIGINT' for _ in range(len(columns))]
    
    # Build SQL code to drop table if exists and create table
    sqlQueryCreate = 'DROP TABLE IF EXISTS '+ tablename + ";\n"
    sqlQueryCreate += 'CREATE TABLE '+ tablename + "("

    # Define columns for table
    
    # isdigit가 아니면 string 그대로 받을 수 있게 TIMESTAMP로 형식 지정
    for i in range(len(typecheck)):
        if (not typecheck[i].isdigit()) and typecheck[i] != "":
            columnType[i] = 'TIMESTAMP'
    
    for i in range(len(columns)):
        sqlQueryCreate += columns[i] + " " + columnType[i] + ",\n"

    sqlQueryCreate = sqlQueryCreate[:-2]
    sqlQueryCreate += ");"

    cur.execute(sqlQueryCreate)
    conn.commit()

    # insert data to db table
    
    # 데이터 첫번째 줄 먼저 입력
    sqlQueryCreate = "INSERT INTO " + tablename + " VALUES (%s" + ", %s" * (len(typecheck) - 1) + ")"
    cur.execute(sqlQueryCreate, [int(i) if i != '' and i.isdigit() else None if i=='' else i for i in typecheck])
    conn.commit()

    # 데이터 나머지 줄 입력
    for line in fileInput:
        line = line.strip().split(',')
        sqlQueryCreate = "INSERT INTO " + tablename + " VALUES (%s" + ", %s" * (len(line) - 1) + ")"
        cur.execute(sqlQueryCreate, [int(i) if i != '' and i.isdigit() else None if i=='' else i for i in line])
        conn.commit()

    print(tablename, "create complete")

cur.close()
conn.close()