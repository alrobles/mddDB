import psycopg2
from csv import reader



db = 'defaultdb'
user = 'doadmin'
pwd = 'AVNS_kd6af68cOs7XfPxhFJJ'
host = 'db-postgresql-nyc1-45961-do-user-12013676-0.b.db.ondigitalocean.com'
port = '25060'

def connect_to_db(db, user, pwd, host, port):
    #establishing the connection
    conn = psycopg2.connect(
    database = db, user = user, password = pwd, host = host, port = port
    )
    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    #Executing an MYSQL function using the execute() method
    cursor.execute("select version()")
    data = cursor.fetchone()
    print("Connection established to: ",data)
    return cursor, conn

def read_csv(path):
    # open file in read mode
    with open('students.csv', 'r') as read_obj:
        # pass the file object to reader() to get the reader object
        csv_reader = reader(read_obj)
        # Iterate over each row in the csv using reader object
        for row in csv_reader:
            # row variable is a list that represents a row in csv
            print(row)


cursor, conn = connect_to_db(db, user, pwd, host, port)    

#testing connection and sample query
cursor.execute('''SELECT * from species limit 2''')

result = cursor.fetchall();
print(result)


#Closing the connection
conn.close()


