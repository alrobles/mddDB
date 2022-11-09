import psycopg2
from csv import reader
import pandas as pd



db = 'defaultdb'
user = 'doadmin'
pwd = 'AVNS_kd6af68cOs7XfPxhFJJ'
host = 'db-postgresql-nyc1-45961-do-user-12013676-0.b.db.ondigitalocean.com'
port = '25060'
filePath = 'data/MDD_v1.2_6485species.csv'
csvColumns = {
    'sciName': 'sciName',
    'id':'id',
    'phylosort':'phylosort',
    'order':'order',
    'family':'family',
    'subfamily':'subfamily',
    'tribe':'tribe',
    'genus':'genus'
    }

database_keys = {
    'species':{'id': 'id', 'sci_id':'sci_id', 'sci_name': 'sci_name', 'version': 'version'},

    'species_family':{'id': 'id', 'family':'family', 'sub_family': 'sub_family', 'tribe': 'tribe'},
    'species_family_mapping':{'id': 'id', 'species_id':'species_id', 'family_id': 'family_id'},

    'species_genus':{'id': 'id', 'genus':'genus'},
    'species_genus_mapping':{'id': 'id', 'species_id':'species_id', 'genus_id': 'genus_id'},

    'species_order':{'id': 'id', 'phylosort':'phylosort','species_order':'species_order'},
    'species_order_mapping':{'id': 'id', 'species_id':'species_id', 'order_id': 'order_id'},
    }

def connect_to_db(db, user, pwd, host, port):
    #establishing the connection
    conn = psycopg2.connect(
    database = db, user = user, password = pwd, host = host, port = port
    )
    conn.autocommit = True

    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    #Executing an MYSQL function using the execute() method
    cursor.execute("select version()")
    data = cursor.fetchone()
    print("Connection established to: ",data)
    return cursor, conn

def read_csv(path = ""):
    # open file in read mode
    df = pd.read_csv (path)
    return df

def insert_species(sciId, sciName, version):
    sqlString = "INSERT INTO species (sci_id, sci_name, version) VALUES (%s,%s,%s) RETURNING id;"
    try:
        cursor.execute(sqlString, (sciId, sciName, version))
        return cursor.fetchone()[0]

    except Exception as e:
        if 'duplicate key value violates unique constraint' in str(e.args):
            sqlString = "SELECT id from species where sci_id = %s and  sci_name = %s and version = %s;"
            try:
                cursor.execute(sqlString, (sciId, sciName, version))
                return cursor.fetchone()[0]
            except Exception as e1:
                print("EXCEPTION OCCURED WHILE READING DATA FROM SPECIES TABLE for QUERY: {} \n Error: {}".format(sqlString, e1))


def insert_family(family, subFamily, tribe):
    sqlString = "INSERT INTO species_family (family, subfamily, tribe) VALUES (%s,%s,%s) RETURNING id;"
    try:
        cursor.execute(sqlString, (family, subFamily, tribe))
        return cursor.fetchone()[0]

    except Exception as e:
       
        if 'duplicate key value violates unique constraint' in str(e.args):
            sqlString = "SELECT id from species_family where family = %s and  subfamily = %s and tribe = %s;"
            try:
                cursor.execute(sqlString, (family, subFamily, tribe))
                return cursor.fetchone()[0]
            except Exception as e1:
                print("EXCEPTION OCCURED WHILE READING DATA FROM SPECIES_FAMILY TABLE for QUERY: {} \n Error: {}".format(sqlString, e1))


def insert_genus(genus):
    sqlString = "INSERT INTO species_genus (genus) VALUES ('{}') RETURNING id;".format(genus)
    try:
        cursor.execute(sqlString)
        return cursor.fetchone()[0]

    except Exception as e:
        print(e)
        if 'duplicate key value violates unique constraint' in str(e.args):
            sqlString = "SELECT id from species_genus where genus = '{}';".format(genus)
            try:
                cursor.execute(sqlString)
                return cursor.fetchone()[0]
            except Exception as e1:
                print("EXCEPTION OCCURED WHILE READING DATA FROM SPECIES_GENUS TABLE for QUERY: {} \n Error: {}".format(sqlString, e1))

def insert_order(order, phylosort):
    sqlString = "INSERT INTO species_order (species_order, phylosort) VALUES (%s,%s) RETURNING id;"
    try:
        cursor.execute(sqlString, (order, phylosort))
        return cursor.fetchone()[0]

    except Exception as e:
        print(e)
        if 'duplicate key value violates unique constraint' in str(e.args):
            sqlString = "SELECT id from species_order where species_order = %s and  phylosort = %s;"
            try:
                cursor.execute(sqlString, (order, phylosort))
                return cursor.fetchone()[0]
            except Exception as e1:
                print("EXCEPTION OCCURED WHILE READING DATA FROM SPECIES_ORDER TABLE for QUERY: {} \n Error: {}".format(sqlString, e1))

def create_mappings(speciesId, familyId, genusId, orderId):
    sqlStringFamily = "INSERT INTO species_family_mapping (species_id, family_id) VALUES (%s,%s) RETURNING id;"
    sqlStringGenus = "INSERT INTO species_genus_mapping (species_id, genus_id) VALUES (%s,%s) RETURNING id;"
    sqlStringOrder = "INSERT INTO species_order_mapping (species_id, order_id) VALUES (%s,%s) RETURNING id;"

    try:
        cursor.execute(sqlStringFamily, (speciesId, familyId))
    except Exception as e:
        print("EXCEPTION OCCURED WHILE INSERTING DATA INTO MAPPINGS TABLE QUERY: {} \nError: {}".format(sqlStringFamily, e))

    try:
        cursor.execute(sqlStringGenus, (speciesId, genusId))
    except Exception as e:
        print("EXCEPTION OCCURED WHILE INSERTING DATA INTO MAPPINGS TABLE QUERY: {} \nError: {}".format(sqlStringGenus, e))

    try:
        cursor.execute(sqlStringOrder, (speciesId, orderId))
    except Exception as e:
        print("EXCEPTION OCCURED WHILE INSERTING DATA INTO MAPPINGS TABLE QUERY: {} \nError: {}".format(sqlStringOrder, e))
    return



def push_to_db(dataFrame, dataVersion):
    for _, row in dataFrame.iterrows():
        speciesId = insert_species(row[csvColumns['id']], row[csvColumns['sciName']], dataVersion)
        print(speciesId)
        familyId = insert_family(row[csvColumns['family']], row[csvColumns['subfamily']], row[csvColumns['tribe']])
        genusId = insert_genus(row[csvColumns['genus']])
        orderId = insert_order(row[csvColumns['order']], row[csvColumns['phylosort']])
        print("Done for species_id: {}, family_id: {}, genus_id: {}, order_id:{}".format(speciesId, familyId, genusId, orderId))
        create_mappings(speciesId, familyId, genusId, orderId)
    return


df = read_csv(filePath)
df = df.replace(pd.np.nan, '', regex=True)
dataVersion = filePath.split('_')[1]

cursor, conn = connect_to_db(db, user, pwd, host, port)    

#testing connection and sample query
cursor.execute('''SELECT * from species limit 2''')

result = cursor.fetchall()
print(result)
push_to_db(df, dataVersion)


#Closing the connection
conn.commit()
conn.close()


