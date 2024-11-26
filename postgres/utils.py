import psycopg2
import configparser

config = configparser.ConfigParser()
config.read('../config.ini') 

host = config['Postgres']['host']
db = config['Postgres']['db']
user = config['Postgres']['user']
password = config['Postgres']['password']

def create_table():
    """ create table in the PostgreSQL database"""
    query = """
        CREATE TABLE table_name(
            category VARCHAR(255) NOT NULL,
            location_type VARCHAR(255),
            location_latitude VARCHAR(255) NOT NULL,
            location_longitude VARCHAR(255) NOT NULL,
            context VARCHAR(255),
            outcome_status VARCHAR(255),
            persistent_id VARCHAR(255),
            id INTEGER PRIMARY KEY,
            location_subtype VARCHAR(255),
            month SMALLINT NOT NULL,
            year SMALLINT NOT NULL,
            borough VARCHAR(255) NOT NULL
        )
        """ #TODO: Change table_name in line 15 to your table name
        
    connection = None
    try:
        connection = psycopg2.connect(host=host,database=db,user=user,password=password)
        cursor = connection.cursor()
        cursor.execute(query=query)
        cursor.close()
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()

if __name__ == "__main__":
    create_table()