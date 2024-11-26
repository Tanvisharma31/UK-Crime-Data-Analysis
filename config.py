import configparser

def create_config():
    config = configparser.ConfigParser()
    config['Postgres'] = {"host" : "host","user":"user","password":"password","db" : "db_name"} #TODO: Change config.ini using this file 
    with open("config.ini","w") as configfile:
        config.write(configfile)

if __name__ == "__main__":
    create_config()