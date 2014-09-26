# This file should offer methods to initialize the database
# functions should check if table is created, then load connections
# else, create the tables and then load connections
# database connection should be an export of this module

import psycopg2


class Database(object):
    """
    This class will provide basic datbase connections and methods.
    TODO: connection pooling
    """
    def __init__(self):
        self.connection = psycopg2.connect(database="postgres", host="/tmp", user="postgres", password="data");
        self.connection.autocommit = True;
        self.cursor = self.connection.cursor();
    
    def __str__(self):
        return "PGSQL database cursor"
    
    def insert(self, row):
        self.cursor.execute("INSERT into fire values (%s, %s, %s, %s, %s, %s, %s, %s, %s)", row);
    
    def query(self):
        self.cursor.execute("SELECT * from fire");
        return self.cursor.fetchall();
    
    def __del__(self):
        self.cursor.close();
        self.connection.close();
        