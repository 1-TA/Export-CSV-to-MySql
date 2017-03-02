import pandas as pd
from mysql.connector import MySQLConnection, Error, connect
from os import listdir, remove
import numpy as np

class LoadCSVtoDB():
    def __init__(self):

    def loadData(self):
        filenames = listdir('C:\\Users\\Admin\\results')
        
        k = len(filenames)
        for i in range(0, k):   
            source = 'C:\\Users\\Admin\\results\\%s'%filenames[i]
            data = pd.read_csv(source)
            print(source)
            data.fillna('NA', inplace = True)
            self.InsertPracticeInfo(data[['TDate','ProcCodeID','TotQty','CustomerID','MasterID','MyZip','MaxPDate']], source)
   
   def InsertPracticeInfo(self, data, filename):
        try:
            conn = connect(host='DB_IP_Address', database = 'DB_NAME', user = 'db_username', password = 'db_password')
            if conn.is_connected():
                print('Connected to the MySql database')            
                query = 'insert into Table_Name (TDate,ProcCodeID,TotQty,CustomerID,MasterID,MyZip,MaxPDate) values(%s, %s, %s, %s, %s, %s, %s)'
                args = (data.values.tolist())
                cursor = conn.cursor()
                cursor.executemany(query,args)
                cursor.fet
                conn.commit()
                
                #Deleting the filename after inserting to db.
                remove(filename)
        except Error as e:
            print(e)
        
        finally:
            cursor.close()
            conn.close()
def main():
    a = LoadCSVtoDB()
    a.loadData()


if __name__ == '__main__' :
    main()
