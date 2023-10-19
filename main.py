from src.extract.csvFiles import CsvFiles
from src.extract.dbFiles import DbFiles

from src.load.saveDb import SaveDb

from src.transform.sqlQuerys import SqlQuerys

from datetime import date
import time


while True:
    today = date.today()
    print ("Running files...", today)
    time.sleep(5)


    ## 1º step - Extract files and save localy in csv
    print("Copying CSV files to the reports folder", today)
    time.sleep(5)
    # csv local files
    CsvFiles.extract('./data', "order_details*.csv" )
    # database files from northwind 
    DbFiles.extract()


    ## 2º step - Loading csv files and save in a database
    print('Saving local data in MySQL Database')
    time.sleep(5)
    SaveDb.load()


    ## 3º step - Transform data - Joining orders tables with order_details
    print("Performing query... Joining orders tables with order_detail")
    time.sleep(5)
    SqlQuerys.transform()

    # Ensuring that the program runs every 24 hours
    time.sleep(24*60*60 -20)