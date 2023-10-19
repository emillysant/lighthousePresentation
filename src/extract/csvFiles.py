import pandas as pd
import glob
import os
import datetime

class CsvFiles():

    def extract(directory, fileName):

        ## Extracting files from csv
        all_files = glob.glob(os.path.join(directory, fileName))

        ## Creating a dataframe
        df = pd.read_csv(all_files[0], names=["order_id", "product_id", "unit_price", "quantity", "discount"], delimiter=',', header=0)
        for f in all_files[1:]:
           temp_df = pd.read_csv(f, names=["order_id", "product_id", "unit_price", "quantity", "discount"], delimiter=',', header=None)
           df = pd.concat([df, temp_df])
        
        ## Creating report and date directories
        todayDate = datetime.date.today()
        reportsPath = f"./reports/{todayDate}"
        if not os.path.exists(reportsPath):
            os.makedirs(reportsPath)

        ## Saving to local repository in CSV
        pathFile = os.path.join(reportsPath, "order_details.csv")
        df.to_csv(pathFile, index=False)
        print(f"File saved in: {pathFile}")