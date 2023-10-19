import psycopg2
import csv
import os
from datetime import date

class DbFiles:

    def __init__(self):
        self.connection = None
        self.host = 'localhost'
        self.port = 5432
        self.database = 'northwind'
        self.user = 'northwind_user'
        self.password = 'thewindisblowing'

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            print('Connection successful!')
        except psycopg2.Error as e:
            print('Error connecting to database:', e)

    def close(self):
        if self.connection:
            self.connection.close()
            print('close connection')

    def export_tables_to_csv(self):
        if not self.connection:
            print('No database connection established.')
            return

        cursor = self.connection.cursor()

        try:
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
            table_names = cursor.fetchall()

            output_dir = f"./reports/{date.today()}"
            os.makedirs(output_dir, exist_ok=True)

            for table_name in table_names:
                csv_file = os.path.join(output_dir, f"{table_name[0]}.csv")

                with open(csv_file, 'w', newline='') as file:
                    writer = csv.writer(file)

                    cursor.execute(f"SELECT * FROM {table_name[0]};")
                    rows = cursor.fetchall()

                    column_names = [desc[0] for desc in cursor.description]
                    writer.writerow(column_names)

                    for row in rows:
                        writer.writerow(row)

                print(f"CSV file '{csv_file}' successfully created for postegres table'{table_name[0]}'.")

        except psycopg2.Error as e:
            print('Error when exporting tables to CSV:', e)

        cursor.close()


    def extract():
        db_connection = DbFiles()
        db_connection.connect()
        db_connection.export_tables_to_csv()
        db_connection.close()
