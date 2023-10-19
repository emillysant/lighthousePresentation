import os
import pandas as pd
import psycopg2

from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import date

class SaveDb:

    def __init__(self):
        self.conexao = None
        self.cursor = None

        load_dotenv()
        self.host = os.getenv('host')
        self.user = os.getenv('user')
        self.password = os.getenv('password')
        self.database = os.getenv('database')
        self.pasta_csv = './reports'

    def conectar(self):
        # Connecting to MySQL database using SQLAlchemy
        try:
            connection_string = f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self.database}"
            self.engine = create_engine(connection_string)
            print('Connection successful!')
        except psycopg2.Error as e:
            print('Error connecting to database:', e)

    def desconectar(self):
        # Close the database connection
        if self.engine:
            self.engine.dispose()
            print('Connection closed.')

    def salvar_csv_no_db(self):
        # Get today
        hoje = date.today().strftime("%Y-%m-%d")
        
        # Check if the folder with today's date exists
        pasta_hoje = os.path.join(self.pasta_csv, hoje)
        if not os.path.isdir(pasta_hoje):
            print(f"The folder with today's date ({hoje}) could not be found.")
            return

        # List all CSV files in the folder with today's date
        arquivos_csv = [
            arquivo for arquivo in os.listdir(pasta_hoje) if arquivo.endswith(".csv")
        ]

        # Iterates over CSV files and saves to the database
        for arquivo in arquivos_csv:
            # Load the CSV file into a pandas DataFrame
            caminho_arquivo = os.path.join(pasta_hoje, arquivo)
            df = pd.read_csv(caminho_arquivo, encoding='latin1')

            # Gets the table name from the CSV file name (without the extension)
            nome_tabela = os.path.splitext(arquivo)[0]

            # Create the table in the database
            df.to_sql(nome_tabela, self.engine, if_exists='replace', index=False)
            print(f"CSV file data '{nome_tabela}' has been added to the table '{nome_tabela}' from MySQL database.")
    
    def load():
        save_on_db = SaveDb()
        save_on_db.conectar()
        save_on_db.salvar_csv_no_db()
        save_on_db.desconectar()