
from airflow.models.baseoperator import BaseOperator
import pandas as pd


class ExcelToCSV(BaseOperator):
    def __init__(self, input_path: str, output_path: str, **kwargs):
        super().__init__(**kwargs)
        self.input_path = input_path
        self.output_path = output_path

    def execute(self, context):
        print(f'Reading CSV file with tab separator: {self.input_path}')

        # Lire le fichier CSV avec gestion d'encodage
        try:
            df = pd.read_csv(self.input_path, sep='\t', encoding='utf-8', engine='python')
        except UnicodeDecodeError:
            print("UTF-8 decoding failed. Trying Latin-1 encoding.")
            df = pd.read_csv(self.input_path, sep='\t', encoding='latin1', engine='python')

        print(df.head())  # Afficher un aperçu du fichier chargé

        # Sauvegarder le fichier avec des virgules comme séparateur
        print(f'Writing transformed CSV file: {self.output_path}')
        df.to_csv(self.output_path, index=False, sep=',', encoding='utf-8')

        return self.output_path

