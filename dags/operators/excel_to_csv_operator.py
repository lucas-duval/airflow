import pandas as pd
import os
from airflow.models import BaseOperator
from datetime import datetime, timedelta

class ConvertXlsToCsvOperator(BaseOperator):
    def __init__(self, start: str, end: str, xls_dir: str, csv_dir: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start = start
        self.end = end
        self.xls_dir = xls_dir
        self.csv_dir = csv_dir

    def execute(self, context):
        """
        Convertit les fichiers .xls en .csv pour une plage de dates donnée.
        """
        # Créer la plage de dates
        start = datetime.strptime(self.start, "%Y-%m-%d")
        end = datetime.strptime(self.end, "%Y-%m-%d")
        date_range = [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end - start).days + 1)]

        # Convertir les fichiers .xls en .csv pour chaque date
        for date_str in date_range:
            xls_path = os.path.join(self.xls_dir, f"eCO2mix_RTE_{date_str}.xls")
            os.makedirs(self.csv_dir, exist_ok=True)
            csv_path = os.path.join(self.csv_dir, f"eCO2mix_RTE_{date_str}.csv")

            print(f'Reading XLS file for date {date_str}: {xls_path}')

            # Essayer de lire le fichier avec différents encodages
            try:
                # Tentative de lecture en UTF-8
                df = pd.read_csv(xls_path, encoding='utf-8-sig', delimiter='\t', engine='python')
            except UnicodeDecodeError:
                print(f"UTF-8 decoding failed for {xls_path}. Trying Latin-1 encoding.")
                # Si l'UTF-8 échoue, essayer avec l'encodage Latin-1
                try:
                    df = pd.read_csv(xls_path, encoding='latin1', delimiter='\t', engine='python')
                except Exception as e:
                    print(f"❌ Erreur lors de la lecture du fichier {xls_path}: {e}")
                    continue  # Passer à la date suivante si une erreur survient
            except Exception as e:
                print(f"❌ Erreur lors de la lecture du fichier {xls_path}: {e}")
                continue  # Passer à la date suivante si une erreur survient

            print(f"✅ Fichier chargé pour {date_str}. Sauvegarde en CSV...")

            # Sauvegarder le fichier avec des virgules comme séparateur
            try:
                df.to_csv(csv_path, index=False, sep=',', encoding='utf-8-sig')
                print(f"✅ Fichier converti et sauvegardé : {csv_path}")
            except Exception as e:
                print(f"❌ Erreur lors de la sauvegarde du fichier CSV {csv_path}: {e}")
