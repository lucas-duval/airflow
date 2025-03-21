from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import os
from datetime import datetime


class Eco2MixOperator(BaseOperator):
    @apply_defaults
    def __init__(self, output_path, start_date=None, end_date=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_path = output_path
        self.start_date = start_date
        self.end_date = end_date

    def execute(self, context):
        base_url = 'https://www.rte-france.com/eco2mix/telecharger-les-indicateurs'

        # Gestion des dates
        if self.start_date and self.end_date:
            start = datetime.strptime(self.start_date, '%Y-%m-%d').strftime('%d/%m/%Y')
            end = datetime.strptime(self.end_date, '%Y-%m-%d').strftime('%d/%m/%Y')
            url = f'{base_url}?debut={start}&fin={end}'
        else:
            url = base_url

        try:
            response = requests.get(url)
            response.raise_for_status()

            # Création du chemin de sortie
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)

            # Sauvegarde du fichier CSV
            with open(self.output_path, 'wb') as file:
                file.write(response.content)

            self.log.info(f'Données téléchargées et sauvegardées à {self.output_path}')

        except requests.exceptions.RequestException as e:
            self.log.error(f'Erreur lors du téléchargement : {e}')
            raise
