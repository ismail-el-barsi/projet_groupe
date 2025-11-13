"""
DAG Airflow pour orchestrer le scraping quotidien à 6h00
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Déterminer les chemins en fonction de l'environnement
dag_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(dag_dir)

# Dans Docker, le parent_dir sera /opt/airflow
services_dir = os.path.join(parent_dir, 'services')

# Ajouter au path Python
if services_dir not in sys.path:
    sys.path.insert(0, services_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Import direct des modules
from fetch_proxies import fetch_all_proxies
from kbo_scraper import KBOScraper
from proxy_manager import ProxyManager

# Arguments par défaut du DAG
default_args = {
    'owner': 'kbo_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def fetch_proxies_task():
    """Tâche pour récupérer les proxies"""
    print("Récupération des proxies...")
    output_file = os.path.join(parent_dir, "proxies_list.txt")
    proxies = fetch_all_proxies(output_file)
    print(f"Total de {len(proxies)} proxies récupérés")
    return len(proxies)


def scrape_enterprises_task():
    """Tâche pour scraper les entreprises"""
    print("Démarrage du scraping des entreprises...")
    
    # Mode avec proxy activé par défaut
    use_proxy = os.getenv('KBO_USE_PROXY', 'true').lower() == 'true'
    
    if use_proxy:
        # Initialiser le gestionnaire de proxies
        proxy_manager = ProxyManager(
            proxy_file=os.path.join(parent_dir, "proxies_list.txt"),
            max_concurrent=20,
            request_delay=20,
            cooldown_time=300
        )
        scraper = KBOScraper(
            proxy_manager=proxy_manager,
            output_dir=os.path.join(parent_dir, "data/html_pages"),
            use_proxy=True
        )
    else:
        # Mode direct (sans proxy)
        scraper = KBOScraper(
            output_dir=os.path.join(parent_dir, "data/html_pages"),
            use_proxy=False
        )
    
    # Scraper les entreprises
    csv_file = os.path.join(parent_dir, "data/enterprise.csv")
    scraper.scrape_from_csv(csv_file, limit=None)
    
    print(f"Scraping terminé - Stats: {scraper.stats}")
    return scraper.stats


def generate_stats_task():
    """Tâche pour générer les statistiques"""
    print("Génération des statistiques...")
    # TODO: Implémenter la génération de statistiques
    # - Nombre de pages scrapées
    # - Nombre d'erreurs
    # - Performance des proxies
    return "Stats générées"


# Définition du DAG
with DAG(
    'kbo_daily_scraping',
    default_args=default_args,
    description='Scraping quotidien des entreprises KBO avec gestion de proxies',
    schedule='0 6 * * *',  # Tous les jours à 6h00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['kbo', 'scraping', 'daily'],
) as dag:
    
    # Tâche 1: Récupérer les proxies
    task_fetch_proxies = PythonOperator(
        task_id='fetch_proxies',
        python_callable=fetch_proxies_task,
    )
    
    # Tâche 2: Scraper les entreprises
    task_scrape = PythonOperator(
        task_id='scrape_enterprises',
        python_callable=scrape_enterprises_task,
    )
    
    # Tâche 3: Générer les statistiques
    task_stats = PythonOperator(
        task_id='generate_statistics',
        python_callable=generate_stats_task,
    )
    
    # Définir l'ordre d'exécution
    task_fetch_proxies >> task_scrape >> task_stats
