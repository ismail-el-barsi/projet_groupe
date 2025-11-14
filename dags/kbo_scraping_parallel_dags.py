"""
DAGs dynamiques pour scraping parallèle par batches
Chaque DAG traite 1000 entreprises en parallèle
"""
import os
import sys
import csv
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ajouter les chemins
dag_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(dag_dir)
services_dir = os.path.join(parent_dir, 'services')

if services_dir not in sys.path:
    sys.path.insert(0, services_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from fetch_proxies import fetch_all_proxies
from proxy_manager import ProxyManager
from kbo_scraper import KBOScraper
from scraping_state_manager import ScrapingStateManager


# Configuration
BATCH_SIZE = 500   # Nombre d'entreprises par batch
MAX_BATCHES = 40   # Maximum de batches à créer (limite pour éviter surcharge)
CSV_FILE = os.path.join(parent_dir, "data/enterprise.csv")
USE_PROXY = os.getenv('KBO_USE_PROXY', 'true').lower() == 'true'

# Arguments par défaut
default_args = {
    'owner': 'kbo_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def load_enterprises_by_batch(csv_file, batch_size):
    """Charge les entreprises et les divise en batches"""
    enterprises = []
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                enterprise_number = row.get('EnterpriseNumber') or row.get('enterprise_number') or row.get('number')
                if enterprise_number:
                    enterprises.append(enterprise_number)
    except Exception as e:
        print(f"Erreur lors de la lecture du CSV: {e}")
        return []
    
    # Créer les batches
    batches = []
    for i in range(0, len(enterprises), batch_size):
        batches.append(enterprises[i:i + batch_size])
    
    return batches


def fetch_proxies_task():
    """Tâche commune pour récupérer les proxies"""
    print("Récupération des proxies...")
    output_file = os.path.join(parent_dir, "proxies_list.txt")
    proxies = fetch_all_proxies(output_file)
    print(f"Total de {len(proxies)} proxies récupérés")
    return len(proxies)


def scrape_batch_task(batch_id, batch_enterprises):
    """Tâche pour scraper un batch d'entreprises"""
    print(f"=== Démarrage du batch {batch_id} ===")
    print(f"Nombre d'entreprises: {len(batch_enterprises)}")
    
    # Initialiser le state manager
    state_manager = ScrapingStateManager(
        state_file=os.path.join(parent_dir, "data/scraping_state.json")
    )
    
    # Créer ou récupérer le batch
    existing_state = state_manager.get_batch_state(batch_id)
    if existing_state['status'] == 'pending':
        state_manager.create_batch(batch_id, batch_enterprises)
    
    state_manager.start_batch(batch_id)
    
    # Récupérer les entreprises restantes à scraper
    remaining = state_manager.get_remaining_enterprises(batch_id)
    if not remaining:
        print(f"Batch {batch_id} déjà complété")
        state_manager.finish_batch(batch_id)
        return
    
    print(f"Entreprises restantes à scraper: {len(remaining)}")
    
    # Initialiser le scraper
    if USE_PROXY:
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
        scraper = KBOScraper(
            output_dir=os.path.join(parent_dir, "data/html_pages"),
            use_proxy=False
        )
    
    # Scraper les entreprises restantes
    for idx, enterprise_number in enumerate(remaining, 1):
        print(f"[{idx}/{len(remaining)}] Scraping {enterprise_number}")
        success = scraper.scrape_enterprise(enterprise_number)
        
        if success:
            state_manager.mark_enterprise_completed(batch_id, enterprise_number)
        else:
            state_manager.mark_enterprise_failed(batch_id, enterprise_number)
    
    # Terminer le batch
    state_manager.finish_batch(batch_id)
    
    # Afficher les stats
    batch_state = state_manager.get_batch_state(batch_id)
    print(f"\n=== Batch {batch_id} terminé ===")
    print(f"Réussis: {len(batch_state['completed'])}/{batch_state['total']}")
    print(f"Échoués: {len(batch_state['failed'])}/{batch_state['total']}")
    
    return {
        'batch_id': batch_id,
        'completed': len(batch_state['completed']),
        'failed': len(batch_state['failed']),
        'total': batch_state['total']
    }


# Charger les entreprises et créer les batches
try:
    batches = load_enterprises_by_batch(CSV_FILE, BATCH_SIZE)
    
    # Limiter le nombre de batches
    if len(batches) > MAX_BATCHES:
        print(f"⚠️ Trop de batches ({len(batches)}). Limitation à {MAX_BATCHES} batches.")
        print(f"   Total entreprises: {len(batches) * BATCH_SIZE}")
        print(f"   Entreprises traitées: {MAX_BATCHES * BATCH_SIZE}")
        batches = batches[:MAX_BATCHES]
    
    print(f"✅ Nombre de batches à créer: {len(batches)}")
except Exception as e:
    print(f"❌ Erreur lors du chargement des batches: {e}")
    batches = []


# Créer un DAG pour chaque batch
for batch_idx, batch_enterprises in enumerate(batches):
    # Nommer les batches par multiples de BATCH_SIZE (batch_500, batch_1000, batch_1500...)
    batch_number = (batch_idx + 1) * BATCH_SIZE
    batch_id = f"batch_{batch_number}"
    dag_id = f"kbo_scraping_{batch_id}"
    
    with DAG(
        dag_id,
        default_args=default_args,
        description=f'Scraping batch {batch_number} ({len(batch_enterprises)} entreprises)',
        schedule='0 6 * * *',  # 6h00 tous les jours
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=['kbo', 'scraping', 'parallel', f'batch_{batch_idx}'],
        max_active_runs=1,
    ) as dag:
        
        # Tâche 1: Récupérer les proxies (partagée)
        task_fetch_proxies = PythonOperator(
            task_id='fetch_proxies',
            python_callable=fetch_proxies_task,
        )
        
        # Tâche 2: Scraper le batch
        task_scrape_batch = PythonOperator(
            task_id=f'scrape_{batch_id}',
            python_callable=scrape_batch_task,
            op_kwargs={
                'batch_id': batch_id,
                'batch_enterprises': batch_enterprises
            },
        )
        
        # Ordre d'exécution
        task_fetch_proxies >> task_scrape_batch
        
        # Enregistrer le DAG dans le namespace global
        globals()[dag_id] = dag

print(f"✅ {len(batches)} DAGs de scraping parallèle créés")
