"""
10 DAGs simples - Chaque DAG scrape 1 entreprise, puis se relance automatiquement
"""
import csv
import json
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Ajouter les chemins
dag_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(dag_dir)
services_dir = os.path.join(parent_dir, 'services')

if services_dir not in sys.path:
    sys.path.insert(0, services_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from fetch_proxies import fetch_all_proxies
from kbo_scraper import KBOScraper
from proxy_manager import ProxyManager

# Configuration
NUM_DAGS = 10   # 10 DAGs seulement
CSV_FILE = os.path.join(parent_dir, "data/enterprise.csv")
HTML_DIR = os.path.join(parent_dir, "data/html_pages")
PROGRESS_FILE = os.path.join(parent_dir, "data/dag_progress.json")
USE_PROXY = os.getenv('KBO_USE_PROXY', 'true').lower() == 'true'  # Toujours activé

# Arguments par défaut
default_args = {
    'owner': 'kbo_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}





def load_all_enterprises():
    """Charge toutes les entreprises depuis le CSV"""
    enterprises = []
    try:
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                enterprise_number = row.get('EnterpriseNumber') or row.get('enterprise_number') or row.get('number')
                if enterprise_number:
                    enterprises.append(enterprise_number)
    except Exception as e:
        print(f"❌ Erreur lors de la lecture du CSV: {e}")
        return []
    
    return enterprises


def is_already_scraped(enterprise_number):
    """Vérifie si une entreprise est déjà scrapée"""
    output_file = os.path.join(HTML_DIR, f"{enterprise_number}.html")
    return os.path.exists(output_file)


def is_being_scraped(enterprise_number):
    """Vérifie si une entreprise est en cours de scraping par un autre DAG"""
    lock_file = os.path.join(parent_dir, "data/locks", f"{enterprise_number}.lock")
    if not os.path.exists(lock_file):
        return False
    
    # Vérifier si le lock est récent (moins de 5 minutes)
    try:
        import time
        file_age = time.time() - os.path.getmtime(lock_file)
        if file_age > 300:  # 5 minutes
            # Lock trop vieux, on le supprime
            os.remove(lock_file)
            return False
        return True
    except:
        return False


def lock_enterprise(enterprise_number, dag_id):
    """Crée un lock pour empêcher d'autres DAGs de scraper cette entreprise"""
    lock_dir = os.path.join(parent_dir, "data/locks")
    os.makedirs(lock_dir, exist_ok=True)
    
    lock_file = os.path.join(lock_dir, f"{enterprise_number}.lock")
    with open(lock_file, 'w') as f:
        f.write(f"{dag_id}\n{datetime.now().isoformat()}")


def unlock_enterprise(enterprise_number):
    """Supprime le lock d'une entreprise"""
    lock_file = os.path.join(parent_dir, "data/locks", f"{enterprise_number}.lock")
    try:
        if os.path.exists(lock_file):
            os.remove(lock_file)
    except:
        pass


def get_dag_progress(dag_id):
    """Récupère l'index actuel pour ce DAG"""
    if not os.path.exists(PROGRESS_FILE):
        return 0
    
    try:
        with open(PROGRESS_FILE, 'r') as f:
            progress = json.load(f)
            return progress.get(dag_id, 0)
    except:
        return 0


def get_failed_count(enterprise_number):
    """Récupère le nombre d'échecs pour une entreprise"""
    failed_file = os.path.join(parent_dir, "data/failed_enterprises.json")
    if not os.path.exists(failed_file):
        return 0
    
    try:
        with open(failed_file, 'r') as f:
            failed = json.load(f)
            return failed.get(enterprise_number, 0)
    except:
        return 0


def mark_enterprise_failed(enterprise_number):
    """Marque une entreprise comme échouée et retourne le nombre total d'échecs"""
    failed_file = os.path.join(parent_dir, "data/failed_enterprises.json")
    failed = {}
    
    if os.path.exists(failed_file):
        try:
            with open(failed_file, 'r') as f:
                failed = json.load(f)
        except:
            pass
    
    failed[enterprise_number] = failed.get(enterprise_number, 0) + 1
    
    os.makedirs(os.path.dirname(failed_file), exist_ok=True)
    with open(failed_file, 'w') as f:
        json.dump(failed, f, indent=2)
    
    return failed[enterprise_number]


def set_dag_progress(dag_id, index):
    """Sauvegarde l'index actuel pour ce DAG"""
    progress = {}
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r') as f:
                progress = json.load(f)
        except:
            pass
    
    progress[dag_id] = index
    
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def get_next_enterprise_for_dag(dag_id):
    """
    Récupère la prochaine entreprise à scraper pour ce DAG
    Commence à l'index sauvegardé, cherche la première non-scrapée
    Ignore les entreprises qui ont échoué plus de 3 fois
    """
    all_enterprises = load_all_enterprises()
    
    if not all_enterprises:
        print(f"❌ Aucune entreprise trouvée dans le CSV")
        return None
    
    # Récupérer l'index actuel de ce DAG
    start_index = get_dag_progress(dag_id)
    
    # Chercher la prochaine entreprise non-scrapée
    for i in range(start_index, len(all_enterprises)):
        enterprise = all_enterprises[i]
        
        # Vérifier si déjà scrapée
        if is_already_scraped(enterprise):
            continue
        
        # Vérifier si en cours de scraping par un autre DAG
        if is_being_scraped(enterprise):
            print(f"⏭️  {enterprise} est en cours de scraping par un autre DAG")
            continue
        
        # Vérifier si elle a déjà échoué trop de fois
        failed_count = get_failed_count(enterprise)
        if failed_count >= 3:
            print(f"⏭️  {enterprise} a échoué {failed_count} fois, passage à la suivante")
            continue
        
        print(f"📋 DAG {dag_id}: Entreprise {enterprise} (index {i}/{len(all_enterprises)}, échecs: {failed_count})")
        return (enterprise, i)
    
    # Si on arrive ici, toutes les entreprises sont scrapées ou ont trop échoué
    print(f"✅ DAG {dag_id}: Toutes les entreprises accessibles sont scrapées !")
    return None



def fetch_proxies_task():
    """Tâche commune pour récupérer les proxies"""
    print("Récupération des proxies...")
    output_file = os.path.join(parent_dir, "proxies_list.txt")
    proxies = fetch_all_proxies(output_file)
    print(f"Total de {len(proxies)} proxies récupérés")
    return len(proxies)


def scrape_single_enterprise_task(dag_id):
    """
    Tâche simple : scrape UNE entreprise, puis termine
    À la prochaine exécution, prendra l'entreprise suivante
    """
    print(f"\n{'='*60}")
    print(f"🚀 {dag_id} - Démarrage")
    print(f"{'='*60}\n")
    
    # Obtenir la prochaine entreprise pour ce DAG
    result = get_next_enterprise_for_dag(dag_id)
    
    if not result:
        print(f"✅ {dag_id}: Rien à faire (tout est scrapé)")
        return {
            'dag_id': dag_id,
            'status': 'no_work',
            'enterprise': None
        }
    
    enterprise_number, index = result  # Déballer le tuple
    
    # LOCK l'entreprise pour éviter que d'autres DAGs la prennent
    lock_enterprise(enterprise_number, dag_id)
    
    print(f"🎯 Scraping de l'entreprise : {enterprise_number}")
    
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
            output_dir=HTML_DIR,
            use_proxy=True
        )
    else:
        scraper = KBOScraper(
            output_dir=HTML_DIR,
            use_proxy=False
        )
    
    # Scraper l'entreprise
    success = scraper.scrape_enterprise(enterprise_number)
    
    # UNLOCK l'entreprise dans tous les cas
    unlock_enterprise(enterprise_number)
    
    # Sauvegarder la progression selon le résultat
    if success:
        set_dag_progress(dag_id, index + 1)
        print(f"✅ {dag_id}: {enterprise_number} scrapé avec succès - index avancé à {index + 1}")
    else:
        # Marquer comme échouée
        fail_count = mark_enterprise_failed(enterprise_number)
        
        if fail_count >= 3:
            # Après 3 échecs, on passe à la suivante
            set_dag_progress(dag_id, index + 1)
            print(f"❌ {dag_id}: {enterprise_number} échec #{fail_count} - ABANDONNÉ, passage à la suivante")
        else:
            # Moins de 3 échecs, on garde le même index pour retry
            print(f"❌ {dag_id}: {enterprise_number} échec #{fail_count}/3 - retry à la prochaine exécution")
    
    # Résultat
    print(f"{'='*60}\n")
    
    return {
        'dag_id': dag_id,
        'status': 'success' if success else 'failed',
        'enterprise': enterprise_number,
        'index': index
    }




# ============================================================================
# GÉNÉRATION DES DAGs
# ============================================================================

print("\n" + "="*70)
print(f"🚀 GÉNÉRATION DES DAGs DE SCRAPING")
print("="*70)

# Vérifier l'état
all_enterprises = load_all_enterprises()
print(f"📊 Total entreprises dans CSV : {len(all_enterprises):,}")
print(f"🔧 Nombre de DAGs de scraping : {NUM_DAGS}")
print("="*70)

# ============================================================================
# DAG 0 : Fetch Proxies (manuel, à exécuter une seule fois)
# ============================================================================

with DAG(
    'kbo_fetch_proxies',
    default_args=default_args,
    description='Récupère les proxies - À exécuter manuellement une fois',
    schedule=None,  # Manuel uniquement
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['kbo', 'setup', 'proxies'],
    max_active_runs=1,
) as dag_proxies:
    
    task_fetch_proxies = PythonOperator(
        task_id='fetch_proxies',
        python_callable=fetch_proxies_task,
    )

globals()['kbo_fetch_proxies'] = dag_proxies

# ============================================================================
# DAGs 1-10 : Scraping en continu
# ============================================================================

# Créer les 10 DAGs
for dag_num in range(1, NUM_DAGS + 1):
    dag_id = f"kbo_scraping_dag_{dag_num}"
    
    with DAG(
        dag_id,
        default_args=default_args,
        description=f'DAG {dag_num} - Scrape 1 entreprise et se relance',
        schedule=None,  # Pas de schedule automatique, se déclenche lui-même
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=['kbo', 'scraping', 'auto', f'dag_{dag_num}'],
        max_active_runs=1,
    ) as dag:
        
        # Tâche : Scraper 1 entreprise
        task_scrape = PythonOperator(
            task_id='scrape_enterprise',
            python_callable=scrape_single_enterprise_task,
            op_kwargs={
                'dag_id': dag_id
            },
        )
        
        # Tâche : Relancer ce même DAG pour la prochaine entreprise
        task_trigger_next = TriggerDagRunOperator(
            task_id='trigger_next_run',
            trigger_dag_id=dag_id,  # Se déclenche lui-même
            wait_for_completion=False,
            reset_dag_run=False,
        )
        
        # Ordre d'exécution : scrape puis relance
        task_scrape >> task_trigger_next
        
        # Enregistrer le DAG
        globals()[dag_id] = dag

print(f"\n✅ 1 DAG de setup + {NUM_DAGS} DAGs de scraping créés")
print(f"")
print(f"📋 Pour démarrer :")
print(f"   1. Exécuter 'kbo_fetch_proxies' une fois (manuel)")
print(f"   2. Activer les 10 DAGs de scraping")
print(f"   3. Cliquer 'Trigger' une fois sur chaque DAG (1 à 10)")
print(f"   4. Les DAGs se relanceront automatiquement après chaque entreprise")
print(f"")
print(f"🔄 Mode : Auto-relance continue")
print("="*70 + "\n")
