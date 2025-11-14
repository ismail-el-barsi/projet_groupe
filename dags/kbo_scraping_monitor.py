"""
DAG de coordination et monitoring pour le scraping parallèle
Surveille tous les batches et affiche les statistiques globales
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Ajouter les chemins
dag_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(dag_dir)
services_dir = os.path.join(parent_dir, 'services')

if services_dir not in sys.path:
    sys.path.insert(0, services_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from scraping_state_manager import ScrapingStateManager


default_args = {
    'owner': 'kbo_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def display_global_stats():
    """Affiche les statistiques globales de scraping"""
    state_manager = ScrapingStateManager(
        state_file=os.path.join(parent_dir, "data/scraping_state.json")
    )
    
    stats = state_manager.get_global_stats()
    
    print("\n" + "="*60)
    print("📊 STATISTIQUES GLOBALES DU SCRAPING")
    print("="*60)
    print(f"Total batches: {stats['total_batches']}")
    print(f"Batches complétés: {stats['completed_batches']}")
    print(f"Batches en cours: {stats['in_progress_batches']}")
    print(f"\nTotal entreprises: {stats['total_enterprises']}")
    print(f"✅ Complétées: {stats['completed_enterprises']}")
    print(f"❌ Échouées: {stats['failed_enterprises']}")
    print(f"\n📈 Progression: {stats['progress_pct']:.2f}%")
    print("="*60 + "\n")
    
    return stats


def display_batch_details():
    """Affiche les détails de chaque batch"""
    state_manager = ScrapingStateManager(
        state_file=os.path.join(parent_dir, "data/scraping_state.json")
    )
    
    print("\n" + "="*60)
    print("📋 DÉTAILS PAR BATCH")
    print("="*60)
    
    for batch_id, batch_data in state_manager.state['batches'].items():
        status_icon = {
            'pending': '⏸️',
            'in_progress': '▶️',
            'completed': '✅',
            'failed': '❌'
        }.get(batch_data['status'], '❓')
        
        print(f"\n{status_icon} {batch_id.upper()}")
        print(f"  Status: {batch_data['status']}")
        print(f"  Total: {batch_data['total']}")
        print(f"  Complétés: {len(batch_data['completed'])}")
        print(f"  Échoués: {len(batch_data['failed'])}")
        
        if batch_data['started_at']:
            print(f"  Démarré: {batch_data['started_at']}")
        if batch_data['finished_at']:
            print(f"  Terminé: {batch_data['finished_at']}")
    
    print("="*60 + "\n")


def retry_failed_enterprises():
    """Réinitialise les entreprises échouées pour retry"""
    state_manager = ScrapingStateManager(
        state_file=os.path.join(parent_dir, "data/scraping_state.json")
    )
    
    total_failed = 0
    for batch_id, batch_data in state_manager.state['batches'].items():
        failed_count = len(batch_data['failed'])
        if failed_count > 0:
            print(f"Réinitialisation de {failed_count} entreprises échouées pour {batch_id}")
            state_manager.reset_failed_enterprises(batch_id)
            total_failed += failed_count
    
    print(f"\n✅ Total: {total_failed} entreprises échouées réinitialisées pour retry")
    return total_failed


# Définition du DAG de monitoring
with DAG(
    'kbo_scraping_monitor',
    default_args=default_args,
    description='Monitoring et coordination des DAGs de scraping parallèle',
    schedule='*/30 * * * *',  # Toutes les 30 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['kbo', 'monitoring', 'coordination'],
) as dag:
    
    # Tâche 1: Afficher les statistiques globales
    task_global_stats = PythonOperator(
        task_id='display_global_stats',
        python_callable=display_global_stats,
    )
    
    # Tâche 2: Afficher les détails par batch
    task_batch_details = PythonOperator(
        task_id='display_batch_details',
        python_callable=display_batch_details,
    )
    
    # Ordre d'exécution
    task_global_stats >> task_batch_details


# DAG manuel pour retry des échecs
with DAG(
    'kbo_scraping_retry_failed',
    default_args=default_args,
    description='Réessayer les entreprises échouées',
    schedule=None,  # Déclenchement manuel uniquement
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['kbo', 'retry', 'manual'],
) as dag_retry:
    
    task_retry = PythonOperator(
        task_id='retry_failed_enterprises',
        python_callable=retry_failed_enterprises,
    )
