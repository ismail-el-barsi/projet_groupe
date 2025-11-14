"""
DAG de monitoring pour le scraping continu
Surveille la progression des 10 DAGs et affiche les statistiques
"""
import csv
import json
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ajouter les chemins
dag_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(dag_dir)

CSV_FILE = os.path.join(parent_dir, "data/enterprise.csv")
HTML_DIR = os.path.join(parent_dir, "data/html_pages")
PROGRESS_FILE = os.path.join(parent_dir, "data/dag_progress.json")

default_args = {
    'owner': 'kbo_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def display_global_stats():
    """Affiche les statistiques globales de scraping"""
    
    # Compter les entreprises totales
    total_enterprises = 0
    try:
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for _ in reader:
                total_enterprises += 1
    except Exception as e:
        print(f"❌ Erreur lecture CSV: {e}")
        return
    
    # Compter les entreprises scrapées
    scraped_enterprises = 0
    if os.path.exists(HTML_DIR):
        scraped_enterprises = len([f for f in os.listdir(HTML_DIR) if f.endswith('.html')])
    
    # Calculer le reste
    remaining = total_enterprises - scraped_enterprises
    progress = (scraped_enterprises / total_enterprises * 100) if total_enterprises > 0 else 0
    
    print("\n" + "="*70)
    print("📊 STATISTIQUES GLOBALES DU SCRAPING")
    print("="*70)
    print(f"📁 Total entreprises    : {total_enterprises:,}")
    print(f"✅ Déjà scrapées        : {scraped_enterprises:,}")
    print(f"⏳ Restantes            : {remaining:,}")
    print(f"📈 Progression          : {progress:.2f}%")
    
    # Barre de progression
    bar_length = 50
    filled = int(bar_length * progress / 100)
    bar = "█" * filled + "░" * (bar_length - filled)
    print(f"\n[{bar}] {progress:.1f}%")
    print("="*70 + "\n")
    
    return {
        'total': total_enterprises,
        'scraped': scraped_enterprises,
        'remaining': remaining,
        'progress': progress
    }


def display_dag_progress():
    """Affiche la progression de chaque DAG"""
    
    # Charger les positions des DAGs
    if not os.path.exists(PROGRESS_FILE):
        print("⚠️  Aucun fichier de progression trouvé")
        return
    
    try:
        with open(PROGRESS_FILE, 'r') as f:
            dag_progress = json.load(f)
    except:
        print("❌ Erreur lecture fichier de progression")
        return
    
    # Compter total entreprises
    total_enterprises = 0
    try:
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for _ in reader:
                total_enterprises += 1
    except:
        total_enterprises = 0
    
    print("\n" + "="*70)
    print("📋 POSITION DE CHAQUE DAG")
    print("="*70)
    
    for dag_id, index in sorted(dag_progress.items()):
        dag_num = dag_id.replace('kbo_scraping_dag_', '')
        progress = (index / total_enterprises * 100) if total_enterprises > 0 else 0
        print(f"DAG {dag_num:>2} : index {index:>7,} / {total_enterprises:,} ({progress:>5.1f}%)")
    
    print("="*70 + "\n")


# Définition du DAG de monitoring
with DAG(
    'kbo_scraping_monitor',
    default_args=default_args,
    description='Monitoring de la progression du scraping continu',
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
    
    # Tâche 2: Afficher la progression des DAGs
    task_dag_progress = PythonOperator(
        task_id='display_dag_progress',
        python_callable=display_dag_progress,
    )
    
    # Ordre d'exécution
    task_global_stats >> task_dag_progress
