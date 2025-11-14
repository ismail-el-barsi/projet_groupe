from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import json
import glob

# Ajouter le répertoire services au path
sys.path.insert(0, '/opt/airflow/services')

from data_validator import DataValidator, generate_validation_summary


# Chemins
JSON_DIR = '/opt/airflow/data/extracted_data'
REPORTS_DIR = '/opt/airflow/data/validation_reports'


def validate_data_quality(**context):
    """Tâche principale de validation de la qualité des données."""
    # Créer le répertoire de rapports s'il n'existe pas
    os.makedirs(REPORTS_DIR, exist_ok=True)
    
    # Lister tous les fichiers JSON (exclure les rapports)
    json_pattern = os.path.join(JSON_DIR, '*.json')
    json_files = [f for f in glob.glob(json_pattern) if 'report' not in f.lower()]
    
    print(f"📁 Nombre de fichiers JSON à valider: {len(json_files)}")
    
    if len(json_files) == 0:
        print("⚠️  Aucun fichier JSON trouvé à valider")
        return {
            'status': 'no_files',
            'message': 'Aucun fichier à valider'
        }
    
    # Initialiser le validateur
    validator = DataValidator()
    
    # Valider tous les fichiers
    print("🔍 Démarrage de la validation...")
    report = validator.validate_all(json_files)
    
    # Générer le résumé textuel
    summary = generate_validation_summary(report)
    print(summary)
    
    # Sauvegarder le rapport complet
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = os.path.join(REPORTS_DIR, f'validation_report_{timestamp}.json')
    
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    print(f"📄 Rapport complet sauvegardé: {report_file}")
    
    # Sauvegarder aussi le dernier rapport (pour faciliter l'accès)
    latest_report_file = os.path.join(REPORTS_DIR, 'latest_validation_report.json')
    with open(latest_report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    # Sauvegarder le résumé textuel
    summary_file = os.path.join(REPORTS_DIR, f'validation_summary_{timestamp}.txt')
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write(summary)
    
    # Pousser les statistiques dans XCom
    context['ti'].xcom_push(key='validation_stats', value=report['statistiques'])
    context['ti'].xcom_push(key='report_file', value=report_file)
    
    return report['statistiques']


def generate_dashboard_data(**context):
    """Génère les données pour le dashboard admin."""
    stats = context['ti'].xcom_pull(task_ids='validate_quality', key='validation_stats')
    
    if not stats:
        print("⚠️  Aucune statistique disponible")
        return
    
    # Créer un fichier JSON pour le dashboard
    dashboard_data = {
        'last_update': datetime.now().isoformat(),
        'metrics': {
            'total_entreprises': stats['total_entreprises'],
            'taux_validite': stats['pourcentage_valides'],
            'taux_erreurs': 100 - stats['pourcentage_valides'],
            'champs_manquants_pct': stats['pourcentage_champs_manquants']
        },
        'status': 'success' if stats['pourcentage_valides'] >= 80 else 'warning'
    }
    
    dashboard_file = os.path.join(REPORTS_DIR, 'dashboard_metrics.json')
    with open(dashboard_file, 'w', encoding='utf-8') as f:
        json.dump(dashboard_data, f, ensure_ascii=False, indent=2)
    
    print(f"📊 Métriques dashboard sauvegardées: {dashboard_file}")
    print(f"✓ Taux de validité: {stats['pourcentage_valides']}%")
    print(f"✓ Taux d'erreurs: {100 - stats['pourcentage_valides']}%")
    
    return dashboard_data


def check_data_quality_threshold(**context):
    """Vérifie que le seuil de qualité est respecté."""
    stats = context['ti'].xcom_pull(task_ids='validate_quality', key='validation_stats')
    
    if not stats:
        raise ValueError("Aucune statistique de validation disponible")
    
    # Seuil de qualité : 80% de validité minimum
    quality_threshold = 80
    validity_rate = stats['pourcentage_valides']
    
    if validity_rate < quality_threshold:
        print(f"⚠️  ALERTE QUALITÉ: Taux de validité ({validity_rate}%) < seuil ({quality_threshold}%)")
        print(f"   Entreprises invalides: {stats['entreprises_invalides']}/{stats['total_entreprises']}")
        # Ne pas échouer la tâche, juste alerter
        return {
            'status': 'warning',
            'message': f"Qualité en dessous du seuil: {validity_rate}% < {quality_threshold}%"
        }
    else:
        print(f"✓ Qualité OK: {validity_rate}% >= {quality_threshold}%")
        return {
            'status': 'success',
            'message': f"Qualité satisfaisante: {validity_rate}%"
        }


# Configuration du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'kbo_data_quality_validation',
    default_args=default_args,
    description='Validation de la qualité des données extraites avec rapport détaillé',
    schedule='0 6 * * *',  # Tous les jours à 6h00 (après le scraping)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['kbo', 'quality', 'validation'],
)

# Tâche 1: Valider la qualité des données
validate_task = PythonOperator(
    task_id='validate_quality',
    python_callable=validate_data_quality,
    dag=dag,
)

# Tâche 2: Générer les données du dashboard
dashboard_task = PythonOperator(
    task_id='generate_dashboard',
    python_callable=generate_dashboard_data,
    dag=dag,
)

# Tâche 3: Vérifier le seuil de qualité
check_threshold_task = PythonOperator(
    task_id='check_threshold',
    python_callable=check_data_quality_threshold,
    dag=dag,
)

# Définir l'ordre des tâches
validate_task >> dashboard_task >> check_threshold_task
