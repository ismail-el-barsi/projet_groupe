import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ajouter le répertoire services au path
sys.path.insert(0, '/opt/airflow/services')

from dashboard_collector import DashboardCollector
from data_validator import DataValidator, generate_validation_summary

# Chemins
DATA_DIR = '/opt/airflow/data'


def validate_data_quality(**context):
    """Tâche principale de validation de la qualité des données depuis PostgreSQL."""
    # Initialiser le collecteur pour accéder à la BDD
    collector = DashboardCollector(DATA_DIR)
    
    # Récupérer toutes les entreprises depuis la BDD
    print("📊 Récupération des entreprises depuis PostgreSQL...")
    session = collector.Session()
    try:
        from dashboard_collector import Entreprise
        entreprises_db = session.query(Entreprise).all()
        
        print(f"📁 Nombre d'entreprises à valider: {len(entreprises_db)}")
        
        if len(entreprises_db) == 0:
            print("⚠️  Aucune entreprise trouvée dans la BDD")
            return {
                'status': 'no_data',
                'message': 'Aucune entreprise à valider'
            }
        
        # Convertir les données JSONB en format compatible avec le validateur
        # Le validateur attend des données au format du parser HTML
        validation_data = []
        for entreprise in entreprises_db:
            # entreprise.data contient déjà le dict complet au format attendu
            validation_data.append({
                'numero_entreprise': entreprise.numero_entreprise,
                'data': entreprise.data  # JSONB déjà parsé en dict Python
            })
        
    finally:
        session.close()
    
    # Initialiser le validateur
    validator = DataValidator()
    
    # Valider toutes les entreprises
    print("🔍 Démarrage de la validation...")
    
    # Adapter la validation pour les données en mémoire
    results = []
    total_files = len(validation_data)
    valid_count = 0
    error_types = {}
    error_locations = {}
    
    for item in validation_data:
        data = item['data']
        numero = item['numero_entreprise']
        
        try:
            validation = validator.validate_entity(data)
            results.append(validation)
            
            if validation['valide']:
                valid_count += 1
            
            # Compter les types d'erreurs et enregistrer les entreprises affectées
            for error in validation['erreurs']:
                error_types[error] = error_types.get(error, 0) + 1
                error_locations.setdefault(error, set()).add(numero)
                
        except Exception as e:
            err_msg = f'erreur_validation: {str(e)}'
            results.append({
                'entreprise': numero,
                'valide': False,
                'erreurs': [err_msg],
                'validation_date': datetime.now().isoformat()
            })
            error_types[err_msg] = error_types.get(err_msg, 0) + 1
            error_locations.setdefault(err_msg, set()).add(numero)
    
    # Calculer les statistiques
    invalid_count = total_files - valid_count
    valid_percentage = (valid_count / total_files * 100) if total_files > 0 else 0
    
    # Calculer les champs manquants
    missing_fields = {k: v for k, v in error_types.items() if 'manquant' in k}
    format_errors = {k: v for k, v in error_types.items() if 'format_invalide' in k or 'type_invalide' in k}
    
    missing_fields_percentage = sum(missing_fields.values()) / (total_files * len(validator.validation_rules['presentation'])) * 100 if total_files > 0 else 0
    
    report = {
        'date_validation': datetime.now().isoformat(),
        'source': 'PostgreSQL (table entreprises)',
        'statistiques': {
            'total_entreprises': total_files,
            'entreprises_valides': valid_count,
            'entreprises_invalides': invalid_count,
            'pourcentage_valides': round(valid_percentage, 2),
            'pourcentage_champs_manquants': round(missing_fields_percentage, 2)
        },
        'repartition_erreurs': error_types,
        'erreurs_localisation': {k: sorted(list(v)) for k, v in error_locations.items()},
        'champs_manquants': missing_fields,
        'erreurs_format': format_errors,
        'details_validations': results
    }
    
    # Persister le rapport de validation en base
    try:
        collector.insert_validation_report(report)
    except Exception as e:
        print(f"⚠️  Impossible d'insérer le rapport de validation en BDD: {e}")

    # Générer le résumé textuel
    summary = generate_validation_summary(report)
    print(summary)
    
    # Afficher résumé dans les logs
    print(f"📄 Rapport de validation généré")
    
    # Pousser les statistiques dans XCom
    context['ti'].xcom_push(key='validation_stats', value=report['statistiques'])
    
    return report['statistiques']


def generate_dashboard_data(**context):
    """Génère les données pour le dashboard admin."""
    stats = context['ti'].xcom_pull(task_ids='validate_quality', key='validation_stats')
    
    if not stats:
        print("⚠️  Aucune statistique disponible")
        return
    
    # Métriques dashboard (déjà disponibles via PostgreSQL)
    dashboard_data = {
        'last_update': datetime.now().isoformat(),
        'source': 'PostgreSQL',
        'metrics': {
            'total_entreprises': stats['total_entreprises'],
            'taux_validite': stats['pourcentage_valides'],
            'taux_erreurs': 100 - stats['pourcentage_valides'],
            'champs_manquants_pct': stats['pourcentage_champs_manquants']
        },
        'status': 'success' if stats['pourcentage_valides'] >= 80 else 'warning'
    }
    
    # Persister les métriques récentes dans la BDD
    try:
        collector = DashboardCollector(DATA_DIR)
        collector.upsert_dashboard_metrics('latest_validation', dashboard_data)
    except Exception as e:
        print(f"⚠️  Impossible d'enregistrer les métriques du dashboard en BDD: {e}")

    print(f"📊 Métriques dashboard disponibles via PostgreSQL")
    print(f"✓ Source: PostgreSQL (table entreprises)")
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
