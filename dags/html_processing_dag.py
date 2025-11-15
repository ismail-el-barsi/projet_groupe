from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import json
from pathlib import Path

# Ajouter le répertoire services au path
sys.path.insert(0, '/opt/airflow/services')

from html_parser import parse_html_file, save_to_json


# Chemins
HTML_DIR = '/opt/airflow/data/html_pages'
OUTPUT_DIR = '/opt/airflow/data/extracted_data'


def process_html_files(**context):
    """Traite tous les fichiers HTML et extrait les données."""
    # Créer le répertoire de sortie s'il n'existe pas
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    processed_files = []
    errors = []
    
    # Lister tous les fichiers HTML
    html_files = [f for f in os.listdir(HTML_DIR) if f.endswith('.html')]
    
    print(f"Nombre de fichiers HTML trouvés: {len(html_files)}")
    
    for html_file in html_files:
        try:
            html_path = os.path.join(HTML_DIR, html_file)
            
            # Extraire les données
            print(f"Traitement de {html_file}...")
            data = parse_html_file(html_path)
            
            # Créer le nom du fichier de sortie
            output_filename = html_file.replace('.html', '.json')
            output_path = os.path.join(OUTPUT_DIR, output_filename)
            
            # Sauvegarder en JSON
            save_to_json(data, output_path)
            
            processed_files.append({
                'file': html_file,
                'output': output_filename,
                'numero_entreprise': data.get('presentation', {}).get('numero_entreprise', 'N/A')
            })
            
            print(f"✓ {html_file} traité avec succès")
            
        except Exception as e:
            error_msg = f"Erreur lors du traitement de {html_file}: {str(e)}"
            print(error_msg)
            errors.append(error_msg)
    
    # Créer un rapport de traitement
    report = {
        'total_files': len(html_files),
        'processed': len(processed_files),
        'errors': len(errors),
        'processed_files': processed_files,
        'error_details': errors
    }
    
    # Sauvegarder le rapport
    report_path = os.path.join(OUTPUT_DIR, 'processing_report.json')
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'='*50}")
    print(f"RAPPORT DE TRAITEMENT")
    print(f"{'='*50}")
    print(f"Total de fichiers: {report['total_files']}")
    print(f"Traités avec succès: {report['processed']}")
    print(f"Erreurs: {report['errors']}")
    print(f"{'='*50}")
    
    # Pousser les statistiques dans XCom
    context['ti'].xcom_push(key='processing_stats', value=report)
    
    return report


def validate_extracted_data(**context):
    """Valide les données extraites."""
    report = context['ti'].xcom_pull(task_ids='process_html', key='processing_stats')
    
    if not report:
        raise ValueError("Aucun rapport de traitement trouvé")
    
    if report['errors'] > 0:
        print(f"⚠️  Attention: {report['errors']} fichier(s) n'ont pas pu être traités")
    
    if report['processed'] == 0:
        raise ValueError("Aucun fichier n'a été traité avec succès")
    
    print(f"✓ Validation réussie: {report['processed']} fichiers traités")
    
    return True


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
    'kbo_html_processing',
    default_args=default_args,
    description='Traite les fichiers HTML KBO pour extraire les données structurées',
    schedule=None,  # Manuel
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['kbo', 'html', 'extraction'],
)

# Tâche 1: Traiter les fichiers HTML
process_task = PythonOperator(
    task_id='process_html',
    python_callable=process_html_files,
    dag=dag,
)

# Tâche 2: Valider les données extraites
validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_extracted_data,
    dag=dag,
)

# Définir l'ordre des tâches
process_task >> validate_task
