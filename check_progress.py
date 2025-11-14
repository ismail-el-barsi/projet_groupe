"""
Script simple pour vérifier la progression du scraping
Usage: python check_progress.py
"""
import csv
import json
import os
from datetime import datetime


def main():
    """Affiche la progression du scraping"""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_file = os.path.join(base_dir, "data/enterprise.csv")
    html_dir = os.path.join(base_dir, "data/html_pages")
    progress_file = os.path.join(base_dir, "data/dag_progress.json")
    
    print("\n" + "="*70)
    print("📊 PROGRESSION DU SCRAPING KBO")
    print("="*70)
    print(f"⏰ Heure: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Compter les entreprises totales
    total_enterprises = 0
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for _ in reader:
                total_enterprises += 1
    except Exception as e:
        print(f"❌ Erreur lecture CSV: {e}")
        return
    
    # Compter les entreprises scrapées
    scraped_enterprises = 0
    if os.path.exists(html_dir):
        scraped_enterprises = len([f for f in os.listdir(html_dir) if f.endswith('.html')])
    
    # Calculer le reste
    remaining = total_enterprises - scraped_enterprises
    progress = (scraped_enterprises / total_enterprises * 100) if total_enterprises > 0 else 0
    
    # Afficher les résultats
    print(f"📁 Total entreprises    : {total_enterprises:,}")
    print(f"✅ Déjà scrapées        : {scraped_enterprises:,}")
    print(f"⏳ Restantes            : {remaining:,}")
    print(f"📈 Progression          : {progress:.2f}%")
    
    # Barre de progression
    bar_length = 50
    filled = int(bar_length * progress / 100)
    bar = "█" * filled + "░" * (bar_length - filled)
    print(f"\n[{bar}] {progress:.1f}%\n")
    
    # Info sur la progression de chaque DAG
    if os.path.exists(progress_file):
        try:
            with open(progress_file, 'r') as f:
                dag_progress = json.load(f)
                print("📋 Position de chaque DAG :")
                for dag_id, index in sorted(dag_progress.items()):
                    dag_num = dag_id.replace('kbo_scraping_dag_', '')
                    print(f"   DAG {dag_num:>2} : index {index:>5} / {total_enterprises}")
                print()
        except:
            pass
    
    # Estimation si on a des données
    if scraped_enterprises > 0 and remaining > 0:
        print("💡 Info:")
        print(f"   - Il reste {remaining:,} entreprises à scraper")
        print(f"   - 10 DAGs travaillent en continu (auto-relance)")
        print(f"   - Chaque DAG passe à la suivante dès qu'il termine")
    
    print("="*70 + "\n")
    
    # Afficher les fichiers récents
    if os.path.exists(html_dir):
        files = [(f, os.path.getmtime(os.path.join(html_dir, f))) 
                 for f in os.listdir(html_dir) if f.endswith('.html')]
        if files:
            files.sort(key=lambda x: x[1], reverse=True)
            recent = files[:5]
            
            print("🕐 Derniers fichiers scrapés:")
            for filename, mtime in recent:
                time_str = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')
                print(f"   - {filename} ({time_str})")
            print()


if __name__ == "__main__":
    main()
