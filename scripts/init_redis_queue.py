"""
Script d'initialisation de la queue Redis
Charge toutes les entreprises du CSV dans Redis avec priorité normale
"""
import csv
import os
import sys

# Ajouter le chemin des services
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
services_dir = os.path.join(parent_dir, 'services')

sys.path.insert(0, services_dir)

from queue_manager import QueueManager


def load_enterprises_from_csv(csv_file):
    """Charge les entreprises depuis le CSV"""
    enterprises = []
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                enterprise_number = (
                    row.get('EnterpriseNumber') or 
                    row.get('enterprise_number') or 
                    row.get('number')
                )
                if enterprise_number:
                    enterprises.append(enterprise_number)
    except Exception as e:
        print(f"❌ Erreur lecture CSV: {e}")
        return []
    
    return enterprises


def init_redis_queue():
    """Initialise la queue Redis avec toutes les entreprises NON SCRAPÉES"""
    print("\n" + "="*70)
    print("🚀 INITIALISATION DE LA QUEUE REDIS")
    print("="*70)
    
    # Charger les entreprises
    csv_file = os.path.join(parent_dir, "data/enterprise.csv")
    enterprises = load_enterprises_from_csv(csv_file)
    
    print(f"\n📊 {len(enterprises):,} entreprises trouvées dans le CSV")
    
    if not enterprises:
        print("❌ Aucune entreprise à charger")
        return
    
    # Initialiser le gestionnaire
    queue_manager = QueueManager()
    
    # Vérifier les stats actuelles
    stats = queue_manager.get_queue_stats()
    print(f"\n📈 Stats Redis actuelles:")
    print(f"   - En attente: {stats['total_pending']}")
    print(f"   - En cours: {stats['total_processing']}")
    print(f"   - Complétés: {stats['total_completed']}")
    print(f"   - Échoués: {stats['total_failed']}")
    
    # Nettoyer les processing bloqués
    print(f"\n♻️  Nettoyage des entreprises bloquées en 'processing'...")
    recovery_result = queue_manager.clear_processing()
    if recovery_result['success']:
        print(f"   → {recovery_result['count']} entreprises remises en queue")
    
    # OPTIMISATION: Lister les fichiers HTML existants (plus rapide que 1.9M checks)
    html_dir = os.path.join(parent_dir, "data/html_pages")
    print(f"\n🔍 Lecture des fichiers HTML existants...")
    
    scraped_enterprises = set()
    if os.path.exists(html_dir):
        for filename in os.listdir(html_dir):
            if filename.endswith('.html'):
                enterprise = filename.replace('.html', '')
                scraped_enterprises.add(enterprise)
    
    print(f"   ✅ {len(scraped_enterprises):,} entreprises déjà scrapées trouvées")
    
    # Marquer les entreprises scrapées comme complétées
    print(f"\n✓ Marquage des entreprises scrapées comme complétées dans Redis...")
    for enterprise in scraped_enterprises:
        queue_manager.mark_as_completed(enterprise)
    
    # Convertir enterprises en set pour filtrage rapide
    all_enterprises = set(enterprises)
    to_scrape = all_enterprises - scraped_enterprises
    
    print(f"   ⏳ {len(to_scrape):,} entreprises à ajouter à la queue")
    
    # Ajouter SEULEMENT un nombre limité à la queue (les DAGs rechargeront automatiquement)
    MAX_QUEUE_SIZE = 1000  # Limite pour ne pas saturer Redis
    
    print(f"\n📥 Ajout des premières entreprises à la queue Redis (max {MAX_QUEUE_SIZE})...")
    print(f"   Note: Les DAGs rechargeront automatiquement quand la queue sera basse")
    
    added_count = 0
    already_count = 0
    
    # Convertir en liste pour limiter
    to_scrape_list = list(to_scrape)[:MAX_QUEUE_SIZE]
    
    for i, enterprise in enumerate(to_scrape_list, 1):
        result = queue_manager.add_to_queue(
            enterprise_number=enterprise,
            priority=1,  # Priorité normale
            requested_by='init_script'
        )
        
        if result['success']:
            action = result.get('action', 'unknown')
            if action == 'added':
                added_count += 1
            elif action in ['already_queued', 'already_completed', 'already_processing']:
                already_count += 1
        
        # Afficher progression
        if i % 100 == 0:
            print(f"   → {i:,}/{len(to_scrape_list):,} traités...")
    
    print(f"\n✅ Traitement terminé:")
    print(f"   - Déjà scrapées (marquées complétées): {len(scraped_enterprises):,}")
    print(f"   - Total restant à scraper: {len(to_scrape):,}")
    print(f"   - Ajoutées à la queue: {added_count:,}")
    print(f"   - Déjà présentes: {already_count:,}")
    print(f"\n💡 Les DAGs rechargeront automatiquement le CSV quand la queue < 100")
    
    # Stats finales
    stats = queue_manager.get_queue_stats()
    print(f"\n📈 Stats Redis finales:")
    print(f"   - En attente: {stats['total_pending']:,}")
    print(f"   - En cours: {stats['total_processing']}")
    print(f"   - Complétés: {stats['total_completed']:,}")
    print(f"   - Échoués: {stats['total_failed']}")
    print(f"   - Haute priorité: {stats['high_priority']}")
    
    print("\n" + "="*70)
    print("✅ INITIALISATION TERMINÉE")
    print("="*70 + "\n")


if __name__ == "__main__":
    init_redis_queue()
