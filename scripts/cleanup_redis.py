#!/usr/bin/env python3
"""
Script pour nettoyer et réinitialiser Redis avec uniquement les entreprises non scrapées
"""
import os
import sys

# Ajouter le répertoire parent au path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(parent_dir, 'services'))

from queue_manager import QueueManager

def cleanup_redis():
    """Nettoie complètement Redis"""
    print("\n" + "="*70)
    print("🧹 NETTOYAGE COMPLET DE REDIS")
    print("="*70)
    
    queue_manager = QueueManager()
    
    # Vérifier les stats avant
    stats_before = queue_manager.get_queue_stats()
    print(f"\n📊 Stats AVANT nettoyage:")
    print(f"   - En attente: {stats_before['total_pending']:,}")
    print(f"   - En cours: {stats_before['total_processing']}")
    print(f"   - Complétés: {stats_before['total_completed']:,}")
    print(f"   - Échoués: {stats_before['total_failed']}")
    
    # Demander confirmation
    print(f"\n⚠️  ATTENTION: Cette opération va SUPPRIMER toutes les données Redis!")
    print(f"   Cela inclut:")
    print(f"   - {stats_before['total_pending']:,} entreprises en attente")
    print(f"   - {stats_before['total_processing']} entreprises en cours")
    print(f"   - {stats_before['total_completed']:,} entreprises complétées")
    print(f"   - {stats_before['total_failed']} entreprises échouées")
    
    response = input("\nContinuer? (oui/non): ").strip().lower()
    
    if response not in ['oui', 'yes', 'y']:
        print("\n❌ Nettoyage annulé")
        return False
    
    # Nettoyer toutes les clés
    print(f"\n🗑️  Suppression de toutes les clés Redis...")
    
    # Nettoyer chaque set Redis
    queue_manager.redis.delete(queue_manager.PENDING_KEY)
    queue_manager.redis.delete(queue_manager.PROCESSING_KEY)
    queue_manager.redis.delete(queue_manager.COMPLETED_KEY)
    queue_manager.redis.delete(queue_manager.FAILED_KEY)
    
    # Nettoyer les métadonnées
    keys = queue_manager.redis.keys(f"{queue_manager.METADATA_PREFIX}*")
    if keys:
        queue_manager.redis.delete(*keys)
        print(f"   → {len(keys):,} métadonnées supprimées")
    
    # Vérifier les stats après
    stats_after = queue_manager.get_queue_stats()
    print(f"\n✅ Redis nettoyé avec succès!")
    print(f"\n📊 Stats APRÈS nettoyage:")
    print(f"   - En attente: {stats_after['total_pending']}")
    print(f"   - En cours: {stats_after['total_processing']}")
    print(f"   - Complétés: {stats_after['total_completed']}")
    print(f"   - Échoués: {stats_after['total_failed']}")
    
    return True


if __name__ == '__main__':
    if cleanup_redis():
        print("\n" + "="*70)
        print("✅ REDIS NETTOYÉ - Vous pouvez maintenant lancer init_redis_queue.py")
        print("="*70 + "\n")
    else:
        print("\n" + "="*70)
        print("❌ OPÉRATION ANNULÉE")
        print("="*70 + "\n")
