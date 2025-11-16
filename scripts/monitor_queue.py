#!/usr/bin/env python3
"""
Script pour monitorer la queue Redis en temps réel
"""
import os
import sys
import time
from datetime import datetime

# Ajouter le répertoire parent au path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(parent_dir, 'services'))

from queue_manager import QueueManager

def monitor_queue(interval=5):
    """Monitore la queue Redis et affiche les stats"""
    queue_manager = QueueManager()
    
    print("\n" + "="*70)
    print("📊 MONITEUR DE QUEUE REDIS")
    print("="*70)
    print(f"Rafraîchissement toutes les {interval} secondes")
    print("Appuyez sur Ctrl+C pour arrêter\n")
    
    last_stats = None
    
    try:
        while True:
            stats = queue_manager.get_queue_stats()
            now = datetime.now().strftime("%H:%M:%S")
            
            # Calculer les changements
            if last_stats:
                pending_diff = stats['total_pending'] - last_stats['total_pending']
                completed_diff = stats['total_completed'] - last_stats['total_completed']
                failed_diff = stats['total_failed'] - last_stats['total_failed']
                
                pending_arrow = "📉" if pending_diff < 0 else "📈" if pending_diff > 0 else "➡️"
                completed_arrow = "🟢" if completed_diff > 0 else "➡️"
                failed_arrow = "🔴" if failed_diff > 0 else "➡️"
            else:
                pending_arrow = completed_arrow = failed_arrow = "➡️"
                pending_diff = completed_diff = failed_diff = 0
            
            # Afficher stats
            print(f"[{now}] "
                  f"En attente: {stats['total_pending']:,} {pending_arrow} ({pending_diff:+d}) | "
                  f"En cours: {stats['total_processing']} | "
                  f"Complétés: {stats['total_completed']:,} {completed_arrow} ({completed_diff:+d}) | "
                  f"Échoués: {stats['total_failed']} {failed_arrow} ({failed_diff:+d}) | "
                  f"Haute priorité: {stats['high_priority']}")
            
            last_stats = stats
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\n\n✅ Monitoring arrêté")
        print("\n" + "="*70)
        print("📊 STATS FINALES:")
        print(f"   - En attente: {stats['total_pending']:,}")
        print(f"   - En cours: {stats['total_processing']}")
        print(f"   - Complétés: {stats['total_completed']:,}")
        print(f"   - Échoués: {stats['total_failed']}")
        print("="*70 + "\n")


if __name__ == '__main__':
    # Intervalle par défaut: 5 secondes
    interval = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    monitor_queue(interval)
