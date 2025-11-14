"""
Gestionnaire d'état pour le scraping - permet de reprendre où on s'est arrêté
"""
import os
import json
import logging
from datetime import datetime
from threading import Lock

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ScrapingStateManager:
    """Gère l'état du scraping avec persistence sur disque"""
    
    def __init__(self, state_file="data/scraping_state.json"):
        self.state_file = state_file
        self.lock = Lock()
        self.state = self._load_state()
    
    def _load_state(self):
        """Charge l'état depuis le fichier"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Erreur lors du chargement de l'état: {e}")
                return self._create_empty_state()
        return self._create_empty_state()
    
    def _create_empty_state(self):
        """Crée un état vide"""
        return {
            'batches': {},
            'completed': [],
            'failed': [],
            'in_progress': [],
            'last_update': None
        }
    
    def _save_state(self):
        """Sauvegarde l'état sur disque"""
        try:
            os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
            self.state['last_update'] = datetime.now().isoformat()
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde de l'état: {e}")
    
    def get_batch_state(self, batch_id):
        """Récupère l'état d'un batch"""
        with self.lock:
            return self.state['batches'].get(batch_id, {
                'status': 'pending',
                'completed': [],
                'failed': [],
                'total': 0,
                'started_at': None,
                'finished_at': None
            })
    
    def create_batch(self, batch_id, enterprise_numbers):
        """Crée un nouveau batch"""
        with self.lock:
            self.state['batches'][batch_id] = {
                'status': 'pending',
                'enterprises': enterprise_numbers,
                'completed': [],
                'failed': [],
                'total': len(enterprise_numbers),
                'started_at': None,
                'finished_at': None
            }
            self._save_state()
            logger.info(f"Batch {batch_id} créé avec {len(enterprise_numbers)} entreprises")
    
    def start_batch(self, batch_id):
        """Marque un batch comme démarré"""
        with self.lock:
            if batch_id in self.state['batches']:
                self.state['batches'][batch_id]['status'] = 'in_progress'
                self.state['batches'][batch_id]['started_at'] = datetime.now().isoformat()
                self._save_state()
                logger.info(f"Batch {batch_id} démarré")
    
    def mark_enterprise_completed(self, batch_id, enterprise_number):
        """Marque une entreprise comme complétée"""
        with self.lock:
            if batch_id in self.state['batches']:
                batch = self.state['batches'][batch_id]
                if enterprise_number not in batch['completed']:
                    batch['completed'].append(enterprise_number)
                if enterprise_number in batch['failed']:
                    batch['failed'].remove(enterprise_number)
                self._save_state()
    
    def mark_enterprise_failed(self, batch_id, enterprise_number):
        """Marque une entreprise comme échouée"""
        with self.lock:
            if batch_id in self.state['batches']:
                batch = self.state['batches'][batch_id]
                if enterprise_number not in batch['failed']:
                    batch['failed'].append(enterprise_number)
                self._save_state()
    
    def finish_batch(self, batch_id):
        """Marque un batch comme terminé"""
        with self.lock:
            if batch_id in self.state['batches']:
                batch = self.state['batches'][batch_id]
                batch['status'] = 'completed'
                batch['finished_at'] = datetime.now().isoformat()
                self._save_state()
                logger.info(f"Batch {batch_id} terminé: {len(batch['completed'])}/{batch['total']} réussis")
    
    def get_remaining_enterprises(self, batch_id):
        """Retourne les entreprises restantes à scraper pour un batch"""
        with self.lock:
            if batch_id not in self.state['batches']:
                return []
            
            batch = self.state['batches'][batch_id]
            completed = set(batch['completed'])
            all_enterprises = batch['enterprises']
            
            return [e for e in all_enterprises if e not in completed]
    
    def get_global_stats(self):
        """Retourne des statistiques globales"""
        with self.lock:
            total_batches = len(self.state['batches'])
            completed_batches = sum(1 for b in self.state['batches'].values() if b['status'] == 'completed')
            in_progress_batches = sum(1 for b in self.state['batches'].values() if b['status'] == 'in_progress')
            
            total_enterprises = sum(b['total'] for b in self.state['batches'].values())
            completed_enterprises = sum(len(b['completed']) for b in self.state['batches'].values())
            failed_enterprises = sum(len(b['failed']) for b in self.state['batches'].values())
            
            return {
                'total_batches': total_batches,
                'completed_batches': completed_batches,
                'in_progress_batches': in_progress_batches,
                'total_enterprises': total_enterprises,
                'completed_enterprises': completed_enterprises,
                'failed_enterprises': failed_enterprises,
                'progress_pct': (completed_enterprises / total_enterprises * 100) if total_enterprises > 0 else 0
            }
    
    def reset_failed_enterprises(self, batch_id):
        """Réinitialise les entreprises échouées pour réessayer"""
        with self.lock:
            if batch_id in self.state['batches']:
                self.state['batches'][batch_id]['failed'] = []
                self._save_state()
                logger.info(f"Entreprises échouées du batch {batch_id} réinitialisées")


if __name__ == "__main__":
    # Test du gestionnaire d'état
    manager = ScrapingStateManager("test_state.json")
    
    # Créer des batches
    manager.create_batch("batch_0", ["0200.001.001", "0200.001.002", "0200.001.003"])
    manager.create_batch("batch_1", ["0200.002.001", "0200.002.002"])
    
    # Simuler le traitement
    manager.start_batch("batch_0")
    manager.mark_enterprise_completed("batch_0", "0200.001.001")
    manager.mark_enterprise_completed("batch_0", "0200.001.002")
    manager.mark_enterprise_failed("batch_0", "0200.001.003")
    manager.finish_batch("batch_0")
    
    # Afficher les stats
    print("Stats globales:", manager.get_global_stats())
    print("Entreprises restantes batch_0:", manager.get_remaining_enterprises("batch_0"))
