"""
Gestionnaire de file d'attente avec Redis et priorités
Utilise des sorted sets Redis pour gérer les priorités dynamiques
"""
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QueueManager:
    """Gestionnaire de file d'attente avec priorités via Redis"""
    
    def __init__(self, redis_url=None):
        """
        Initialise le gestionnaire de queue Redis
        
        Args:
            redis_url: URL de connexion Redis
        """
        # Détecter si on est en local ou dans Docker
        default_redis = os.getenv("REDIS_URL", None)
        if not default_redis:
            # Essayer de détecter l'environnement
            try:
                import socket
                # Si on peut résoudre 'redis', on est probablement dans Docker
                socket.gethostbyname('redis')
                default_redis = "redis://redis:6379/0"
            except:
                # Sinon, on est en local
                default_redis = "redis://localhost:6379/0"
        
        self.redis_url = redis_url or default_redis
        try:
            self.redis = redis.Redis.from_url(self.redis_url, decode_responses=True, socket_connect_timeout=2)
            # Test de connexion
            self.redis.ping()
            logger.info(f"✓ Connecté à Redis: {self.redis_url}")
        except Exception as e:
            logger.error(f"❌ Impossible de se connecter à Redis ({self.redis_url}): {e}")
            logger.warning("⚠️  Mode dégradé: Queue Redis non disponible")
            self.redis = None
        
        # Clés Redis
        self.QUEUE_KEY = "scraping:queue"  # Sorted set: score = priorité + timestamp
        self.PROCESSING_KEY = "scraping:processing"  # Set des entreprises en cours
        self.COMPLETED_KEY = "scraping:completed"  # Set des entreprises terminées
        self.FAILED_KEY = "scraping:failed"  # Hash des échecs avec détails
        self.METADATA_KEY = "scraping:metadata"  # Hash des métadonnées par entreprise
        
    def add_to_queue(self, enterprise_number: str, priority: int = 1, requested_by: str = 'system') -> Dict:
        """
        Ajoute une entreprise à la file d'attente
        
        Args:
            enterprise_number: Numéro d'entreprise (format: 0201.543.234)
            priority: 1=normal, 2=haute (recherche manuelle), 3=très haute
            requested_by: Source de la demande (system, user, manual)
        
        Returns:
            dict: Résultat de l'ajout
        """
        if not self.redis:
            logger.warning("Redis non disponible, impossible d'ajouter à la queue")
            return {'success': False, 'error': 'redis_unavailable'}
        
        try:
            # Vérifier si déjà complété
            if self.redis.sismember(self.COMPLETED_KEY, enterprise_number):
                return {
                    'success': True,
                    'action': 'already_completed',
                    'enterprise_number': enterprise_number
                }
            
            # Vérifier si en cours de traitement
            if self.redis.sismember(self.PROCESSING_KEY, enterprise_number):
                return {
                    'success': True,
                    'action': 'already_processing',
                    'enterprise_number': enterprise_number
                }
            
            # Calculer le score (priorité inversée car score bas = haute priorité)
            # Score = -priorité * 1000000 + timestamp
            # Ainsi: priorité 3 = -3000000, priorité 2 = -2000000, priorité 1 = -1000000
            timestamp = datetime.now().timestamp()
            score = -(priority * 1000000) + timestamp
            
            # Vérifier si déjà dans la queue
            current_score = self.redis.zscore(self.QUEUE_KEY, enterprise_number)
            
            if current_score is not None:
                # Si nouvelle priorité est plus haute (score plus bas), mettre à jour
                if score < current_score:
                    self.redis.zadd(self.QUEUE_KEY, {enterprise_number: score})
                    # Mettre à jour les métadonnées
                    self._update_metadata(enterprise_number, priority, requested_by)
                    logger.info(f"✓ Priorité augmentée: {enterprise_number} (priorité: {priority})")
                    return {
                        'success': True,
                        'action': 'priority_updated',
                        'enterprise_number': enterprise_number,
                        'priority': priority
                    }
                else:
                    return {
                        'success': True,
                        'action': 'already_queued',
                        'enterprise_number': enterprise_number
                    }
            
            # Ajouter à la queue
            self.redis.zadd(self.QUEUE_KEY, {enterprise_number: score})
            
            # Sauvegarder les métadonnées
            self._update_metadata(enterprise_number, priority, requested_by)
            
            logger.info(f"✓ Ajouté à la queue: {enterprise_number} (priorité: {priority}, score: {score:.2f})")
            
            return {
                'success': True,
                'action': 'added',
                'enterprise_number': enterprise_number,
                'priority': priority
            }
            
        except Exception as e:
            logger.exception(f"Erreur ajout à la queue: {e}")
            return {'success': False, 'error': str(e)}
    
    def _update_metadata(self, enterprise_number: str, priority: int, requested_by: str):
        """Sauvegarde les métadonnées d'une entreprise"""
        metadata = {
            'priority': priority,
            'requested_by': requested_by,
            'added_at': datetime.now().isoformat(),
            'attempts': 0,
            'assigned_dag': None,  # Quel DAG va traiter cette entreprise
            'assigned_at': None
        }
        self.redis.hset(
            f"{self.METADATA_KEY}:{enterprise_number}",
            mapping={k: json.dumps(v) if isinstance(v, (dict, list)) else str(v) for k, v in metadata.items()}
        )
    
    def get_next_to_scrape(self, count: int = 1, dag_id: str = None) -> List[str]:
        """
        Récupère les prochaines entreprises à scraper (par ordre de priorité)
        
        Args:
            count: Nombre d'entreprises à récupérer
            dag_id: ID du DAG qui récupère les entreprises
        
        Returns:
            list: Liste des numéros d'entreprises
        """
        if not self.redis:
            return []
        
        try:
            # Récupérer les N premiers éléments (score le plus bas = priorité la plus haute)
            enterprises = self.redis.zrange(self.QUEUE_KEY, 0, count - 1)
            
            if enterprises:
                # Marquer comme en traitement et assigner le DAG
                for enterprise in enterprises:
                    self._mark_as_processing(enterprise, dag_id)
                
                if dag_id:
                    logger.info(f"🎯 {dag_id} → {len(enterprises)} entreprise(s) assignées: {enterprises}")
                else:
                    logger.info(f"→ {len(enterprises)} entreprise(s) à scraper: {enterprises}")
            
            return list(enterprises)
            
        except Exception as e:
            logger.exception(f"Erreur get_next_to_scrape: {e}")
            return []
    
    def _mark_as_processing(self, enterprise_number: str, dag_id: str = None):
        """Marque une entreprise comme en cours de traitement et assigne le DAG"""
        # Retirer de la queue
        self.redis.zrem(self.QUEUE_KEY, enterprise_number)
        # Ajouter au set de processing
        self.redis.sadd(self.PROCESSING_KEY, enterprise_number)
        # Incrémenter le compteur de tentatives
        metadata_key = f"{self.METADATA_KEY}:{enterprise_number}"
        self.redis.hincrby(metadata_key, 'attempts', 1)
        
        # Assigner le DAG et le timestamp
        if dag_id:
            self.redis.hset(metadata_key, 'assigned_dag', dag_id)
            self.redis.hset(metadata_key, 'assigned_at', datetime.now().isoformat())
            logger.info(f"✓ {enterprise_number} → Assigné à {dag_id}")
        else:
            logger.debug(f"→ En traitement: {enterprise_number}")
    
    def mark_as_completed(self, enterprise_number: str):
        """Marque une entreprise comme terminée avec succès"""
        try:
            # Retirer de processing
            self.redis.srem(self.PROCESSING_KEY, enterprise_number)
            # Ajouter aux complétés
            self.redis.sadd(self.COMPLETED_KEY, enterprise_number)
            # Supprimer des échecs si présent
            self.redis.hdel(self.FAILED_KEY, enterprise_number)
            
            logger.info(f"✓ Complété: {enterprise_number}")
            
            return {'success': True, 'enterprise_number': enterprise_number}
            
        except Exception as e:
            logger.exception(f"Erreur mark_as_completed: {e}")
            return {'success': False, 'error': str(e)}
    
    def mark_as_failed(self, enterprise_number: str, error_type: str = 'unknown', error_msg: str = ''):
        """
        Marque une entreprise comme échouée
        
        Args:
            enterprise_number: Numéro d'entreprise
            error_type: Type d'erreur (timeout, ip_blocked, etc.)
            error_msg: Message d'erreur
        """
        try:
            # Retirer de processing
            self.redis.srem(self.PROCESSING_KEY, enterprise_number)
            
            # Récupérer les métadonnées
            metadata_key = f"{self.METADATA_KEY}:{enterprise_number}"
            attempts = int(self.redis.hget(metadata_key, 'attempts') or 0)
            
            # Si moins de 3 tentatives, remettre en queue avec priorité basse
            if attempts < 3:
                # Remettre en queue avec priorité 1 (normale)
                self.add_to_queue(enterprise_number, priority=1, requested_by='retry')
                logger.warning(f"⚠ Échec {enterprise_number} (tentative {attempts}/3) - remis en queue")
            else:
                # Trop de tentatives, marquer comme échec définitif
                failure_data = {
                    'error_type': error_type,
                    'error_msg': error_msg,
                    'attempts': attempts,
                    'failed_at': datetime.now().isoformat()
                }
                self.redis.hset(self.FAILED_KEY, enterprise_number, json.dumps(failure_data))
                logger.error(f"❌ Échec définitif: {enterprise_number} (3 tentatives)")
            
            return {'success': True, 'enterprise_number': enterprise_number}
            
        except Exception as e:
            logger.exception(f"Erreur mark_as_failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def get_queue_stats(self) -> Dict:
        """Récupère les statistiques de la file d'attente"""
        if not self.redis:
            return {
                'total_pending': 0,
                'total_processing': 0,
                'total_completed': 0,
                'total_failed': 0,
                'high_priority': 0,
                'total_queue': 0
            }
        
        try:
            total_pending = self.redis.zcard(self.QUEUE_KEY)
            total_processing = self.redis.scard(self.PROCESSING_KEY)
            total_completed = self.redis.scard(self.COMPLETED_KEY)
            total_failed = self.redis.hlen(self.FAILED_KEY)
            
            # Compter les éléments haute priorité (score < -1500000)
            high_priority = self.redis.zcount(self.QUEUE_KEY, '-inf', -1500000)
            
            return {
                'total_pending': total_pending,
                'total_processing': total_processing,
                'total_completed': total_completed,
                'total_failed': total_failed,
                'high_priority': high_priority,
                'total_queue': total_pending + total_processing
            }
            
        except Exception as e:
            logger.exception(f"Erreur get_queue_stats: {e}")
            return {
                'total_pending': 0,
                'total_processing': 0,
                'total_completed': 0,
                'total_failed': 0,
                'high_priority': 0,
                'total_queue': 0
            }
    
    def get_queue_items(self, status: str = 'pending', limit: int = 100, offset: int = 0) -> List[Dict]:
        """
        Récupère les éléments de la file d'attente
        
        Args:
            status: pending, processing, completed, failed
            limit: Nombre max de résultats
            offset: Décalage
        
        Returns:
            list: Liste des items avec métadonnées
        """
        if not self.redis:
            return []
        
        try:
            items = []
            
            if status == 'pending':
                # Récupérer depuis sorted set avec scores
                entries = self.redis.zrange(self.QUEUE_KEY, offset, offset + limit - 1, withscores=True)
                
                for enterprise_number, score in entries:
                    # Calculer la priorité depuis le score
                    priority = int(-score // 1000000)
                    
                    # Récupérer les métadonnées
                    metadata = self._get_metadata(enterprise_number)
                    
                    items.append({
                        'enterprise_number': enterprise_number,
                        'priority': priority,
                        'priority_label': self._get_priority_label(priority),
                        'status': 'pending',
                        'score': score,
                        **metadata
                    })
            
            elif status == 'processing':
                enterprises = list(self.redis.smembers(self.PROCESSING_KEY))[offset:offset + limit]
                for enterprise_number in enterprises:
                    metadata = self._get_metadata(enterprise_number)
                    priority = metadata.get('priority', 1)
                    if isinstance(priority, str):
                        try:
                            priority = int(priority)
                        except:
                            priority = 1
                    items.append({
                        'enterprise_number': enterprise_number,
                        'status': 'processing',
                        'priority': priority,
                        'priority_label': self._get_priority_label(priority),
                        **metadata
                    })
            
            elif status == 'completed':
                enterprises = list(self.redis.smembers(self.COMPLETED_KEY))[offset:offset + limit]
                for enterprise_number in enterprises:
                    items.append({
                        'enterprise_number': enterprise_number,
                        'status': 'completed'
                    })
            
            elif status == 'failed':
                failed_data = self.redis.hgetall(self.FAILED_KEY)
                for enterprise_number, data_json in list(failed_data.items())[offset:offset + limit]:
                    data = json.loads(data_json)
                    items.append({
                        'enterprise_number': enterprise_number,
                        'status': 'failed',
                        **data
                    })
            
            return items
            
        except Exception as e:
            logger.exception(f"Erreur get_queue_items: {e}")
            return []
    
    def _get_metadata(self, enterprise_number: str) -> Dict:
        """Récupère les métadonnées d'une entreprise"""
        try:
            metadata_key = f"{self.METADATA_KEY}:{enterprise_number}"
            data = self.redis.hgetall(metadata_key)
            
            if not data:
                return {}
            
            # Convertir les valeurs
            result = {}
            for key, value in data.items():
                try:
                    # Essayer de parser comme JSON
                    result[key] = json.loads(value)
                except:
                    # Sinon garder comme string
                    result[key] = value
            
            return result
            
        except Exception as e:
            logger.debug(f"Pas de métadonnées pour {enterprise_number}: {e}")
            return {}
    
    def _get_priority_label(self, priority: int) -> str:
        """Retourne le label de priorité"""
        if priority >= 3:
            return 'Très haute'
        elif priority == 2:
            return 'Haute'
        else:
            return 'Normale'
    
    def get_item_metadata(self, enterprise_number: str) -> Dict:
        """
        Récupère les métadonnées publiques d'une entreprise en queue
        Utilisé par l'API pour afficher les infos de queue
        """
        if not self.redis:
            return None
        
        try:
            # Vérifier si l'entreprise est en queue ou en processing
            in_queue = self.redis.zscore(self.QUEUE_KEY, enterprise_number) is not None
            in_processing = self.redis.sismember(self.PROCESSING_KEY, enterprise_number)
            
            if not in_queue and not in_processing:
                return None  # Pas en queue
            
            # Récupérer les métadonnées
            metadata = self._get_metadata(enterprise_number)
            
            if in_processing:
                metadata['status'] = 'processing'
            else:
                metadata['status'] = 'pending'
            
            return metadata
            
        except Exception as e:
            logger.debug(f"Erreur get_item_metadata pour {enterprise_number}: {e}")
            return None
    
    def remove_from_queue(self, enterprise_number: str) -> Dict:
        """Supprime une entreprise de la file d'attente"""
        if not self.redis:
            return {'success': False, 'error': 'redis_unavailable'}
        
        try:
            # Retirer de tous les ensembles
            removed = 0
            removed += self.redis.zrem(self.QUEUE_KEY, enterprise_number)
            removed += self.redis.srem(self.PROCESSING_KEY, enterprise_number)
            
            # Supprimer les métadonnées
            self.redis.delete(f"{self.METADATA_KEY}:{enterprise_number}")
            
            if removed > 0:
                logger.info(f"✓ Retiré de la queue: {enterprise_number}")
                return {'success': True, 'enterprise_number': enterprise_number}
            else:
                return {'success': False, 'error': 'not_found'}
            
        except Exception as e:
            logger.exception(f"Erreur remove_from_queue: {e}")
            return {'success': False, 'error': str(e)}
    
    def clear_processing(self):
        """Nettoie les entreprises bloquées en 'processing' (utile au redémarrage)"""
        try:
            processing = self.redis.smembers(self.PROCESSING_KEY)
            count = 0
            
            for enterprise in processing:
                # Remettre en queue avec priorité normale
                self.redis.srem(self.PROCESSING_KEY, enterprise)
                self.add_to_queue(enterprise, priority=1, requested_by='recovery')
                count += 1
            
            if count > 0:
                logger.info(f"♻️  {count} entreprise(s) remises en queue depuis processing")
            
            return {'success': True, 'count': count}
            
        except Exception as e:
            logger.exception(f"Erreur clear_processing: {e}")
            return {'success': False, 'error': str(e)}
    
    def get_enterprises_by_dag(self) -> Dict[str, List[str]]:
        """Retourne les entreprises groupées par DAG qui les traite"""
        if not self.redis:
            return {}
        
        try:
            dag_assignments = {}
            
            # Parcourir toutes les entreprises en processing
            processing = self.redis.smembers(self.PROCESSING_KEY)
            
            for enterprise in processing:
                metadata = self._get_metadata(enterprise)
                dag_id = metadata.get('assigned_dag')
                
                if dag_id:
                    if dag_id not in dag_assignments:
                        dag_assignments[dag_id] = []
                    dag_assignments[dag_id].append(enterprise)
            
            return dag_assignments
            
        except Exception as e:
            logger.exception(f"Erreur get_enterprises_by_dag: {e}")
            return {}


if __name__ == "__main__":
    # Test du gestionnaire
    qm = QueueManager()
    
    # Stats
    print("Stats initiales:", qm.get_queue_stats())
    
    # Ajouter des entreprises
    qm.add_to_queue("0201.543.234", priority=1, requested_by='system')
    qm.add_to_queue("0200.065.765", priority=2, requested_by='user')
    qm.add_to_queue("0200.068.636", priority=3, requested_by='manual')
    
    print("\nStats après ajout:", qm.get_queue_stats())
    
    # Récupérer les items
    print("\nItems en attente:")
    items = qm.get_queue_items('pending', limit=10)
    for item in items:
        print(f"  - {item['enterprise_number']} (priorité: {item['priority_label']})")
    
    # Récupérer le suivant
    next_items = qm.get_next_to_scrape(count=2)
    print(f"\nProchain à scraper: {next_items}")
    
    print("\nStats finales:", qm.get_queue_stats())
