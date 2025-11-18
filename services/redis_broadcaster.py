"""
Service pour diffuser les changements Redis via WebSocket
"""
import time
import logging
from threading import Thread

logger = logging.getLogger(__name__)


class RedisBroadcaster:
    """
    Surveille Redis et diffuse les changements via WebSocket
    """
    
    def __init__(self, queue_manager, socketio_app):
        self.queue_manager = queue_manager
        self.socketio = socketio_app
        self.running = False
        self.thread = None
        self.last_stats = {}
        self.last_assignments = {}
        
    def start(self):
        """Démarre le thread de surveillance"""
        if self.running:
            return
            
        self.running = True
        self.thread = Thread(target=self._monitor_redis, daemon=True)
        self.thread.start()
        logger.info("📡 RedisBroadcaster démarré")
        
    def stop(self):
        """Arrête le thread de surveillance"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=2)
        logger.info("📡 RedisBroadcaster arrêté")
        
    def _monitor_redis(self):
        """Surveille Redis et émet les changements"""
        while self.running:
            try:
                # Vérifier les stats de la queue
                current_stats = self.queue_manager.get_queue_stats()
                
                if current_stats != self.last_stats:
                    # Stats ont changé, diffuser
                    self.socketio.emit('queue_stats_update', current_stats, namespace='/')
                    logger.debug(f"📊 Stats queue diffusées: {current_stats}")
                    self.last_stats = current_stats.copy()
                
                # Vérifier les assignments DAG
                current_assignments = self.queue_manager.get_enterprises_by_dag()
                
                if current_assignments != self.last_assignments:
                    # Assignments ont changé, diffuser
                    self.socketio.emit('dag_assignments_update', current_assignments, namespace='/')
                    logger.debug(f"🎯 Assignments DAG diffusés: {len(current_assignments)} DAGs actifs")
                    self.last_assignments = current_assignments.copy()
                
            except Exception as e:
                logger.error(f"Erreur monitoring Redis: {e}")
            
            # Attendre 500ms avant la prochaine vérification (temps réel)
            time.sleep(0.5)
