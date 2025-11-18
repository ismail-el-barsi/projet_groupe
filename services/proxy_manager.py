"""
Gestionnaire de proxies avec gestion des contraintes:
- Maximum 20 requêtes simultanées
- Une requête par IP toutes les 20 secondes
- Cooldown de 5 minutes si échec
"""
import logging
import os
import time
from datetime import datetime, timedelta

import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProxyManager:
    def __init__(self, proxy_file="proxies_list.txt", max_concurrent=20, request_delay=20, cooldown_time=300, redis_url=None):
        """
        Initialise le gestionnaire de proxies
        
        Args:
            proxy_file: Fichier contenant la liste des proxies
            max_concurrent: Nombre maximum de requêtes simultanées
            request_delay: Délai entre deux requêtes pour une même IP (secondes)
            cooldown_time: Temps de cooldown après un échec (secondes)
        """
        self.proxy_file = proxy_file
        self.max_concurrent = max_concurrent
        self.request_delay = request_delay
        self.cooldown_time = cooldown_time
        
        self.proxies = []
        self.current_proxy = None
        # Correction : utiliser l'URL Docker Compose par défaut
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://redis:6379/0")
        self.redis = redis.Redis.from_url(self.redis_url, decode_responses=True)
        self.load_proxies()
    
    def load_proxies(self):
        """Charge les proxies depuis le fichier"""
        try:
            with open(self.proxy_file, 'r') as f:
                self.proxies = [line.strip() for line in f if line.strip()]
            logger.info(f"Chargé {len(self.proxies)} proxies")
            # Stocker la liste des proxies dans Redis pour référence globale
            self.redis.delete("proxy_list")
            if self.proxies:
                self.redis.rpush("proxy_list", *self.proxies)
        except FileNotFoundError:
            logger.warning(f"Fichier {self.proxy_file} non trouvé. Aucun proxy chargé.")
            self.proxies = []
    
    def get_proxy(self):
        """
        Obtient un proxy disponible selon les contraintes
        
        Returns:
            dict: Configuration du proxy ou None si aucun disponible
        """
        # Vérifier le nombre de requêtes actives globales
        active_requests = int(self.redis.get("active_requests") or 0)
        if active_requests >= self.max_concurrent:
            logger.debug("Maximum de requêtes simultanées atteint")
            return None, None

        now = datetime.now().timestamp()
        proxies = self.redis.lrange("proxy_list", 0, -1) or self.proxies
        for proxy in proxies:
            cooldown_until = float(self.redis.get(f"proxy:{proxy}:cooldown_until") or 0)
            last_used = float(self.redis.get(f"proxy:{proxy}:last_used") or 0)

            if now < cooldown_until:
                continue
            if (now - last_used) < self.request_delay:
                continue

            # Proxy disponible
            self.redis.set(f"proxy:{proxy}:last_used", now)
            self.redis.incr("active_requests")
            self.current_proxy = proxy
            proxy_config = {
                'http': f'http://{proxy}',
                'https': f'http://{proxy}'
            }
            logger.debug(f"Proxy attribué: {proxy}")
            return proxy, proxy_config

        logger.debug("Aucun proxy disponible pour le moment")
        self.current_proxy = None
        return None, None
    
    def release_proxy(self, proxy, success=True):
        """
        Libère un proxy après utilisation
        
        Args:
            proxy: Le proxy à libérer
            success: Si la requête a réussi ou non
        """
        # Décrémenter le nombre de requêtes actives globales
        pipe = self.redis.pipeline()
        pipe.decr("active_requests")
        if not success:
            cooldown_until = datetime.now().timestamp() + self.cooldown_time
            pipe.set(f"proxy:{proxy}:cooldown_until", cooldown_until)
            logger.info(f"Proxy {proxy} mis en cooldown pour {self.cooldown_time}s")
        else:
            logger.debug(f"Proxy {proxy} libéré avec succès")
        pipe.execute()
    
    def get_stats(self):
        """Retourne les statistiques du gestionnaire"""
        now = datetime.now().timestamp()
        available = 0
        in_cooldown = 0
        waiting_delay = 0
        proxies = self.redis.lrange("proxy_list", 0, -1) or self.proxies
        for proxy in proxies:
            cooldown_until = float(self.redis.get(f"proxy:{proxy}:cooldown_until") or 0)
            last_used = float(self.redis.get(f"proxy:{proxy}:last_used") or 0)
            if now < cooldown_until:
                in_cooldown += 1
            elif (now - last_used) < self.request_delay:
                waiting_delay += 1
            else:
                available += 1
        return {
            'total_proxies': len(proxies),
            'active_requests': int(self.redis.get("active_requests") or 0),
            'available': available,
            'in_cooldown': in_cooldown,
            'waiting_delay': waiting_delay
        }


if __name__ == "__main__":
    # Test du gestionnaire avec Redis
    manager = ProxyManager()
    print("Stats:", manager.get_stats())
    proxy, config = manager.get_proxy()
    if proxy:
        print(f"Proxy obtenu: {proxy}")
        print(f"Config: {config}")
        time.sleep(1)
        manager.release_proxy(proxy, success=True)
        print("Proxy libéré")
    else:
        print("Aucun proxy disponible")
