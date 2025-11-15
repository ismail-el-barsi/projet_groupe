"""
Gestionnaire de proxies avec gestion des contraintes:
- Maximum 20 requêtes simultanées
- Une requête par IP toutes les 20 secondes
- Cooldown de 5 minutes si échec
"""
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta
from threading import Lock

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProxyManager:
    def __init__(self, proxy_file="proxies_list.txt", max_concurrent=20, request_delay=20, cooldown_time=300):
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
        self.last_used = defaultdict(lambda: datetime.min)
        self.cooldown_until = defaultdict(lambda: datetime.min)
        self.active_requests = 0
        self.lock = Lock()
        self.current_proxy = None  # Ajouter cet attribut
        
        self.load_proxies()
    
    def load_proxies(self):
        """Charge les proxies depuis le fichier"""
        try:
            with open(self.proxy_file, 'r') as f:
                self.proxies = [line.strip() for line in f if line.strip()]
            logger.info(f"Chargé {len(self.proxies)} proxies")
        except FileNotFoundError:
            logger.warning(f"Fichier {self.proxy_file} non trouvé. Aucun proxy chargé.")
            self.proxies = []
    
    def get_proxy(self):
        """
        Obtient un proxy disponible selon les contraintes
        
        Returns:
            dict: Configuration du proxy ou None si aucun disponible
        """
        with self.lock:
            # Vérifier le nombre de requêtes actives
            if self.active_requests >= self.max_concurrent:
                logger.debug("Maximum de requêtes simultanées atteint")
                return None
            
            now = datetime.now()
            
            # Trouver un proxy disponible
            for proxy in self.proxies:
                # Vérifier si en cooldown
                if now < self.cooldown_until[proxy]:
                    continue
                
                # Vérifier le délai entre requêtes
                time_since_last = (now - self.last_used[proxy]).total_seconds()
                if time_since_last < self.request_delay:
                    continue
                
                # Proxy disponible
                self.last_used[proxy] = now
                self.active_requests += 1
                self.current_proxy = proxy  # Sauvegarder le proxy actuel
                
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
        with self.lock:
            self.active_requests = max(0, self.active_requests - 1)
            
            if not success:
                # Mettre en cooldown
                self.cooldown_until[proxy] = datetime.now() + timedelta(seconds=self.cooldown_time)
                logger.info(f"Proxy {proxy} mis en cooldown pour {self.cooldown_time}s")
            else:
                logger.debug(f"Proxy {proxy} libéré avec succès")
    
    def get_stats(self):
        """Retourne les statistiques du gestionnaire"""
        now = datetime.now()
        available = 0
        in_cooldown = 0
        waiting_delay = 0
        
        for proxy in self.proxies:
            if now < self.cooldown_until[proxy]:
                in_cooldown += 1
            elif (now - self.last_used[proxy]).total_seconds() < self.request_delay:
                waiting_delay += 1
            else:
                available += 1
        
        return {
            'total_proxies': len(self.proxies),
            'active_requests': self.active_requests,
            'available': available,
            'in_cooldown': in_cooldown,
            'waiting_delay': waiting_delay
        }


if __name__ == "__main__":
    # Test du gestionnaire
    manager = ProxyManager()
    print("Stats:", manager.get_stats())
    
    # Test d'obtention d'un proxy
    proxy, config = manager.get_proxy()
    if proxy:
        print(f"Proxy obtenu: {proxy}")
        print(f"Config: {config}")
        
        # Simuler une utilisation
        time.sleep(1)
        manager.release_proxy(proxy, success=True)
        print("Proxy libéré")
    else:
        print("Aucun proxy disponible")
