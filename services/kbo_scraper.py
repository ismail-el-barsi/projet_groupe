"""
Script de scraping simple avec gestion des proxies et des contraintes
"""
import csv
import logging
import os
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from proxy_manager import ProxyManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KBOScraper:
    def __init__(self, proxy_manager=None, output_dir="data/html_pages", use_proxy=True):
        """
        Initialise le scraper KBO
        
        Args:
            proxy_manager: Instance de ProxyManager (optionnel si use_proxy=False)
            output_dir: Répertoire pour stocker les pages HTML
            use_proxy: Utiliser les proxies ou non (False = connexion directe)
        """
        self.proxy_manager = proxy_manager
        self.output_dir = output_dir
        self.use_proxy = use_proxy
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'errors': []
        }
        
        # Créer le répertoire de sortie
        os.makedirs(output_dir, exist_ok=True)
        
        if use_proxy and proxy_manager is None:
            logger.warning("use_proxy=True mais aucun proxy_manager fourni. Mode direct activé.")
            self.use_proxy =True
    
    def scrape_enterprise(self, enterprise_number, retry_queue=None):
        """
        Scrape une entreprise depuis le site KBO
        
        Args:
            enterprise_number: Numéro d'entreprise à scraper
            retry_queue: File d'attente pour les retry (optionnel)
        
        Returns:
            bool: True si succès, False sinon
        """
        # Vérifier si le fichier existe déjà
        output_file = os.path.join(self.output_dir, f"{enterprise_number}.html")
        if os.path.exists(output_file):
            logger.info(f"⏭ {enterprise_number} déjà scrapé, ignoré")
            self.stats['total'] += 1
            self.stats['success'] += 1
            return True
        
        self.stats['total'] += 1
        max_retries = 5  # Augmenté à 5 tentatives
        retry_count = 0
        
        while retry_count < max_retries:
            retry_count += 1
            
            # Mode avec ou sans proxy
            if self.use_proxy and self.proxy_manager:
                # Obtenir un proxy disponible
                proxy, proxy_config = self.proxy_manager.get_proxy()
                
                if not proxy:
                    logger.warning("Aucun proxy disponible, attente de 5s...")
                    time.sleep(5)
                    retry_count -= 1  # Ne pas compter cette tentative
                    continue
                
                proxy_str = proxy
            else:
                # Mode direct sans proxy
                proxy = None
                proxy_config = None
                proxy_str = "DIRECT"
            
            try:
                # URL du site KBO - formulaire de recherche avec langue française
                url = f"https://kbopub.economie.fgov.be/kbopub/zoeknummerform.html?lang=fr&nummer={enterprise_number}&actionLu=Rechercher"
                
                # Headers avec langue française
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept-Language': 'fr-FR,fr;q=0.9',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
                }
                
                # Faire la requête
                logger.info(f"Scraping {enterprise_number} avec {proxy_str}")
                response = requests.get(
                    url,
                    proxies=proxy_config,
                    timeout=15,
                    headers=headers
                )
                
                # Vérifier le statut
                if response.status_code == 404:
                    logger.warning(f"Entreprise {enterprise_number} non trouvée (404)")
                    if self.use_proxy:
                        self.proxy_manager.release_proxy(proxy, success=True)
                    self.stats['failed'] += 1
                    return False
                
                if response.status_code != 200:
                    logger.error(f"Erreur HTTP {response.status_code} pour {enterprise_number}")
                    if self.use_proxy and proxy:
                        self.proxy_manager.release_proxy(proxy, success=False)
                    continue
                
                # Sauvegarder la page HTML
                output_file = os.path.join(self.output_dir, f"{enterprise_number}.html")
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                
                logger.info(f"✓ Page {enterprise_number} sauvegardée avec succès")
                if self.use_proxy and proxy:
                    self.proxy_manager.release_proxy(proxy, success=True)
                self.stats['success'] += 1
                
                # Délai respectueux entre requêtes en mode direct
                if not self.use_proxy or not proxy:
                    time.sleep(2)
                
                return True
                
            except requests.exceptions.Timeout:
                logger.error(f"Timeout pour {enterprise_number} avec {proxy_str} (tentative {retry_count}/{max_retries})")
                if self.use_proxy and proxy:
                    self.proxy_manager.release_proxy(proxy, success=False)
                time.sleep(1)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Erreur réseau pour {enterprise_number}: {str(e)[:100]} (tentative {retry_count}/{max_retries})")
                if self.use_proxy and proxy:
                    self.proxy_manager.release_proxy(proxy, success=False)
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Erreur inattendue pour {enterprise_number}: {e} (tentative {retry_count}/{max_retries})")
                if self.use_proxy and proxy:
                    self.proxy_manager.release_proxy(proxy, success=False)
                time.sleep(1)
        
        # Échec après tous les retries
        logger.error(f"❌ Échec du scraping de {enterprise_number} après {max_retries} tentatives")
        self.stats['failed'] += 1
        self.stats['errors'].append({
            'enterprise_number': enterprise_number,
            'error': 'Max retries exceeded'
        })
        
        # Ajouter à la file de retry basse priorité si fournie
        if retry_queue is not None:
            retry_queue.append(enterprise_number)
        
        return False
    
    def scrape_from_csv(self, csv_file, limit=None):
        """
        Scrape les entreprises depuis un fichier CSV
        
        Args:
            csv_file: Fichier CSV contenant les numéros d'entreprises
            limit: Limite du nombre d'entreprises à scraper (optionnel)
        """
        logger.info(f"Démarrage du scraping depuis {csv_file}")
        retry_queue = []
        
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                enterprises = list(reader)
                
                if limit:
                    enterprises = enterprises[:limit]
                
                logger.info(f"Nombre d'entreprises à scraper: {len(enterprises)}")
                
                for idx, row in enumerate(enterprises, 1):
                    # Chercher le numéro d'entreprise dans les colonnes possibles
                    enterprise_number = row.get('EnterpriseNumber') or row.get('enterprise_number') or row.get('number')
                    
                    if not enterprise_number:
                        logger.warning(f"Ligne {idx}: numéro d'entreprise non trouvé")
                        continue
                    
                    logger.info(f"[{idx}/{len(enterprises)}] Scraping {enterprise_number}")
                    self.scrape_enterprise(enterprise_number, retry_queue)
                    
                    # Afficher les stats périodiquement
                    if idx % 10 == 0:
                        logger.info(f"Stats: {self.stats}")
                        logger.info(f"Proxy Manager: {self.proxy_manager.get_stats()}")
                
                # Traiter la file de retry avec priorité basse
                if retry_queue:
                    logger.info(f"Traitement de {len(retry_queue)} entreprises en retry...")
                    for enterprise_number in retry_queue:
                        self.scrape_enterprise(enterprise_number)
        
        except FileNotFoundError:
            logger.error(f"Fichier {csv_file} non trouvé")
        except Exception as e:
            logger.error(f"Erreur lors du scraping: {e}")
        
        finally:
            logger.info("=== RAPPORT FINAL ===")
            logger.info(f"Total: {self.stats['total']}")
            logger.info(f"Succès: {self.stats['success']}")
            logger.info(f"Échecs: {self.stats['failed']}")
            if self.stats['errors']:
                logger.info(f"Erreurs: {len(self.stats['errors'])}")


def main():
    """Fonction principale pour tester le scraper"""
    import sys

    # Déterminer le mode - PAR DÉFAUT: SANS PROXY
    use_proxy = "--proxy" in sys.argv
    
    if use_proxy:
        logger.info("=== MODE AVEC PROXIES ===")
        # Initialiser le gestionnaire de proxies
        proxy_manager = ProxyManager(
            proxy_file="proxies_list.txt",
            max_concurrent=20,
            request_delay=20,
            cooldown_time=300
        )
        scraper = KBOScraper(proxy_manager, use_proxy=True)
    else:
        logger.info("=== MODE DIRECT (SANS PROXY) ===")
        logger.info("Connexion directe au site KBO")
        scraper = KBOScraper(use_proxy=False)
    
    # Scraper depuis le CSV enterprise.csv (limité à 10 pour le test)
    scraper.scrape_from_csv("data/enterprise.csv", limit=10)


if __name__ == "__main__":
    main()
    main()
