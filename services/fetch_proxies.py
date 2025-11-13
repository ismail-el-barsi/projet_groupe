"""
Script pour récupérer et maintenir une liste de proxies gratuits
"""
import logging
import re

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fetch_proxies_from_spys():
    """Récupère les proxies depuis spys.me"""
    try:
        logger.info("Récupération des proxies depuis spys.me...")
        regex = r"[0-9]+(?:\.[0-9]+){3}:[0-9]+"
        response = requests.get("https://spys.me/proxy.txt", timeout=10)
        proxies = re.findall(regex, response.text, re.MULTILINE)
        logger.info(f"Trouvé {len(proxies)} proxies depuis spys.me")
        return proxies
    except Exception as e:
        logger.error(f"Erreur lors de la récupération depuis spys.me: {e}")
        return []


def fetch_proxies_from_free_proxy_list():
    """Récupère les proxies depuis free-proxy-list.net"""
    try:
        logger.info("Récupération des proxies depuis free-proxy-list.net...")
        response = requests.get("https://free-proxy-list.net/", timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
        td_elements = soup.select('.fpl-list .table tbody tr td')
        
        proxies = []
        for j in range(0, len(td_elements), 8):
            if j + 1 < len(td_elements):
                ip = td_elements[j].text.strip()
                port = td_elements[j + 1].text.strip()
                proxies.append(f"{ip}:{port}")
        
        logger.info(f"Trouvé {len(proxies)} proxies depuis free-proxy-list.net")
        return proxies
    except Exception as e:
        logger.error(f"Erreur lors de la récupération depuis free-proxy-list.net: {e}")
        return []


def fetch_all_proxies(output_file="proxies_list.txt"):
    """Récupère tous les proxies et les sauvegarde dans un fichier"""
    all_proxies = []
    
    # Récupération depuis les différentes sources
    all_proxies.extend(fetch_proxies_from_spys())
    all_proxies.extend(fetch_proxies_from_free_proxy_list())
    
    # Suppression des doublons
    unique_proxies = list(set(all_proxies))
    logger.info(f"Total de {len(unique_proxies)} proxies uniques")
    
    # Sauvegarde dans le fichier
    with open(output_file, 'w') as file:
        for proxy in unique_proxies:
            file.write(f"{proxy}\n")
    
    logger.info(f"Proxies sauvegardés dans {output_file}")
    return unique_proxies


if __name__ == "__main__":
    fetch_all_proxies()
