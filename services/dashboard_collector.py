"""
Collecteur de statistiques pour le dashboard administrateur
Met à jour dashboard_stats.json en temps réel
"""
import glob
import json
import os
from datetime import datetime, timedelta
from pathlib import Path


class DashboardCollector:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.stats_file = os.path.join(data_dir, "dashboard_stats.json")
        self.queue_file = os.path.join(data_dir, "enterprise_queue.json")
        self.failed_file = os.path.join(data_dir, "failed_enterprises.json")
        self.html_dir = os.path.join(data_dir, "html_pages")
        self.locks_dir = os.path.join(data_dir, "locks")
        self.csv_file = os.path.join(data_dir, "enterprise.csv")
        
    def load_stats(self):
        """Charge les stats existantes"""
        if os.path.exists(self.stats_file):
            with open(self.stats_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return self._init_stats()
    
    def _init_stats(self):
        """Initialise les stats"""
        return {
            "general": {
                "total_scraped": 0,
                "total_queue": 0,
                "total_failed": 0,
                "total_closed": 0,
                "last_update": ""
            },
            "performance": {
                "success_rate_per_minute": 0,
                "avg_requests_per_minute": 0,
                "total_requests_last_hour": 0,
                "success_requests_last_hour": 0
            },
            "ips": {},
            "queue": {
                "current_index": 0,
                "total": 0,
                "pending": []
            },
            "failures": {
                "by_type": {
                    "ip_blocked": 0,
                    "parsing_error": 0,
                    "document_error": 0,
                    "network_error": 0,
                    "other": 0
                },
                "recent": []
            },
            "dags_status": {}
        }
    
    def save_stats(self, stats):
        """Sauvegarde les stats"""
        with open(self.stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
    
    def count_scraped_enterprises(self):
        """Compte les fichiers HTML scrappés"""
        if not os.path.exists(self.html_dir):
            return 0
        html_files = glob.glob(os.path.join(self.html_dir, "*.html"))
        return len(html_files)
    
    def get_queue_info(self):
        """Récupère les infos de la queue"""
        if not os.path.exists(self.queue_file):
            return {"current_index": 0, "total": 0}
        
        with open(self.queue_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def get_failed_info(self):
        """Récupère les infos sur les échecs"""
        if not os.path.exists(self.failed_file):
            return {}
        
        with open(self.failed_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def count_active_locks(self):
        """Compte les locks actifs (scraping en cours)"""
        if not os.path.exists(self.locks_dir):
            return 0
        
        locks = glob.glob(os.path.join(self.locks_dir, "*.lock"))
        valid_locks = 0
        
        for lock_file in locks:
            try:
                with open(lock_file, 'r') as f:
                    lock_data = json.load(f)
                    lock_time = datetime.fromisoformat(lock_data['timestamp'])
                    # Lock valide si moins de 5 minutes
                    if (datetime.now() - lock_time).seconds < 300:
                        valid_locks += 1
            except:
                continue
        
        return valid_locks
    
    def update_general_stats(self):
        """Met à jour les statistiques générales"""
        stats = self.load_stats()
        
        # Nombre total scrappé
        total_scraped = self.count_scraped_enterprises()
        stats['general']['total_scraped'] = total_scraped
        
        # Info queue
        queue_info = self.get_queue_info()
        current_idx = queue_info.get('current_index', 0)
        total = queue_info.get('total', 0)
        pending = max(0, total - current_idx)
        
        stats['general']['total_queue'] = pending
        stats['queue'] = {
            "current_index": current_idx,
            "total": total,
            "pending": pending
        }
        
        # Échecs
        failed_info = self.get_failed_info()
        stats['general']['total_failed'] = len(failed_info)
        
        # Lire dag_progress.json pour afficher position de chaque DAG
        dag_progress_file = os.path.join(self.data_dir, "dag_progress.json")
        if os.path.exists(dag_progress_file):
            try:
                with open(dag_progress_file, 'r') as f:
                    dag_progress = json.load(f)
                    
                # Mettre à jour les positions dans dags_status
                for dag_id, position in dag_progress.items():
                    if dag_id not in stats['dags_status']:
                        stats['dags_status'][dag_id] = {
                            'total_scraped': 0,
                            'last_scrape': '',
                            'status': 'running',
                            'current_position': 0
                        }
                    stats['dags_status'][dag_id]['current_position'] = position
            except:
                pass
        
        # Timestamp
        stats['general']['last_update'] = datetime.now().isoformat()
        
        self.save_stats(stats)
        return stats
    
    def record_scraping_success(self, enterprise_id, dag_id, proxy_ip=None, duration=0):
        """Enregistre un scraping réussi"""
        stats = self.load_stats()
        
        # Mise à jour général
        stats['general']['total_scraped'] += 1
        
        # Performance
        if 'scraping_history' not in stats:
            stats['scraping_history'] = []
        
        stats['scraping_history'].append({
            'timestamp': datetime.now().isoformat(),
            'enterprise_id': enterprise_id,
            'dag_id': dag_id,
            'proxy_ip': proxy_ip,
            'duration': duration,
            'success': True
        })
        
        # Garder seulement les 1000 derniers
        if len(stats['scraping_history']) > 1000:
            stats['scraping_history'] = stats['scraping_history'][-1000:]
        
        # Mise à jour IP
        if proxy_ip:
            if proxy_ip not in stats['ips']:
                stats['ips'][proxy_ip] = {
                    'total_requests': 0,
                    'successful_requests': 0,
                    'failed_requests': 0,
                    'status': 'actif',
                    'last_used': '',
                    'last_success': ''
                }
            
            stats['ips'][proxy_ip]['total_requests'] += 1
            stats['ips'][proxy_ip]['successful_requests'] += 1
            stats['ips'][proxy_ip]['last_used'] = datetime.now().isoformat()
            stats['ips'][proxy_ip]['last_success'] = datetime.now().isoformat()
            stats['ips'][proxy_ip]['status'] = 'actif'
        
        # Mise à jour DAG
        if dag_id not in stats['dags_status']:
            stats['dags_status'][dag_id] = {
                'total_scraped': 0,
                'last_scrape': '',
                'status': 'running'
            }
        
        stats['dags_status'][dag_id]['total_scraped'] += 1
        stats['dags_status'][dag_id]['last_scrape'] = datetime.now().isoformat()
        
        stats['general']['last_update'] = datetime.now().isoformat()
        
        self.save_stats(stats)
        # Mettre à jour les performances immédiatement
        self.update_performance_metrics()
    
    def record_scraping_failure(self, enterprise_id, dag_id, proxy_ip=None, error_type='other', error_msg=''):
        """Enregistre un échec de scraping"""
        stats = self.load_stats()
        
        # Mise à jour échecs
        stats['general']['total_failed'] += 1
        
        if error_type in stats['failures']['by_type']:
            stats['failures']['by_type'][error_type] += 1
        else:
            stats['failures']['by_type']['other'] += 1
        
        # Ajouter aux échecs récents
        stats['failures']['recent'].append({
            'timestamp': datetime.now().isoformat(),
            'enterprise_id': enterprise_id,
            'dag_id': dag_id,
            'proxy_ip': proxy_ip,
            'error_type': error_type,
            'error_msg': error_msg
        })
        
        # Garder seulement les 100 derniers échecs
        if len(stats['failures']['recent']) > 100:
            stats['failures']['recent'] = stats['failures']['recent'][-100:]
        
        # Mise à jour IP si échec proxy
        if proxy_ip and error_type == 'ip_blocked':
            if proxy_ip in stats['ips']:
                stats['ips'][proxy_ip]['status'] = 'bloqué'
                stats['ips'][proxy_ip]['failed_requests'] += 1
        
        stats['general']['last_update'] = datetime.now().isoformat()
        
        self.save_stats(stats)
        # Mettre à jour les performances immédiatement
        self.update_performance_metrics()
    
    def update_performance_metrics(self):
        """Calcule les métriques de performance"""
        stats = self.load_stats()
        
        if 'scraping_history' not in stats or not stats['scraping_history']:
            stats['performance'] = {
                'success_rate_per_minute': 0,
                'avg_requests_per_minute': 0,
                'total_requests_last_hour': 0,
                'success_requests_last_hour': 0
            }
            self.save_stats(stats)
            return
        
        now = datetime.now()
        one_hour_ago = now - timedelta(hours=1)
        one_minute_ago = now - timedelta(minutes=1)
        
        # Filtrer dernière heure
        recent_hour = [
            h for h in stats['scraping_history']
            if datetime.fromisoformat(h['timestamp']) > one_hour_ago
        ]
        
        # Filtrer dernière minute
        recent_minute = [
            h for h in stats['scraping_history']
            if datetime.fromisoformat(h['timestamp']) > one_minute_ago
        ]
        
        # Compter succès et échecs de la dernière heure
        success_hour = len([h for h in recent_hour if h.get('success')])
        
        # Ajouter les échecs récents de la dernière heure
        failures_hour = []
        if 'failures' in stats and 'recent' in stats['failures']:
            failures_hour = [
                f for f in stats['failures']['recent']
                if datetime.fromisoformat(f['timestamp']) > one_hour_ago
            ]
        
        total_hour = success_hour + len(failures_hour)
        
        stats['performance']['total_requests_last_hour'] = total_hour
        stats['performance']['success_requests_last_hour'] = success_hour
        
        if total_hour > 0:
            stats['performance']['avg_requests_per_minute'] = total_hour / 60.0
            stats['performance']['success_rate_per_minute'] = (success_hour / total_hour) * 100
        else:
            stats['performance']['avg_requests_per_minute'] = 0
            stats['performance']['success_rate_per_minute'] = 0
        
        self.save_stats(stats)
    
    def get_dashboard_data(self):
        """Retourne toutes les données pour le dashboard"""
        self.update_general_stats()
        self.update_performance_metrics()
        return self.load_stats()


def update_dashboard_stats(data_dir=None):
    """Fonction helper pour mise à jour rapide"""
    if not data_dir:
        data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
    
    collector = DashboardCollector(data_dir)
    return collector.update_general_stats()
    return collector.update_general_stats()
