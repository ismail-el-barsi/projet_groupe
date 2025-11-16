"""
Collecteur de statistiques pour le dashboard administrateur
Utilise PostgreSQL pour stocker les statistiques en temps réel
"""
import glob
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import (Boolean, Column, DateTime, Float, Integer, String,
                        Text, create_engine)
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

logger = logging.getLogger(__name__)
if not logger.handlers:
    # Basic configuration if not already configured by Airflow
    logging.basicConfig(level=logging.INFO)

class ScrapingHistory(Base):
    __tablename__ = 'scraping_history'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, default=datetime.now, index=True)
    enterprise_id = Column(String(50), index=True)
    dag_id = Column(String(100), index=True)
    proxy_ip = Column(String(50))
    duration = Column(Float, default=0)
    success = Column(Boolean, default=True)
    error_type = Column(String(50))
    error_msg = Column(Text)

class ProxyStats(Base):
    __tablename__ = 'proxy_stats'
    
    proxy_ip = Column(String(50), primary_key=True)
    total_requests = Column(Integer, default=0)
    successful_requests = Column(Integer, default=0)
    failed_requests = Column(Integer, default=0)
    status = Column(String(20), default='actif')
    last_used = Column(DateTime)
    last_success = Column(DateTime)

class DagStatus(Base):
    __tablename__ = 'dag_status'
    
    dag_id = Column(String(100), primary_key=True)
    total_scraped = Column(Integer, default=0)
    last_scrape = Column(DateTime)
    status = Column(String(20), default='running')
    current_position = Column(Integer, default=0)


class Entreprise(Base):
    __tablename__ = 'entreprises'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    numero_entreprise = Column(String(50), unique=True, index=True, nullable=False)
    denomination = Column(String(255))
    status = Column(String(50))
    extraction_date = Column(DateTime, default=datetime.now)
    last_update = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # Données complètes en JSONB (PostgreSQL)
    data = Column(JSONB, nullable=False)
    
    # Champs extraits pour faciliter les recherches
    adresse = Column(Text)
    forme_juridique = Column(String(100))
    numero_tva = Column(String(50))
    date_creation = Column(String(50))


class ProcessingReport(Base):
    __tablename__ = 'processing_reports'

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=datetime.now, index=True)
    total_files = Column(Integer)
    processed = Column(Integer)
    errors = Column(Integer)
    payload = Column(JSONB)


class ValidationReport(Base):
    __tablename__ = 'validation_reports'

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=datetime.now, index=True)
    source = Column(String(100))
    statistiques = Column(JSONB)
    repartition_erreurs = Column(JSONB)
    erreurs_localisation = Column(JSONB)
    details = Column(JSONB)


class DashboardMetric(Base):
    __tablename__ = 'dashboard_metrics'

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(100), unique=True, index=True, nullable=False)
    last_update = Column(DateTime, default=datetime.now)
    metrics = Column(JSONB)


class ScrapingQueue(Base):
    """File d'attente pour le scraping avec gestion de priorités"""
    __tablename__ = 'scraping_queue'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    enterprise_number = Column(String(50), unique=True, index=True, nullable=False)
    priority = Column(Integer, default=1, index=True)  # 1=normal, 2=haute, 3=très haute
    status = Column(String(20), default='pending', index=True)  # pending, processing, completed, failed
    added_at = Column(DateTime, default=datetime.now, index=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    attempts = Column(Integer, default=0)
    last_error = Column(Text, nullable=True)
    requested_by = Column(String(50), default='system')  # system, user, manual
    

class DashboardCollector:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.queue_file = os.path.join(data_dir, "enterprise_queue.json")
        self.failed_file = os.path.join(data_dir, "failed_enterprises.json")
        self.html_dir = os.path.join(data_dir, "html_pages")
        self.locks_dir = os.path.join(data_dir, "locks")
        
        # Connexion PostgreSQL (base dédiée KBO)
        self.engine = create_engine(
            'postgresql+psycopg2://kbo_admin:kbo_2025_secure@postgres_kbo/kbo_dashboard'
        )
        # create tables if they don't exist (safe for concurrent processes)
        Base.metadata.create_all(self.engine, checkfirst=True)
        self.Session = sessionmaker(bind=self.engine)
        
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
        session = self.Session()
        try:
            # Nombre total scrappé
            total_scraped = self.count_scraped_enterprises()
            
            # Info queue
            queue_info = self.get_queue_info()
            current_idx = queue_info.get('current_index', 0)
            total = queue_info.get('total', 0)
            pending = max(0, total - current_idx)
            
            # Échecs
            failed_info = self.get_failed_info()
            total_failed = len(failed_info)
            
            # Lire dag_progress.json pour afficher position de chaque DAG
            dag_progress_file = os.path.join(self.data_dir, "dag_progress.json")
            if os.path.exists(dag_progress_file):
                try:
                    with open(dag_progress_file, 'r') as f:
                        dag_progress = json.load(f)
                        
                    # Mettre à jour les positions dans la base
                    for dag_id, position in dag_progress.items():
                        stmt = insert(DagStatus).values(
                            dag_id=dag_id,
                            current_position=position
                        ).on_conflict_do_update(
                            index_elements=['dag_id'],
                            set_={'current_position': position}
                        )
                        session.execute(stmt)
                    session.commit()
                except Exception:
                    session.rollback()
                    logger.exception("Erreur lecture ou écriture dag_progress")
            
            return {
                "general": {
                    "total_scraped": total_scraped,
                    "total_queue": pending,
                    "total_failed": total_failed,
                    "last_update": datetime.now().isoformat()
                },
                "queue": {
                    "current_index": current_idx,
                    "total": total,
                    "pending": pending
                }
            }
        finally:
            session.close()
    
    def record_scraping_success(self, enterprise_id, dag_id, proxy_ip=None, duration=0):
        """Enregistre un scraping réussi"""
        session = self.Session()
        try:
            # Enregistrer dans l'historique
            history = ScrapingHistory(
                timestamp=datetime.now(),
                enterprise_id=enterprise_id,
                dag_id=dag_id,
                proxy_ip=proxy_ip,
                duration=duration,
                success=True
            )
            session.add(history)
            
            # Mise à jour IP
            if proxy_ip:
                stmt = insert(ProxyStats).values(
                    proxy_ip=proxy_ip,
                    total_requests=1,
                    successful_requests=1,
                    failed_requests=0,
                    status='actif',
                    last_used=datetime.now(),
                    last_success=datetime.now()
                ).on_conflict_do_update(
                    index_elements=['proxy_ip'],
                    set_={
                        'total_requests': ProxyStats.total_requests + 1,
                        'successful_requests': ProxyStats.successful_requests + 1,
                        'last_used': datetime.now(),
                        'last_success': datetime.now(),
                        'status': 'actif'
                    }
                )
                session.execute(stmt)
            
            # Mise à jour DAG
            stmt = insert(DagStatus).values(
                dag_id=dag_id,
                total_scraped=1,
                last_scrape=datetime.now(),
                status='running'
            ).on_conflict_do_update(
                index_elements=['dag_id'],
                set_={
                    'total_scraped': DagStatus.total_scraped + 1,
                    'last_scrape': datetime.now()
                }
            )
            session.execute(stmt)
            
            session.commit()
            logger.debug(
                "Recorded scraping success: enterprise=%s dag=%s proxy=%s duration=%s",
                enterprise_id,
                dag_id,
                proxy_ip,
                duration,
            )
        finally:
            session.close()
    
    def record_scraping_failure(self, enterprise_id, dag_id, proxy_ip=None, error_type='other', error_msg=''):
        """Enregistre un échec de scraping"""
        session = self.Session()
        try:
            # Enregistrer dans l'historique
            history = ScrapingHistory(
                timestamp=datetime.now(),
                enterprise_id=enterprise_id,
                dag_id=dag_id,
                proxy_ip=proxy_ip,
                success=False,
                error_type=error_type,
                error_msg=error_msg
            )
            session.add(history)
            
            # Mise à jour IP si échec proxy
            if proxy_ip and error_type == 'ip_blocked':
                stmt = insert(ProxyStats).values(
                    proxy_ip=proxy_ip,
                    total_requests=1,
                    successful_requests=0,
                    failed_requests=1,
                    status='bloqué',
                    last_used=datetime.now()
                ).on_conflict_do_update(
                    index_elements=['proxy_ip'],
                    set_={
                        'total_requests': ProxyStats.total_requests + 1,
                        'failed_requests': ProxyStats.failed_requests + 1,
                        'status': 'bloqué',
                        'last_used': datetime.now()
                    }
                )
                session.execute(stmt)
            
            session.commit()
            logger.debug(
                "Recorded scraping failure: enterprise=%s dag=%s proxy=%s type=%s",
                enterprise_id,
                dag_id,
                proxy_ip,
                error_type,
            )
        except Exception:
            session.rollback()
            logger.exception(
                "Erreur en enregistrant l'échec de scraping: enterprise=%s dag=%s",
                enterprise_id,
                dag_id,
            )
            raise
        finally:
            session.close()
    
    def update_performance_metrics(self):
        """Calcule les métriques de performance"""
        session = self.Session()
        try:
            now = datetime.now()

            # Try multiple windows to compute real data (no arbitrary fallback)
            windows = [
                (timedelta(hours=1), '1h'),
                (timedelta(hours=24), '24h'),
                (None, 'all')
            ]

            used_window = None
            total_requests = 0
            success_requests = 0
            window_minutes = None

            for win, label in windows:
                if win is None:
                    # all time
                    total_requests = session.query(ScrapingHistory).count()
                    success_requests = session.query(ScrapingHistory).filter(ScrapingHistory.success == True).count()
                    used_window = 'all'
                    window_minutes = max(1, (now - datetime(1970,1,1)).total_seconds() / 60.0)  # avoid div by zero
                else:
                    since = now - win
                    total_requests = session.query(ScrapingHistory).filter(ScrapingHistory.timestamp > since).count()
                    success_requests = session.query(ScrapingHistory).filter(
                        ScrapingHistory.timestamp > since,
                        ScrapingHistory.success == True
                    ).count()
                    if total_requests > 0:
                        used_window = label
                        window_minutes = win.total_seconds() / 60.0
                        break

            # If we didn't find a non-zero window and used_window == 'all' we still have totals
            if used_window is None and total_requests > 0:
                used_window = 'all'
                # approximate window_minutes as total minutes since first record
                first = session.query(ScrapingHistory).order_by(ScrapingHistory.timestamp.asc()).first()
                if first and first.timestamp:
                    window_minutes = max(1, (now - first.timestamp).total_seconds() / 60.0)
                else:
                    window_minutes = 60.0

            # Compute metrics
            if window_minutes and window_minutes > 0:
                avg_requests_per_minute = float(total_requests) / float(window_minutes)
            else:
                avg_requests_per_minute = 0.0

            success_rate = (float(success_requests) / float(total_requests) * 100.0) if total_requests > 0 else 0.0

            return {
                "performance": {
                    "success_rate_per_minute": success_rate,
                    "avg_requests_per_minute": avg_requests_per_minute,
                    "total_requests_window": total_requests,
                    "success_requests_window": success_requests,
                    "window": used_window
                }
            }
        finally:
            session.close()
    
    def get_failures_stats(self):
        """Récupère les statistiques d'échecs"""
        session = self.Session()
        try:
            # Compter par type d'erreur
            failures_by_type = {}
            for error_type in ['timeout', 'ip_blocked', 'network_error', 'parsing_error', 'other']:
                count = session.query(ScrapingHistory).filter(
                    ScrapingHistory.success == False,
                    ScrapingHistory.error_type == error_type
                ).count()
                failures_by_type[error_type] = count
            
            # Récupérer les 100 derniers échecs
            recent_failures = session.query(ScrapingHistory).filter(
                ScrapingHistory.success == False
            ).order_by(ScrapingHistory.timestamp.desc()).limit(100).all()
            
            recent = [{
                'timestamp': f.timestamp.isoformat(),
                'enterprise_id': f.enterprise_id,
                'dag_id': f.dag_id,
                'proxy_ip': f.proxy_ip,
                'error_type': f.error_type,
                'error_msg': f.error_msg
            } for f in recent_failures]
            
            return {
                "failures": {
                    "by_type": failures_by_type,
                    "recent": recent
                }
            }
        finally:
            session.close()
    
    def get_ips_stats(self):
        """Récupère les statistiques des IPs"""
        session = self.Session()
        try:
            proxies = session.query(ProxyStats).all()
            
            ips_dict = {}
            for proxy in proxies:
                ips_dict[proxy.proxy_ip] = {
                    'total_requests': proxy.total_requests,
                    'successful_requests': proxy.successful_requests,
                    'failed_requests': proxy.failed_requests,
                    'status': proxy.status,
                    'last_used': proxy.last_used.isoformat() if proxy.last_used else '',
                    'last_success': proxy.last_success.isoformat() if proxy.last_success else ''
                }
            
            return {"ips": ips_dict}
        finally:
            session.close()

    def get_ips_stats_paginated(self, page=1, per_page=20):
        """Récupère les statistiques des IPs avec pagination.

        Retourne dict: total, page, per_page, items (list of {ip, stats})
        """
        session = self.Session()
        try:
            total = session.query(ProxyStats).count()
            offset = max(0, (page - 1) * per_page)
            proxies = session.query(ProxyStats).order_by(ProxyStats.proxy_ip).limit(per_page).offset(offset).all()

            items = []
            for proxy in proxies:
                items.append({
                    'proxy_ip': proxy.proxy_ip,
                    'total_requests': proxy.total_requests,
                    'successful_requests': proxy.successful_requests,
                    'failed_requests': proxy.failed_requests,
                    'status': proxy.status,
                    'last_used': proxy.last_used.isoformat() if proxy.last_used else '',
                    'last_success': proxy.last_success.isoformat() if proxy.last_success else ''
                })

            return {'total': total, 'page': page, 'per_page': per_page, 'items': items}
        finally:
            session.close()
    
    def get_dags_stats(self):
        """Récupère les statistiques des DAGs"""
        session = self.Session()
        try:
            dags = session.query(DagStatus).all()
            
            dags_dict = {}
            for dag in dags:
                dags_dict[dag.dag_id] = {
                    'total_scraped': dag.total_scraped,
                    'last_scrape': dag.last_scrape.isoformat() if dag.last_scrape else '',
                    'status': dag.status,
                    'current_position': dag.current_position
                }
            
            return {"dags_status": dags_dict}
        finally:
            session.close()
    
    def insert_entreprise(self, data):
        """Insère ou met à jour les données d'une entreprise dans la BDD."""
        session = self.Session()
        try:
            # Extraire les champs clés
            presentation = data.get('presentation', {})
            infos_juridiques = data.get('informations_juridiques', {})
            
            numero_entreprise = presentation.get('numero_entreprise')
            if not numero_entreprise:
                raise ValueError("Numéro d'entreprise manquant dans les données")
            
            # Préparer les données pour l'insertion
            entreprise_data = {
                'numero_entreprise': numero_entreprise,
                'denomination': presentation.get('denomination'),
                'status': presentation.get('status'),
                'extraction_date': datetime.now(),
                'last_update': datetime.now(),
                'data': data,  # Stockage complet en JSONB
                'adresse': presentation.get('adresse_principale'),
                'forme_juridique': infos_juridiques.get('forme_juridique'),
                'numero_tva': infos_juridiques.get('numero_tva'),
                'date_creation': presentation.get('date_creation')
            }
            
            # Upsert : mise à jour si existe, insertion sinon
            stmt = insert(Entreprise).values(**entreprise_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['numero_entreprise'],
                set_={
                    'denomination': stmt.excluded.denomination,
                    'status': stmt.excluded.status,
                    'last_update': datetime.now(),
                    'data': stmt.excluded.data,
                    'adresse': stmt.excluded.adresse,
                    'forme_juridique': stmt.excluded.forme_juridique,
                    'numero_tva': stmt.excluded.numero_tva,
                    'date_creation': stmt.excluded.date_creation
                }
            )
            
            session.execute(stmt)
            session.commit()
            
            logger.info(f"✓ Entreprise {numero_entreprise} insérée/mise à jour dans la BDD")
            return {'success': True, 'numero_entreprise': numero_entreprise}
            
        except Exception as e:
            session.rollback()
            logger.exception(f"Erreur insertion entreprise: {e}")
            raise
        finally:
            session.close()
    
    def get_entreprise(self, numero_entreprise):
        """Récupère les données complètes d'une entreprise."""
        session = self.Session()
        try:
            entreprise = session.query(Entreprise).filter(
                Entreprise.numero_entreprise == numero_entreprise
            ).first()
            
            if not entreprise:
                return None
            
            return {
                'id': entreprise.id,
                'numero_entreprise': entreprise.numero_entreprise,
                'denomination': entreprise.denomination,
                'status': entreprise.status,
                'extraction_date': entreprise.extraction_date.isoformat() if entreprise.extraction_date else None,
                'last_update': entreprise.last_update.isoformat() if entreprise.last_update else None,
                'data': entreprise.data,  # Données complètes
                'adresse': entreprise.adresse,
                'forme_juridique': entreprise.forme_juridique,
                'numero_tva': entreprise.numero_tva,
                'date_creation': entreprise.date_creation
            }
        finally:
            session.close()
    
    def search_entreprises(self, query, limit=50):
        """Recherche des entreprises par numéro ou dénomination."""
        session = self.Session()
        try:
            # Recherche par numéro exact ou par nom (ILIKE)
            query_filter = session.query(Entreprise).filter(
                (Entreprise.numero_entreprise.like(f'%{query}%')) |
                (Entreprise.denomination.ilike(f'%{query}%'))
            ).limit(limit)
            
            entreprises = query_filter.all()
            
            results = []
            for e in entreprises:
                results.append({
                    'numero_entreprise': e.numero_entreprise,
                    'denomination': e.denomination,
                    'status': e.status,
                    'adresse': e.adresse,
                    'forme_juridique': e.forme_juridique,
                    'last_update': e.last_update.isoformat() if e.last_update else None
                })
            
            return results
        finally:
            session.close()

    def search_entreprises_paginated(self, query, page=1, per_page=20):
        """Recherche paginée des entreprises par numéro ou dénomination.

        Retourne un dict avec `total`, `page`, `per_page` et `results`.
        """
        session = self.Session()
        try:
            base_q = session.query(Entreprise).filter(
                (Entreprise.numero_entreprise.like(f'%{query}%')) |
                (Entreprise.denomination.ilike(f'%{query}%'))
            )
            total = base_q.count()
            offset = max(0, (page - 1) * per_page)
            entreprises = base_q.order_by(Entreprise.last_update.desc()).limit(per_page).offset(offset).all()

            results = []
            for e in entreprises:
                results.append({
                    'numero_entreprise': e.numero_entreprise,
                    'denomination': e.denomination,
                    'status': e.status,
                    'adresse': e.adresse,
                    'forme_juridique': e.forme_juridique,
                    'last_update': e.last_update.isoformat() if e.last_update else None
                })

            return {'total': total, 'page': page, 'per_page': per_page, 'results': results}
        finally:
            session.close()
    
    def list_all_entreprises(self, limit=100):
        """Liste toutes les entreprises (limitées)."""
        session = self.Session()
        try:
            entreprises = session.query(Entreprise).order_by(
                Entreprise.last_update.desc()
            ).limit(limit).all()
            
            results = []
            for e in entreprises:
                results.append({
                    'numero_entreprise': e.numero_entreprise,
                    'denomination': e.denomination,
                    'status': e.status,
                    'adresse': e.adresse,
                    'forme_juridique': e.forme_juridique,
                    'last_update': e.last_update.isoformat() if e.last_update else None
                })
            
            return results
        finally:
            session.close()

    def list_all_entreprises_paginated(self, page=1, per_page=20):
        """Liste paginée de toutes les entreprises ordonnées par `last_update`."""
        session = self.Session()
        try:
            total = session.query(Entreprise).count()
            offset = max(0, (page - 1) * per_page)
            entreprises = session.query(Entreprise).order_by(
                Entreprise.last_update.desc()
            ).limit(per_page).offset(offset).all()

            results = []
            for e in entreprises:
                results.append({
                    'numero_entreprise': e.numero_entreprise,
                    'denomination': e.denomination,
                    'status': e.status,
                    'adresse': e.adresse,
                    'forme_juridique': e.forme_juridique,
                    'last_update': e.last_update.isoformat() if e.last_update else None
                })

            return {'total': total, 'page': page, 'per_page': per_page, 'results': results}
        finally:
            session.close()
    
    def get_all_entreprises_count(self):
        """Retourne le nombre total d'entreprises dans la BDD."""
        session = self.Session()
        try:
            count = session.query(Entreprise).count()
            return count
        finally:
            session.close()

    def insert_processing_report(self, report: dict):
        """Insère un rapport de traitement (HTML -> BDD) dans `processing_reports`."""
        session = self.Session()
        try:
            pr = ProcessingReport(
                total_files=report.get('total_files'),
                processed=report.get('processed'),
                errors=report.get('errors'),
                payload=report
            )
            session.add(pr)
            session.commit()
            logger.info(f"Processing report inserted id={pr.id}")
            return {'success': True, 'id': pr.id}
        except Exception:
            session.rollback()
            logger.exception("Erreur insertion processing report")
            return {'success': False, 'error': 'insertion_failed'}
        finally:
            session.close()

    def insert_validation_report(self, report: dict):
        """Insère un rapport de validation dans `validation_reports`."""
        session = self.Session()
        try:
            vr = ValidationReport(
                source=report.get('source', 'unknown'),
                statistiques=report.get('statistiques'),
                repartition_erreurs=report.get('repartition_erreurs'),
                erreurs_localisation=report.get('erreurs_localisation'),
                details=report.get('details_validations') if report.get('details_validations') is not None else report.get('details')
            )
            session.add(vr)
            session.commit()
            logger.info(f"Validation report inserted id={vr.id}")
            return {'success': True, 'id': vr.id}
        except Exception:
            session.rollback()
            logger.exception("Erreur insertion validation report")
            return {'success': False, 'error': 'insertion_failed'}
        finally:
            session.close()

    def upsert_dashboard_metrics(self, key: str, metrics: dict):
        """Insère ou met à jour les métriques du dashboard dans `dashboard_metrics`.

        key: identifiant (e.g. 'latest')
        metrics: dict JSON serializable
        """
        session = self.Session()
        try:
            stmt = insert(DashboardMetric).values(
                key=key,
                last_update=datetime.now(),
                metrics=metrics
            ).on_conflict_do_update(
                index_elements=['key'],
                set_={
                    'last_update': datetime.now(),
                    'metrics': stmt.excluded.metrics
                }
            )
            session.execute(stmt)
            session.commit()
            logger.info(f"Dashboard metrics upserted key={key}")
            return {'success': True}
        except Exception:
            session.rollback()
            logger.exception("Erreur upsert dashboard metrics")
            return {'success': False, 'error': 'upsert_failed'}
        finally:
            session.close()
    
    def get_dashboard_data(self):
        """Retourne toutes les données pour le dashboard"""
        general = self.update_general_stats()
        performance = self.update_performance_metrics()
        failures = self.get_failures_stats()
        ips = self.get_ips_stats()
        dags = self.get_dags_stats()
        
        return {
            **general,
            **performance,
            **failures,
            **ips,
            **dags
        }
    
    # === Gestion de la file d'attente avec priorités ===
    
    def add_to_queue(self, enterprise_number, priority=1, requested_by='system'):
        """Ajoute une entreprise à la file d'attente.
        
        Args:
            enterprise_number: Numéro d'entreprise
            priority: 1=normal, 2=haute (recherche manuelle), 3=très haute
            requested_by: 'system', 'user', 'manual'
        
        Returns:
            dict: Résultat de l'ajout
        """
        session = self.Session()
        try:
            # Vérifier si l'entreprise est déjà en queue
            existing = session.query(ScrapingQueue).filter(
                ScrapingQueue.enterprise_number == enterprise_number
            ).first()
            
            if existing:
                # Si existe et que la nouvelle priorité est plus haute, mettre à jour
                if priority > existing.priority:
                    existing.priority = priority
                    existing.requested_by = requested_by
                    existing.added_at = datetime.now()
                    session.commit()
                    logger.info(f"Priorité augmentée pour {enterprise_number}: {priority}")
                    return {'success': True, 'action': 'priority_updated', 'enterprise_number': enterprise_number}
                else:
                    return {'success': True, 'action': 'already_queued', 'enterprise_number': enterprise_number}
            
            # Vérifier si déjà scrapé (existe dans la table entreprises)
            entreprise_exists = session.query(Entreprise).filter(
                Entreprise.numero_entreprise == enterprise_number
            ).first()
            
            if entreprise_exists:
                return {'success': True, 'action': 'already_scraped', 'enterprise_number': enterprise_number}
            
            # Ajouter à la queue
            queue_item = ScrapingQueue(
                enterprise_number=enterprise_number,
                priority=priority,
                status='pending',
                requested_by=requested_by
            )
            session.add(queue_item)
            session.commit()
            
            logger.info(f"Ajouté à la queue: {enterprise_number} (priorité: {priority})")
            return {'success': True, 'action': 'added', 'enterprise_number': enterprise_number}
            
        except Exception as e:
            session.rollback()
            logger.exception(f"Erreur ajout à la queue: {e}")
            return {'success': False, 'error': str(e)}
        finally:
            session.close()
    
    def get_queue_stats(self):
        """Récupère les statistiques de la file d'attente."""
        session = self.Session()
        try:
            total_pending = session.query(ScrapingQueue).filter(
                ScrapingQueue.status == 'pending'
            ).count()
            
            total_processing = session.query(ScrapingQueue).filter(
                ScrapingQueue.status == 'processing'
            ).count()
            
            total_completed = session.query(ScrapingQueue).filter(
                ScrapingQueue.status == 'completed'
            ).count()
            
            total_failed = session.query(ScrapingQueue).filter(
                ScrapingQueue.status == 'failed'
            ).count()
            
            high_priority = session.query(ScrapingQueue).filter(
                ScrapingQueue.status == 'pending',
                ScrapingQueue.priority >= 2
            ).count()
            
            return {
                'total_pending': total_pending,
                'total_processing': total_processing,
                'total_completed': total_completed,
                'total_failed': total_failed,
                'high_priority': high_priority,
                'total_queue': total_pending + total_processing
            }
        finally:
            session.close()
    
    def get_queue_items(self, status='pending', limit=100, page=1, per_page=20):
        """Récupère les éléments de la file d'attente.
        
        Args:
            status: Filtre par statut (pending, processing, completed, failed)
            limit: Limite de résultats (deprecated, use pagination)
            page: Numéro de page
            per_page: Éléments par page
        
        Returns:
            dict: {total, page, per_page, items}
        """
        session = self.Session()
        try:
            query = session.query(ScrapingQueue)
            
            if status:
                query = query.filter(ScrapingQueue.status == status)
            
            # Trier par priorité décroissante, puis par date d'ajout
            query = query.order_by(
                ScrapingQueue.priority.desc(),
                ScrapingQueue.added_at.asc()
            )
            
            total = query.count()
            offset = max(0, (page - 1) * per_page)
            items = query.limit(per_page).offset(offset).all()
            
            results = []
            for item in items:
                results.append({
                    'id': item.id,
                    'enterprise_number': item.enterprise_number,
                    'priority': item.priority,
                    'priority_label': self._get_priority_label(item.priority),
                    'status': item.status,
                    'added_at': item.added_at.isoformat() if item.added_at else None,
                    'started_at': item.started_at.isoformat() if item.started_at else None,
                    'completed_at': item.completed_at.isoformat() if item.completed_at else None,
                    'attempts': item.attempts,
                    'last_error': item.last_error,
                    'requested_by': item.requested_by
                })
            
            return {
                'total': total,
                'page': page,
                'per_page': per_page,
                'items': results
            }
        finally:
            session.close()
    
    def _get_priority_label(self, priority):
        """Retourne le label de priorité."""
        if priority >= 3:
            return 'Très haute'
        elif priority == 2:
            return 'Haute'
        else:
            return 'Normale'
    
    def update_queue_item_status(self, enterprise_number, status, error=None):
        """Met à jour le statut d'un élément de la queue."""
        session = self.Session()
        try:
            item = session.query(ScrapingQueue).filter(
                ScrapingQueue.enterprise_number == enterprise_number
            ).first()
            
            if not item:
                return {'success': False, 'error': 'not_found'}
            
            item.status = status
            
            if status == 'processing':
                item.started_at = datetime.now()
                item.attempts += 1
            elif status == 'completed':
                item.completed_at = datetime.now()
            elif status == 'failed':
                item.completed_at = datetime.now()
                item.last_error = error
            
            session.commit()
            return {'success': True, 'enterprise_number': enterprise_number}
            
        except Exception as e:
            session.rollback()
            logger.exception(f"Erreur mise à jour queue: {e}")
            return {'success': False, 'error': str(e)}
        finally:
            session.close()
    
    def remove_from_queue(self, enterprise_number):
        """Supprime une entreprise de la file d'attente."""
        session = self.Session()
        try:
            item = session.query(ScrapingQueue).filter(
                ScrapingQueue.enterprise_number == enterprise_number
            ).first()
            
            if not item:
                return {'success': False, 'error': 'not_found'}
            
            session.delete(item)
            session.commit()
            
            logger.info(f"Supprimé de la queue: {enterprise_number}")
            return {'success': True, 'enterprise_number': enterprise_number}
            
        except Exception as e:
            session.rollback()
            logger.exception(f"Erreur suppression queue: {e}")
            return {'success': False, 'error': str(e)}
        finally:
            session.close()
    
    def get_next_to_scrape(self, limit=1):
        """Récupère les prochaines entreprises à scraper (priorité décroissante)."""
        session = self.Session()
        try:
            items = session.query(ScrapingQueue).filter(
                ScrapingQueue.status == 'pending'
            ).order_by(
                ScrapingQueue.priority.desc(),
                ScrapingQueue.added_at.asc()
            ).limit(limit).all()
            
            results = []
            for item in items:
                results.append({
                    'enterprise_number': item.enterprise_number,
                    'priority': item.priority,
                    'added_at': item.added_at.isoformat() if item.added_at else None
                })
            
            return results
        finally:
            session.close()


def update_dashboard_stats(data_dir=None):
    """Fonction helper pour mise à jour rapide"""
    if not data_dir:
        data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
    
    collector = DashboardCollector(data_dir)
    return collector.update_general_stats()
    collector = DashboardCollector(data_dir)
    return collector.update_general_stats()
    collector = DashboardCollector(data_dir)
    return collector.update_general_stats()
