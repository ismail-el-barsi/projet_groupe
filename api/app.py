"""
Application Flask pour le frontend KBO Dashboard
"""
import os
import sys
from datetime import datetime

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request, redirect, url_for
from flask_cors import CORS
from flask_socketio import SocketIO, emit

# Charger les variables d'environnement depuis .env
load_dotenv()

# Ajouter le chemin des services
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'services'))

from dashboard_collector import DashboardCollector
from queue_manager import QueueManager
from redis_broadcaster import RedisBroadcaster

from config import Config

app = Flask(__name__)
app.config.from_object(Config)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
CORS(app)

# Initialiser SocketIO
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Configuration
DATA_DIR = app.config['DATA_DIR']

print(f"📊 Configuration Flask:")
print(f"   - DATA_DIR: {DATA_DIR}")
print(f"   - WebSocket: Activé")

# Remplacer postgres_kbo par localhost dans l'environnement Docker
# pour que Flask puisse se connecter depuis l'hôte
os.environ['POSTGRES_KBO_HOST'] = app.config['POSTGRES_HOST']

# Initialiser le collector - il utilisera la même connexion que les DAGs
# mais via localhost:5433 au lieu de postgres_kbo:5432
from sqlalchemy import create_engine

# Créer une connexion spéciale pour Flask qui utilise localhost
flask_db_uri = (
    f"postgresql+psycopg2://{app.config['POSTGRES_USER']}:{app.config['POSTGRES_PASSWORD']}"
    f"@{app.config['POSTGRES_HOST']}:{app.config['POSTGRES_PORT']}/{app.config['POSTGRES_DB']}"
)

# Monkey patch temporaire pour le collector
import dashboard_collector as dc_module

original_init = dc_module.DashboardCollector.__init__

def patched_init(self, data_dir):
    self.data_dir = data_dir
    self.queue_file = os.path.join(data_dir, "enterprise_queue.json")
    self.failed_file = os.path.join(data_dir, "failed_enterprises.json")
    self.html_dir = os.path.join(data_dir, "html_pages")
    self.locks_dir = os.path.join(data_dir, "locks")
    
    # Utiliser l'URI Flask au lieu de postgres_kbo
    self.engine = create_engine(flask_db_uri)
    from sqlalchemy.orm import sessionmaker
    dc_module.Base.metadata.create_all(self.engine, checkfirst=True)
    self.Session = sessionmaker(bind=self.engine)

dc_module.DashboardCollector.__init__ = patched_init

collector = DashboardCollector(DATA_DIR)
queue_manager = QueueManager()  # Gestionnaire de queue Redis

# Initialiser le broadcaster Redis -> WebSocket
broadcaster = RedisBroadcaster(queue_manager, socketio)
broadcaster.start()  # Démarre la surveillance en arrière-plan


@app.route('/')
def index():
    """Page d'accueil avec recherche"""
    return render_template('index.html')


@app.route('/entreprise/<numero>')
def entreprise_detail(numero):
    """Page de détails d'une entreprise"""
    return render_template('entreprise.html', numero=numero)


@app.route('/entreprise/<numero>/original')
def entreprise_original(numero):
    """Redirige vers la page publique KBO pour le numéro d'entreprise donné"""
    # Normaliser le numéro (supprimer les points)
    numero_clean = numero.replace('.', '').replace(' ', '')
    # Utiliser le format de recherche KBO (numéro sans points)
    kbo_url = f"https://kbopub.economie.fgov.be/kbopub/zoeknummerform.html?lang=fr&nummer={numero_clean}&actionLu=Rechercher"
    return redirect(kbo_url)


@app.route('/dashboard')
def dashboard():
    """Dashboard administrateur"""
    return render_template('dashboard.html')


@app.route('/api/search')
def api_search():
    """API de recherche d'entreprises"""
    query = request.args.get('q', '').strip()
    # pagination
    try:
        page = int(request.args.get('page', '1'))
        per_page = int(request.args.get('per_page', '20'))
    except ValueError:
        page = 1
        per_page = 20
    
    try:
        # Si aucune query, utiliser la liste paginée
        if not query:
            page_data = collector.list_all_entreprises_paginated(page=page, per_page=per_page)
        else:
            # Recherche paginée dans la BDD PostgreSQL
            page_data = collector.search_entreprises_paginated(query, page=page, per_page=per_page)
            
            # Si aucun résultat et que la query ressemble à un numéro d'entreprise
            # Ajouter à la queue avec priorité haute (recherche manuelle)
            if page_data['total'] == 0 and query:
                # Vérifier si c'est un format de numéro valide (avec ou sans points)
                import re
                numero_clean = query.replace('.', '').replace(' ', '')
                if re.match(r'^\d{10}$', numero_clean):
                    # Formater correctement
                    numero_formatted = f"{numero_clean[:4]}.{numero_clean[4:7]}.{numero_clean[7:]}"
                    # Ajouter à la queue Redis avec priorité haute
                    result = queue_manager.add_to_queue(
                        enterprise_number=numero_formatted,
                        priority=2,  # Priorité haute car recherche manuelle
                        requested_by='user_search'
                    )
                    
                    # Créer un résultat virtuel pour afficher l'entreprise en attente
                    if result['success']:
                        page_data['results'] = [{
                            'numero_entreprise': numero_formatted,
                            'denomination': 'En cours de chargement...',
                            'adresse': 'Entreprise ajoutée à la file d\'attente',
                            'forme_juridique': '-',
                            'status': 'pending',
                            'status_display': 'En attente',
                            'is_scraped': False,
                            'in_queue': True,
                            'queue_priority': 2,
                            'queue_status': 'pending'
                        }]
                        page_data['total'] = 1
                        page_data['count'] = 1

        # Marquer les résultats comme scrapés et enrichir avec info queue
        for result in page_data['results']:
            # Vérifier si l'entreprise est en queue AVANT de marquer comme scraped
            numero = result.get('numero_entreprise')
            queue_info = None
            
            if numero:
                queue_info = queue_manager.get_item_metadata(numero)
                if queue_info:
                    result['in_queue'] = True
                    result['queue_priority'] = queue_info.get('priority', 1)
                    result['queue_status'] = queue_info.get('status', 'pending')
                    # Si en queue, pas encore scrapé
                    result['is_scraped'] = False
                else:
                    result['in_queue'] = False
                    result['queue_priority'] = 1
                    result['queue_status'] = None
                    # Si pas en queue et dans la BDD, c'est scrapé
                    result['is_scraped'] = True
            else:
                result['is_scraped'] = True
                result['in_queue'] = False
                result['queue_priority'] = 1
                result['queue_status'] = None
            
            result['status_display'] = result.get('status', 'Inconnu')

        return jsonify({
            'success': True,
            'query': query,
            'total': page_data.get('total', 0),
            'page': page_data.get('page', page),
            'per_page': page_data.get('per_page', per_page),
            'count': len(page_data.get('results', [])),
            'results': page_data.get('results', [])
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/entreprise/<numero>')
def api_entreprise(numero):
    """API pour récupérer les détails d'une entreprise depuis PostgreSQL"""
    try:
        # Nettoyer le numéro (enlever les points)
        numero_clean = numero.replace('.', '')
        numero_formatted = f"{numero_clean[:4]}.{numero_clean[4:7]}.{numero_clean[7:]}"
        
        # Récupérer depuis la BDD PostgreSQL
        entreprise = collector.get_entreprise(numero_formatted)
        
        if not entreprise:
            return jsonify({
                'success': False,
                'error': 'not_found',
                'message': 'Entreprise non trouvée dans la base de données'
            }), 404
        
        return jsonify({
            'success': True,
            'entreprise': entreprise
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/dashboard/stats')
def api_dashboard_stats():
    """API pour les statistiques du dashboard"""
    try:
        # Récupérer toutes les stats
        stats = collector.get_dashboard_data()
        
        # Ajouter les stats de la queue Redis
        queue_stats = queue_manager.get_queue_stats()
        stats['queue'] = queue_stats
        
        # Ajouter timestamp
        stats['timestamp'] = datetime.now().isoformat()
        
        return jsonify({
            'success': True,
            'data': stats
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/dashboard/validation')
def api_validation_report():
    """API pour récupérer le dernier rapport de validation"""
    try:
        session = collector.Session()
        from dashboard_collector import ValidationReport

        # Récupérer le dernier rapport
        report = session.query(ValidationReport).order_by(
            ValidationReport.created_at.desc()
        ).first()
        
        session.close()
        
        if not report:
            return jsonify({
                'success': False,
                'message': 'Aucun rapport de validation disponible'
            }), 404
        
        return jsonify({
            'success': True,
            'report': {
                'date': report.created_at.isoformat(),
                'source': report.source,
                'statistiques': report.statistiques,
                'repartition_erreurs': report.repartition_erreurs
            }
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/dashboard/proxies')
def api_proxies_stats():
    """API pour les statistiques des proxies"""
    try:
        # pagination support
        try:
            page = int(request.args.get('page', '1'))
            per_page = int(request.args.get('per_page', '20'))
        except ValueError:
            page = 1
            per_page = 20

        page_data = collector.get_ips_stats_paginated(page=page, per_page=per_page)
        return jsonify({
            'success': True,
            'total': page_data.get('total', 0),
            'page': page_data.get('page', page),
            'per_page': page_data.get('per_page', per_page),
            'items': page_data.get('items', [])
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/stats/count')
def api_stats_count():
    """API pour compter les entreprises dans la BDD PostgreSQL"""
    try:
        # Tout est dans la BDD PostgreSQL maintenant
        count = collector.get_all_entreprises_count()
        
        return jsonify({
            'success': True,
            'total_in_db': count,
            'total_scraped': count  # Même valeur car tout ce qui est en BDD a été scrapé et traité
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/queue/stats')
def api_queue_stats():
    """API pour les statistiques de la file d'attente (Redis)"""
    try:
        stats = queue_manager.get_queue_stats()
        return jsonify({
            'success': True,
            'stats': stats
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/queue/items')
def api_queue_items():
    """API pour récupérer les éléments de la file d'attente (Redis)"""
    try:
        status = request.args.get('status', 'pending')
        page = int(request.args.get('page', '1'))
        per_page = int(request.args.get('per_page', '20'))
        
        offset = (page - 1) * per_page
        items = queue_manager.get_queue_items(
            status=status,
            limit=per_page,
            offset=offset
        )
        
        # Calculer le total depuis les stats
        stats = queue_manager.get_queue_stats()
        total = {
            'pending': stats['total_pending'],
            'processing': stats['total_processing'],
            'completed': stats['total_completed']
        }.get(status, 0)
        
        return jsonify({
            'success': True,
            'total': total,
            'page': page,
            'per_page': per_page,
            'items': items
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/queue/add', methods=['POST'])
def api_queue_add():
    """API pour ajouter une entreprise à la file d'attente avec priorité (Redis)"""
    try:
        data = request.get_json()
        
        if not data or 'enterprise_number' not in data:
            return jsonify({
                'success': False,
                'error': 'enterprise_number requis'
            }), 400
        
        enterprise_number = data['enterprise_number']
        priority = data.get('priority', 2)  # Par défaut priorité haute (recherche manuelle)
        requested_by = data.get('requested_by', 'user')
        
        result = queue_manager.add_to_queue(
            enterprise_number=enterprise_number,
            priority=priority,
            requested_by=requested_by
        )
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/queue/remove/<enterprise_number>', methods=['DELETE'])
def api_queue_remove(enterprise_number):
    """API pour supprimer une entreprise de la file d'attente (Redis)"""
    try:
        result = queue_manager.remove_from_queue(enterprise_number)
        return jsonify(result)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/queue/dag-assignments')
def api_queue_dag_assignments():
    """API pour voir quelles entreprises sont assignées à quels DAGs"""
    try:
        assignments = queue_manager.get_enterprises_by_dag()
        return jsonify({
            'success': True,
            'assignments': assignments,
            'total_dags': len(assignments)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================================
# WebSocket Events
# ============================================================================

@socketio.on('connect')
def handle_connect():
    """Client connecté au WebSocket"""
    print('🔌 Client WebSocket connecté')
    emit('connected', {'status': 'connected'})


@socketio.on('disconnect')
def handle_disconnect():
    """Client déconnecté du WebSocket"""
    print('🔌 Client WebSocket déconnecté')


@socketio.on('subscribe_queue')
def handle_subscribe_queue():
    """Client s'abonne aux mises à jour de la queue Redis"""
    print('📡 Client abonné aux mises à jour de la queue')
    # Envoyer immédiatement les stats actuelles
    try:
        stats = queue_manager.get_queue_stats()
        emit('queue_stats_update', stats)
    except Exception as e:
        print(f'Erreur lors de l\'envoi des stats: {e}')


@socketio.on('subscribe_dag_assignments')
def handle_subscribe_dag_assignments():
    """Client s'abonne aux assignments DAG"""
    print('📡 Client abonné aux assignments DAG')
    try:
        assignments = queue_manager.get_enterprises_by_dag()
        emit('dag_assignments_update', assignments)
    except Exception as e:
        print(f'Erreur lors de l\'envoi des assignments: {e}')


def broadcast_queue_update():
    """Diffuser une mise à jour de la queue à tous les clients connectés"""
    try:
        stats = queue_manager.get_queue_stats()
        socketio.emit('queue_stats_update', stats, broadcast=True)
    except Exception as e:
        print(f'Erreur broadcast queue: {e}')


def broadcast_dag_assignments_update():
    """Diffuser une mise à jour des assignments DAG"""
    try:
        assignments = queue_manager.get_enterprises_by_dag()
        socketio.emit('dag_assignments_update', assignments, broadcast=True)
    except Exception as e:
        print(f'Erreur broadcast assignments: {e}')


if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, allow_unsafe_werkzeug=True)
