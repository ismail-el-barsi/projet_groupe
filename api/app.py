"""
Application Flask pour le frontend KBO Dashboard
"""
import os
import sys
from datetime import datetime

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS

# Charger les variables d'environnement depuis .env
load_dotenv()

# Ajouter le chemin des services
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'services'))

from dashboard_collector import DashboardCollector

from config import Config

app = Flask(__name__)
app.config.from_object(Config)
CORS(app)

# Configuration
DATA_DIR = app.config['DATA_DIR']

print(f"📊 Configuration Flask:")
print(f"   - DATA_DIR: {DATA_DIR}")

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


@app.route('/')
def index():
    """Page d'accueil avec recherche"""
    return render_template('index.html')


@app.route('/entreprise/<numero>')
def entreprise_detail(numero):
    """Page de détails d'une entreprise"""
    return render_template('entreprise.html', numero=numero)


@app.route('/dashboard')
def dashboard():
    """Dashboard administrateur"""
    return render_template('dashboard.html')


@app.route('/api/search')
def api_search():
    """API de recherche d'entreprises"""
    query = request.args.get('q', '').strip()
    
    try:
        # Si aucune query, retourner toutes les entreprises (limitées)
        if not query:
            results = collector.list_all_entreprises(limit=100)
        else:
            # Recherche dans la BDD PostgreSQL
            results = collector.search_entreprises(query, limit=50)
        
        # Les données viennent de la BDD, donc déjà disponibles
        for result in results:
            result['is_scraped'] = True  # Si en BDD, c'est déjà scrapé et traité
            result['status_display'] = result.get('status', 'Inconnu')
        
        return jsonify({
            'success': True,
            'query': query,
            'count': len(results),
            'results': results
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
        stats = collector.get_ips_stats()
        return jsonify({
            'success': True,
            'data': stats
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


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
