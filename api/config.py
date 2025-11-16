"""
Configuration pour l'application Flask
"""
import os


class Config:
    """Configuration de base"""
    
    # Flask
    SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
    DEBUG = os.getenv('FLASK_DEBUG', 'True').lower() == 'true'
    
    # Base de données PostgreSQL
    # En mode local (hors Docker), utiliser localhost
    # En mode Docker, utiliser postgres_kbo
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5433')
    POSTGRES_DB = os.getenv('POSTGRES_DB', 'kbo_dashboard')
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'kbo_admin')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'kbo_2025_secure')
    
    # Construire l'URL de connexion
    SQLALCHEMY_DATABASE_URI = (
        f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}'
        f'@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
    )
    
    # Chemins
    DATA_DIR = os.getenv('DATA_DIR', os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data'))
