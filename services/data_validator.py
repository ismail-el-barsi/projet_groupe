"""
Validateur de qualité des données extraites
"""
import json
import re
from datetime import datetime
from typing import Dict, List, Any


class DataValidator:
    """Valide la qualité des données extraites des fichiers JSON."""
    
    def __init__(self):
        self.validation_rules = {
            'presentation': {
                'numero_entreprise': {
                    'required': True,
                    'format': r'^\d{4}\.\d{3}\.\d{3}$',  # Format: 0201.543.234
                    'description': 'Format numéro entreprise belge'
                },
                'denomination': {
                    'required': True,
                    'format': r'^.{1,200}$',  # Au moins 1 caractère
                    'description': 'Dénomination non vide'
                },
                'adresse_principale': {
                    'required': True,
                    'format': r'^.{5,}$',  # Au moins 5 caractères
                    'description': 'Adresse complète'
                },
                'status': {
                    'required': True,
                    'format': r'^(Actief|Gestopt|Inactief)$',
                    'description': 'Statut valide'
                },
                'date_creation': {
                    'required': False,
                    'format': r'^\d{1,2}\s\w+\s\d{4}$',  # Format: 26 oktober 1948
                    'description': 'Date au format néerlandais'
                },
                'telephone': {
                    'required': False,
                    'format': r'^\+?[\d\s\-\(\)\/\.]{8,}$',  # Accepte /, . et espaces
                    'description': 'Numéro de téléphone valide'
                },
                'email': {
                    'required': False,
                    'format': r'^[\w\.-]+@[\w\.-]+\.\w+$',
                    'description': 'Email valide'
                }
            },
            'informations_juridiques': {
                'forme_juridique': {
                    'required': True,
                    'format': r'^.{3,}$',
                    'description': 'Forme juridique non vide'
                },
                'type_entite': {
                    'required': True,
                    'format': r'^(Rechtspersoon|Natuurlijk persoon)$',
                    'description': 'Type entité valide'
                },
                'numero_tva': {
                    'required': False,
                    'format': r'^\d{10}$',  # Format: 0201543234 (sans points)
                    'description': 'Numéro TVA à 10 chiffres'
                },
                'nombre_etablissements': {
                    'required': False,
                    'type': int,
                    'min': 0,
                    'description': 'Nombre entier positif'
                }
            }
        }
    
    def validate_field(self, field_name: str, value: Any, rules: Dict) -> Dict:
        """Valide un champ selon les règles définies."""
        errors = []
        
        # Vérifier la présence
        if rules.get('required', False):
            if value is None or (isinstance(value, str) and not value.strip()):
                errors.append(f"{field_name}_manquant")
                return {
                    'field': field_name,
                    'value': value,
                    'valid': False,
                    'errors': errors
                }
        
        # Si le champ est vide mais non requis, c'est OK
        if value is None or (isinstance(value, str) and not value.strip()):
            return {
                'field': field_name,
                'value': value,
                'valid': True,
                'errors': []
            }
        
        # Vérifier le type
        if 'type' in rules:
            if not isinstance(value, rules['type']):
                errors.append(f"{field_name}_type_invalide")
        
        # Vérifier le format (regex)
        if 'format' in rules and isinstance(value, str):
            if not re.match(rules['format'], value):
                errors.append(f"{field_name}_format_invalide")
        
        # Vérifier les valeurs min/max pour les nombres
        if isinstance(value, (int, float)):
            if 'min' in rules and value < rules['min']:
                errors.append(f"{field_name}_valeur_trop_petite")
            if 'max' in rules and value > rules['max']:
                errors.append(f"{field_name}_valeur_trop_grande")
        
        return {
            'field': field_name,
            'value': value,
            'valid': len(errors) == 0,
            'errors': errors
        }
    
    def validate_entity(self, data: Dict) -> Dict:
        """Valide une entité complète."""
        entity_id = data.get('presentation', {}).get('numero_entreprise', 'UNKNOWN')
        all_errors = []
        field_validations = []
        
        # Valider les champs de présentation
        presentation = data.get('presentation', {})
        for field_name, rules in self.validation_rules['presentation'].items():
            value = presentation.get(field_name)
            validation = self.validate_field(field_name, value, rules)
            field_validations.append(validation)
            if not validation['valid']:
                all_errors.extend(validation['errors'])
        
        # Valider les informations juridiques
        juridiques = data.get('informations_juridiques', {})
        for field_name, rules in self.validation_rules['informations_juridiques'].items():
            value = juridiques.get(field_name)
            validation = self.validate_field(field_name, value, rules)
            field_validations.append(validation)
            if not validation['valid']:
                all_errors.extend(validation['errors'])
        
        # Vérifier les dirigeants
        dirigeants = data.get('dirigeants', [])
        if not isinstance(dirigeants, list):
            all_errors.append('dirigeants_format_invalide')
        
        is_valid = len(all_errors) == 0
        
        return {
            'entreprise': entity_id,
            'valide': is_valid,
            'erreurs': all_errors,
            'field_validations': field_validations,
            'validation_date': datetime.now().isoformat()
        }
    
    def validate_all(self, json_files: List[str]) -> Dict:
        """Valide tous les fichiers JSON et génère un rapport."""
        results = []
        total_files = len(json_files)
        valid_count = 0
        error_types = {}
        
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                validation = self.validate_entity(data)
                results.append(validation)
                
                if validation['valide']:
                    valid_count += 1
                
                # Compter les types d'erreurs
                for error in validation['erreurs']:
                    error_types[error] = error_types.get(error, 0) + 1
                
            except Exception as e:
                results.append({
                    'entreprise': json_file,
                    'valide': False,
                    'erreurs': [f'erreur_lecture_fichier: {str(e)}'],
                    'validation_date': datetime.now().isoformat()
                })
        
        # Calculer les statistiques
        invalid_count = total_files - valid_count
        valid_percentage = (valid_count / total_files * 100) if total_files > 0 else 0
        
        # Calculer les champs manquants
        missing_fields = {k: v for k, v in error_types.items() if 'manquant' in k}
        format_errors = {k: v for k, v in error_types.items() if 'format_invalide' in k or 'type_invalide' in k}
        
        missing_fields_percentage = sum(missing_fields.values()) / (total_files * len(self.validation_rules['presentation'])) * 100 if total_files > 0 else 0
        
        report = {
            'date_validation': datetime.now().isoformat(),
            'statistiques': {
                'total_entreprises': total_files,
                'entreprises_valides': valid_count,
                'entreprises_invalides': invalid_count,
                'pourcentage_valides': round(valid_percentage, 2),
                'pourcentage_champs_manquants': round(missing_fields_percentage, 2)
            },
            'repartition_erreurs': error_types,
            'champs_manquants': missing_fields,
            'erreurs_format': format_errors,
            'details_validations': results
        }
        
        return report


def generate_validation_summary(report: Dict) -> str:
    """Génère un résumé textuel du rapport de validation."""
    stats = report['statistiques']
    
    summary = f"""
╔══════════════════════════════════════════════════════════════╗
║         RAPPORT DE VALIDATION DES DONNÉES KBO                ║
╚══════════════════════════════════════════════════════════════╝

📊 STATISTIQUES GLOBALES
  • Total d'entreprises : {stats['total_entreprises']}
  • Entreprises valides : {stats['entreprises_valides']} ({stats['pourcentage_valides']}%)
  • Entreprises invalides : {stats['entreprises_invalides']}
  • % de champs manquants : {stats['pourcentage_champs_manquants']}%

⚠️  RÉPARTITION DES ERREURS PAR TYPE
"""
    
    for error_type, count in sorted(report['repartition_erreurs'].items(), key=lambda x: x[1], reverse=True):
        summary += f"  • {error_type}: {count}\n"
    
    summary += f"\n📅 Date de validation : {report['date_validation']}\n"
    
    return summary
