import json
import os
import re
from datetime import datetime

from bs4 import BeautifulSoup


class KBOHTMLParser:
    """Parser pour extraire les données des pages HTML KBO."""
    
    def __init__(self, html_content):
        self.soup = BeautifulSoup(html_content, 'html.parser')

    @staticmethod
    def _clean_whitespace(text: str) -> str:
        """Compresse les espaces et retours à la ligne superflus."""
        return re.sub(r'\s+', ' ', text).strip()
        
    def extract_data(self):
        """Extrait toutes les données principales du HTML."""
        data = {
            'extraction_date': datetime.now().isoformat(),
            'presentation': self._extract_presentation(),
            'informations_juridiques': self._extract_legal_info(),
            'activite_detaillee': self._extract_detailed_activity(),
            'dirigeants': self._extract_directors(),
            'qualites': self._extract_qualites(),
            'activites_tva': self._extract_activites_tva(),
            'activites_onss': self._extract_activites_onss()
        }
        return data
    
    def _extract_presentation(self):
        """Extrait les informations de présentation."""
        presentation = {}
        
        # Numéro d'entreprise
        numero_elem = self.soup.find('td', string=lambda x: x and "Numéro d'entreprise" in x)
        if numero_elem:
            numero_td = numero_elem.find_next_sibling('td')
            raw_numero = numero_td.get_text(separator=' ', strip=True) if numero_td else ''
            match = re.search(r'\d{4}\.\d{3}\.\d{3}', raw_numero)
            if match:
                presentation['numero_entreprise'] = match.group(0)
        
        # Dénomination
        nom_elem = self.soup.find('td', string=lambda x: x and 'Dénomination' in x)
        if nom_elem:
            nom_td = nom_elem.find_next_sibling('td')
            presentation['denomination'] = nom_td.contents[0].strip() if nom_td.contents else nom_td.text.strip()
        
        # Adresse principale
        adresse_elem = self.soup.find('td', string=lambda x: x and 'Adresse du siège' in x)
        if adresse_elem:
            adresse_td = adresse_elem.find_next_sibling('td')
            adresse_text = adresse_td.get_text(separator=' ', strip=True)
            adresse_text = self._clean_whitespace(adresse_text.split('Depuis')[0])
            if adresse_text and adresse_text.isdigit() and len(adresse_text) < 5:
                adresse_text = f"{adresse_text} (adresse BCE indisponible)"
            presentation['adresse_principale'] = adresse_text
        
        # Status
        status_elem = self.soup.find('td', string=lambda x: x and 'Statut' in x)
        if status_elem:
            status_td = status_elem.find_next_sibling('td')
            presentation['status'] = status_td.get_text(strip=True)
        
        # Date de création
        date_elem = self.soup.find('td', string=lambda x: x and 'Date de début' in x)
        if date_elem:
            date_td = date_elem.find_next_sibling('td')
            raw_date = date_td.get_text(separator=' ', strip=True) if date_td else ''
            date_match = re.search(r'\d{1,2}\s\w+\s\d{4}', raw_date)
            if date_match:
                presentation['date_creation'] = date_match.group(0)
        
        # Contact - Téléphone
        tel_elem = self.soup.find('td', string=lambda x: x and 'Numéro de téléphone' in x)
        if tel_elem:
            tel_td = tel_elem.find_next_sibling('td')
            if tel_td:
                tel_table = tel_td.find('table')
                if tel_table:
                    tel_text = tel_table.get_text(strip=True).split('Depuis')[0].strip()
                    if tel_text and tel_text != 'Pas de données reprises dans la BCE.':
                        presentation['telephone'] = tel_text
        
        # Contact - Fax
        fax_elem = self.soup.find('td', string=lambda x: x and 'Numéro de fax' in x)
        if fax_elem:
            fax_td = fax_elem.find_next_sibling('td')
            if fax_td:
                fax_table = fax_td.find('table')
                if fax_table:
                    fax_text = fax_table.get_text(strip=True).split('Depuis')[0].strip()
                    if fax_text and fax_text != 'Pas de données reprises dans la BCE.':
                        presentation['fax'] = fax_text
        
        # Contact - Email
        email_elem = self.soup.find('td', string=lambda x: x and 'E-mail' in x)
        if email_elem:
            email_td = email_elem.find_next_sibling('td')
            if email_td:
                email_link = email_td.find('a')
                if email_link:
                    presentation['email'] = email_link.text.strip()
        
        # Contact - Web
        web_elem = self.soup.find('td', string=lambda x: x and 'Adresse web' in x)
        if web_elem:
            web_td = web_elem.find_next_sibling('td')
            if web_td:
                web_link = web_td.find('a')
                if web_link:
                    presentation['site_web'] = web_link['href'] if 'href' in web_link.attrs else web_link.text.strip()
        
        return presentation
    
    def _extract_legal_info(self):
        """Extrait les informations juridiques."""
        legal_info = {}
        
        # Forme juridique
        forme_elem = self.soup.find('td', string=lambda x: x and 'Forme légale' in x)
        if forme_elem:
            forme_td = forme_elem.find_next_sibling('td')
            if forme_td:
                # Prendre le premier élément texte
                forme_text = forme_td.get_text(strip=True).split('Depuis')[0].strip()
                legal_info['forme_juridique'] = forme_text
        
        # Type d'entité
        type_elem = self.soup.find('td', string=lambda x: x and "Type d'entité" in x)
        if type_elem:
            legal_info['type_entite'] = type_elem.find_next_sibling('td').text.strip()
        
        # Situation juridique
        situation_elem = self.soup.find('td', string=lambda x: x and 'Situation juridique' in x)
        if situation_elem:
            situation_td = situation_elem.find_next_sibling('td')
            legal_info['situation_juridique'] = situation_td.get_text(separator=' ', strip=True)
        
        # Numéro de TVA
        numero_elem = self.soup.find('td', string=lambda x: x and "Numéro d'entreprise" in x)
        if numero_elem:
            numero_td = numero_elem.find_next_sibling('td')
            raw_numero = numero_td.get_text(separator=' ', strip=True) if numero_td else ''
            match = re.search(r'\d{10}', raw_numero.replace('.', ''))
            if match:
                legal_info['numero_tva'] = match.group(0)
        
        # Siège social (SIRET)
        siret_elem = self.soup.find('td', string=lambda x: x and 'SIRET' in str(x))
        if siret_elem:
            legal_info['siret_siege'] = siret_elem.find_next_sibling('td').text.strip()
        
        # Nombre d'établissements
        ve_elem = self.soup.find('td', string=lambda x: x and "Nombre d'unités d'établissement" in str(x))
        if ve_elem:
            ve_td = ve_elem.find_next_sibling('td')
            if ve_td:
                strong = ve_td.find('strong')
                if strong:
                    legal_info['nombre_etablissements'] = int(strong.text.strip())
        
        # Capital social
        capital_elem = self.soup.find('td', string=lambda x: x and 'Capital' in str(x))
        if capital_elem:
            legal_info['capital_social'] = capital_elem.find_next_sibling('td').text.strip()
        
        return legal_info
    
    def _extract_detailed_activity(self):
        """Extrait l'activité détaillée."""
        activity = {}
        
        # Activités TVA (aperçu textuel)
        tva_activities = []
        tva_section = self.soup.find('h2', string=lambda x: x and 'Activités TVA' in x)
        if tva_section:
            tva_table = tva_section.find_parent('table')
            if tva_table:
                tva_rows = tva_table.find_all('td', class_=['QL', 'RL'])
                for row in tva_rows:
                    text = row.get_text(strip=True)
                    if 'TVA' in text:
                        tva_activities.append(text)
        
        activity['activites_tva'] = tva_activities
        
        return activity
    
    def _extract_directors(self):
        """Extrait les dirigeants et représentants."""
        directors = []
        
        # Trouver la table des fonctions
        fonctions_table = self.soup.find('table', id='toonfctie')
        if fonctions_table:
            rows = fonctions_table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 3:
                    director = {
                        'qualite': cells[0].get_text(strip=True),
                        'nom_prenom': cells[1].get_text(strip=True),
                        'date_nomination': (cells[2].get_text(strip=True)
                                            .replace('Depuis le ', '')
                                            .replace('Depuis ', ''))
                    }
                    directors.append(director)
        
        return directors
    
    def _extract_qualites(self):
        """Extrait les qualités mentionnées dans la fiche."""
        qualites = []
        
        qualites_header = self.soup.find('h2', string='Qualités')
        if qualites_header:
            current = qualites_header.find_parent('tr')
            while current:
                current = current.find_next_sibling('tr')
                if not current:
                    break
                td = current.find('td', class_=['QL', 'RL'])
                if not td:
                    header = current.find('h2')
                    if header:
                        break
                    continue
                text = td.get_text(strip=True)
                if text and text != 'Pas de données reprises dans la BCE.':
                    qualites.append(text)
        
        return qualites
    
    def _extract_activites_tva(self):
        """Extrait les activités TVA avec leurs codes NACE."""
        activities = []
        
        tva_header = self.soup.find('h2', string=lambda x: x and 'Activités TVA' in x)
        if tva_header:
            current = tva_header.find_parent('tr')
            while current:
                current = current.find_next_sibling('tr')
                if not current:
                    break
                td = current.find('td', class_=['QL', 'RL'])
                if not td:
                    header = current.find('h2')
                    if header:
                        break
                    continue
                text = td.get_text(strip=True)
                if 'TVA' not in text:
                    continue
                link = td.find('a')
                code = ''
                description = ''
                if link:
                    href = link.get('href', '')
                    if 'nace.code=' in href:
                        code = href.split('nace.code=')[1].split('&')[0]
                    description = link.get_text(strip=True)
                date_match = text.split('Depuis')[-1].strip() if 'Depuis' in text else ''
                activities.append({
                    'code_nace': code,
                    'description': description,
                    'date_debut': date_match
                })
        
        return activities
    
    def _extract_activites_onss(self):
        """Extrait les activités ONSS avec codes NACE."""
        activities = []
        
        onss_header = self.soup.find('h2', string=lambda x: x and 'Activités ONSS' in x)
        if onss_header:
            current = onss_header.find_parent('tr')
            while current:
                current = current.find_next_sibling('tr')
                if not current:
                    break
                td = current.find('td', class_=['QL', 'RL'])
                if not td:
                    header = current.find('h2')
                    if header:
                        break
                    continue
                text = td.get_text(strip=True)
                if 'ONSS' not in text:
                    continue
                link = td.find('a')
                code = ''
                description = ''
                if link:
                    href = link.get('href', '')
                    if 'nace.code=' in href:
                        code = href.split('nace.code=')[1].split('&')[0]
                    description = link.get_text(strip=True)
                date_match = text.split('Depuis')[-1].strip() if 'Depuis' in text else ''
                activities.append({
                    'code_nace': code,
                    'description': description,
                    'date_debut': date_match
                })
        
        return activities


def parse_html_file(html_file_path):
    """Parse un fichier HTML et retourne les données extraites."""
    with open(html_file_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    parser = KBOHTMLParser(html_content)
    return parser.extract_data()


def save_to_json(data, output_path):
    """Sauvegarde les données extraites en JSON."""
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
