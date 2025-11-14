from bs4 import BeautifulSoup
import json
import os
from datetime import datetime


class KBOHTMLParser:
    """Parser pour extraire les données des pages HTML KBO."""
    
    def __init__(self, html_content):
        self.soup = BeautifulSoup(html_content, 'html.parser')
        
    def extract_data(self):
        """Extrait toutes les données principales du HTML."""
        data = {
            'extraction_date': datetime.now().isoformat(),
            'presentation': self._extract_presentation(),
            'informations_juridiques': self._extract_legal_info(),
            'activite_detaillee': self._extract_detailed_activity(),
            'dirigeants': self._extract_directors(),
            'hoedanigheden': self._extract_hoedanigheden(),
            'activites_btw': self._extract_btw_activities(),
            'activites_rsz': self._extract_rsz_activities()
        }
        return data
    
    def _extract_presentation(self):
        """Extrait les informations de présentation."""
        presentation = {}
        
        # Numéro d'entreprise
        numero_elem = self.soup.find('td', string='Ondernemingsnummer:')
        if numero_elem:
            presentation['numero_entreprise'] = numero_elem.find_next_sibling('td').text.strip()
        
        # Dénomination
        nom_elem = self.soup.find('td', string='Naam:')
        if nom_elem:
            nom_td = nom_elem.find_next_sibling('td')
            presentation['denomination'] = nom_td.contents[0].strip() if nom_td.contents else nom_td.text.strip()
        
        # Adresse principale
        adresse_elem = self.soup.find('td', string='Adres van de zetel:')
        if adresse_elem:
            adresse_td = adresse_elem.find_next_sibling('td')
            adresse_text = adresse_td.get_text(separator=' ', strip=True)
            presentation['adresse_principale'] = adresse_text.split('Sinds')[0].strip()
        
        # Status
        status_elem = self.soup.find('td', string='Status:')
        if status_elem:
            status_td = status_elem.find_next_sibling('td')
            presentation['status'] = status_td.get_text(strip=True)
        
        # Date de création
        date_elem = self.soup.find('td', string='Begindatum:')
        if date_elem:
            presentation['date_creation'] = date_elem.find_next_sibling('td').get_text(strip=True).split('\n')[0]
        
        # Contact - Téléphone
        tel_elem = self.soup.find('td', string=lambda x: x and 'Telefoonnummer' in x)
        if tel_elem:
            tel_td = tel_elem.find_next_sibling('td')
            if tel_td:
                tel_table = tel_td.find('table')
                if tel_table:
                    tel_text = tel_table.get_text(strip=True).split('Sinds')[0].strip()
                    if tel_text and tel_text != 'Geen gegevens opgenomen in KBO.':
                        presentation['telephone'] = tel_text
        
        # Contact - Fax
        fax_elem = self.soup.find('td', string=lambda x: x and 'Faxnummer' in x)
        if fax_elem:
            fax_td = fax_elem.find_next_sibling('td')
            if fax_td:
                fax_table = fax_td.find('table')
                if fax_table:
                    fax_text = fax_table.get_text(strip=True).split('Sinds')[0].strip()
                    if fax_text and fax_text != 'Geen gegevens opgenomen in KBO.':
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
        web_elem = self.soup.find('td', string=lambda x: x and 'Webadres' in x)
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
        forme_elem = self.soup.find('td', string=lambda x: x and 'Rechtsvorm' in x)
        if forme_elem:
            forme_td = forme_elem.find_next_sibling('td')
            if forme_td:
                # Prendre le premier élément texte
                forme_text = forme_td.get_text(strip=True).split('Sinds')[0].strip()
                legal_info['forme_juridique'] = forme_text
        
        # Type d'entité
        type_elem = self.soup.find('td', string=lambda x: x and 'Type entiteit' in x)
        if type_elem:
            legal_info['type_entite'] = type_elem.find_next_sibling('td').text.strip()
        
        # Situation juridique
        situation_elem = self.soup.find('td', string=lambda x: x and 'Rechtstoestand' in x)
        if situation_elem:
            situation_td = situation_elem.find_next_sibling('td')
            legal_info['situation_juridique'] = situation_td.get_text(separator=' ', strip=True)
        
        # Numéro de TVA
        numero_elem = self.soup.find('td', string='Ondernemingsnummer:')
        if numero_elem:
            numero = numero_elem.find_next_sibling('td').text.strip()
            legal_info['numero_tva'] = numero.replace('.', '')
        
        # Siège social (SIRET)
        siret_elem = self.soup.find('td', string=lambda x: x and 'SIRET' in str(x))
        if siret_elem:
            legal_info['siret_siege'] = siret_elem.find_next_sibling('td').text.strip()
        
        # Nombre d'établissements
        ve_elem = self.soup.find('td', string=lambda x: x and 'Aantal vestigingseenheden' in str(x))
        if ve_elem:
            ve_td = ve_elem.find_next_sibling('td')
            if ve_td:
                strong = ve_td.find('strong')
                if strong:
                    legal_info['nombre_etablissements'] = int(strong.text.strip())
        
        # Capital social
        capital_elem = self.soup.find('td', string=lambda x: x and 'Capital social' in str(x))
        if capital_elem:
            legal_info['capital_social'] = capital_elem.find_next_sibling('td').text.strip()
        
        return legal_info
    
    def _extract_detailed_activity(self):
        """Extrait l'activité détaillée."""
        activity = {}
        
        # Activités BTW (TVA)
        btw_activities = []
        btw_section = self.soup.find('h2', string='Btw-activiteiten Nacebelcode versie 2025')
        if btw_section:
            btw_table = btw_section.find_parent('table')
            btw_rows = btw_table.find_all('td', class_='QL')
            for row in btw_rows:
                text = row.get_text(strip=True)
                if 'Btw' in text and 'Nacebelcode' not in text:
                    btw_activities.append(text)
        
        activity['activites_btw'] = btw_activities
        
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
                        'date_nomination': cells[2].get_text(strip=True).replace('Sinds ', '')
                    }
                    directors.append(director)
        
        return directors
    
    def _extract_hoedanigheden(self):
        """Extrait les hoedanigheden (qualités)."""
        hoedanigheden = []
        
        # Chercher la section Hoedanigheden
        hoedanigheden_header = self.soup.find('h2', string='Hoedanigheden')
        if hoedanigheden_header:
            table = hoedanigheden_header.find_parent('table')
            # Trouver toutes les lignes après le header
            current = hoedanigheden_header.find_parent('tr')
            while current:
                current = current.find_next_sibling('tr')
                if not current:
                    break
                td = current.find('td', class_=['QL', 'RL'])
                if td:
                    text = td.get_text(strip=True)
                    # Ignorer les cellules vides et les sections suivantes
                    if (text and 
                        not text.startswith('&nbsp;') and 
                        'Toelatingen' not in text and
                        'Geen gegevens' not in text and
                        'Btw' not in text and
                        'RSZ' not in text and
                        len(text) > 3):
                        hoedanigheden.append(text)
                    if 'Toelatingen' in text or 'Btw-activiteiten' in text:
                        break
        
        return hoedanigheden
    
    def _extract_btw_activities(self):
        """Extrait les activités BTW avec codes NACE."""
        activities = []
        
        btw_header = self.soup.find('h2', string=lambda x: x and 'Btw-activiteiten Nacebelcode versie 2025' in x)
        if btw_header:
            current = btw_header.find_parent('tr')
            while current:
                current = current.find_next_sibling('tr')
                if not current:
                    break
                td = current.find('td', class_='QL')
                if td:
                    text = td.get_text(strip=True)
                    if 'Btw' in text and 'Nacebelcode' not in text:
                        # Extraire le code NACE et la description
                        link = td.find('a')
                        if link:
                            href = link.get('href', '')
                            code = href.split('nace.code=')[1].split('&')[0] if 'nace.code=' in href else ''
                            description = link.text.strip()
                            # Extraire la date
                            date_match = text.split('Sinds')[-1].strip() if 'Sinds' in text else ''
                            activities.append({
                                'code_nace': code,
                                'description': description,
                                'date_debut': date_match
                            })
                    if 'RSZ' in text or 'Toon de activiteiten' in text:
                        break
        
        return activities
    
    def _extract_rsz_activities(self):
        """Extrait les activités RSZ avec codes NACE."""
        activities = []
        
        rsz_header = self.soup.find('h2', string=lambda x: x and 'RSZ-activiteiten Nacebelcode versie 2025' in x)
        if rsz_header:
            current = rsz_header.find_parent('tr')
            while current:
                current = current.find_next_sibling('tr')
                if not current:
                    break
                td = current.find('td', class_='QL')
                if td:
                    text = td.get_text(strip=True)
                    if 'RSZ' in text and 'Nacebelcode' not in text:
                        # Extraire le code NACE et la description
                        link = td.find('a')
                        if link:
                            href = link.get('href', '')
                            code = href.split('nace.code=')[1].split('&')[0] if 'nace.code=' in href else ''
                            description = link.text.strip()
                            # Extraire la date
                            date_match = text.split('Sinds')[-1].strip() if 'Sinds' in text else ''
                            activities.append({
                                'code_nace': code,
                                'description': description,
                                'date_debut': date_match
                            })
                    if 'Toon de activiteiten' in text:
                        break
        
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
