// Script pour la page de détails d'entreprise
const loadingSpinner = document.getElementById('loadingSpinner');
const errorMessage = document.getElementById('errorMessage');
const entrepriseContent = document.getElementById('entrepriseContent');

// Charger les données de l'entreprise
async function loadEntreprise() {
    try {
        const response = await fetch(`/api/entreprise/${numeroEntreprise}`);
        const data = await response.json();
        
        loadingSpinner.style.display = 'none';
        
        if (!data.success) {
            showError(data.message || 'Entreprise non trouvée');
            return;
        }
        
        displayEntreprise(data.entreprise);
        
    } catch (error) {
        loadingSpinner.style.display = 'none';
        showError(`Erreur de chargement: ${error.message}`);
    }
}

// Afficher une erreur
function showError(message) {
    errorMessage.textContent = message;
    errorMessage.style.display = 'block';
}

// Afficher les données de l'entreprise
function displayEntreprise(entreprise) {
    entrepriseContent.style.display = 'block';
    
    const data = entreprise.data || {};
    const presentation = data.presentation || {};
    const juridique = data.informations_juridiques || {};
    const dirigeants = data.dirigeants || [];
    const qualites = data.qualites || [];
    const activitesTVA = data.activites_tva || [];
    const activitesONSS = data.activites_onss || [];
    
    // En-tête
    document.getElementById('denomination').textContent = presentation.denomination || 'N/A';
    document.getElementById('numeroEntreprise').textContent = presentation.numero_entreprise || numeroEntreprise;
    
    // Badge de statut
    const statusBadge = document.getElementById('statusBadge');
    const status = presentation.status || 'Inconnu';
    statusBadge.textContent = status;
    
    if (status.toLowerCase().includes('actif')) {
        statusBadge.className = 'status-badge actif';
    } else {
        statusBadge.className = 'status-badge inactive';
    }
    
    // Onglet Présentation
    document.getElementById('adresse').textContent = presentation.adresse_principale || 'N/A';
    document.getElementById('dateCreation').textContent = presentation.date_creation || 'N/A';
    document.getElementById('status').textContent = status;
    
    // Contact
    const contactInfo = document.getElementById('contactInfo');
    let contactHTML = '';
    if (presentation.telephone) {
        contactHTML += `<p><strong>☎️ Téléphone:</strong> ${presentation.telephone}</p>`;
    }
    if (presentation.email) {
        contactHTML += `<p><strong>✉️ Email:</strong> <a href="mailto:${presentation.email}">${presentation.email}</a></p>`;
    }
    if (presentation.site_web) {
        contactHTML += `<p><strong>🌐 Site web:</strong> <a href="${presentation.site_web}" target="_blank">${presentation.site_web}</a></p>`;
    }
    if (presentation.fax) {
        contactHTML += `<p><strong>📠 Fax:</strong> ${presentation.fax}</p>`;
    }
    contactInfo.innerHTML = contactHTML || '<p>Aucune information de contact disponible</p>';
    
    // Onglet Juridique
    document.getElementById('formeJuridique').textContent = juridique.forme_juridique || 'N/A';
    document.getElementById('typeEntite').textContent = juridique.type_entite || 'N/A';
    document.getElementById('nombreEtablissements').textContent = juridique.nombre_etablissements !== undefined ? juridique.nombre_etablissements : 'N/A';
    document.getElementById('capitalSocial').textContent = juridique.capital_social || 'N/A';
    document.getElementById('situationJuridique').textContent = juridique.situation_juridique || 'N/A';
    
    // Onglet Activités TVA
    const activitesTVADiv = document.getElementById('activitesTVA');
    if (activitesTVA.length > 0) {
        activitesTVADiv.innerHTML = activitesTVA.map(act => `
            <div class="activity-item">
                <div class="activity-code">Code NACE: ${act.code_nace || 'N/A'}</div>
                <div class="activity-description">${act.description || 'N/A'}</div>
                <div class="activity-date">Depuis: ${act.date_debut || 'N/A'}</div>
            </div>
        `).join('');
    } else {
        activitesTVADiv.innerHTML = '<p>Aucune activité TVA enregistrée</p>';
    }
    
    // Onglet Activités ONSS
    const activitesONSSDiv = document.getElementById('activitesONSS');
    if (activitesONSS.length > 0) {
        activitesONSSDiv.innerHTML = activitesONSS.map(act => `
            <div class="activity-item">
                <div class="activity-code">Code NACE: ${act.code_nace || 'N/A'}</div>
                <div class="activity-description">${act.description || 'N/A'}</div>
                <div class="activity-date">Depuis: ${act.date_debut || 'N/A'}</div>
            </div>
        `).join('');
    } else {
        activitesONSSDiv.innerHTML = '<p>Aucune activité ONSS enregistrée</p>';
    }
    
    // Onglet Dirigeants
    const dirigeantsList = document.getElementById('dirigeantsList');
    if (dirigeants.length > 0) {
        dirigeantsList.innerHTML = dirigeants.map(dir => `
            <div class="dirigeant-item">
                <div class="dirigeant-name">${dir.nom_prenom || 'N/A'}</div>
                <div class="dirigeant-role">${dir.qualite || 'N/A'}</div>
                <div class="dirigeant-date">Nommé le: ${dir.date_nomination || 'N/A'}</div>
            </div>
        `).join('');
    } else {
        dirigeantsList.innerHTML = '<p>Aucun dirigeant enregistré</p>';
    }
    
    // Qualités
    const qualitesList = document.getElementById('qualitesList');
    if (qualites.length > 0) {
        qualitesList.innerHTML = '<ul>' + qualites.map(q => `<li>${q}</li>`).join('') + '</ul>';
    } else {
        qualitesList.innerHTML = '<p>Aucune qualité enregistrée</p>';
    }
}

// Gestion des onglets
document.querySelectorAll('.tab-button').forEach(button => {
    button.addEventListener('click', () => {
        // Retirer la classe active de tous les boutons et contenus
        document.querySelectorAll('.tab-button').forEach(btn => btn.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
        
        // Ajouter la classe active au bouton cliqué
        button.classList.add('active');
        
        // Afficher le contenu correspondant
        const tabId = button.getAttribute('data-tab');
        document.getElementById(tabId).classList.add('active');
    });
});

// Charger les données au chargement de la page
loadEntreprise();
