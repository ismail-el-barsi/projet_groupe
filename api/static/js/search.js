// Script pour la page de recherche
const searchInput = document.getElementById('searchInput');
const searchBtn = document.getElementById('searchBtn');
const showAllBtn = document.getElementById('showAllBtn');
const searchInfo = document.getElementById('searchInfo');
const resultsSection = document.getElementById('resultsSection');
const resultsGrid = document.getElementById('resultsGrid');
const loadingSpinner = document.getElementById('loadingSpinner');

// Fonction de recherche
async function searchEntreprises(query) {
    // Afficher le spinner
    loadingSpinner.style.display = 'block';
    resultsSection.style.display = 'none';
    searchInfo.textContent = '';
    
    try {
        const response = await fetch(`/api/search?q=${encodeURIComponent(query)}`);
        const data = await response.json();
        
        loadingSpinner.style.display = 'none';
        
        if (!data.success) {
            searchInfo.textContent = `Erreur: ${data.error}`;
            searchInfo.style.color = 'var(--danger-color)';
            return;
        }
        
        if (data.count === 0) {
            searchInfo.textContent = 'Aucune entreprise trouvée';
            searchInfo.style.color = 'var(--text-secondary)';
            return;
        }
        
        // Afficher les résultats
        displayResults(data.results);
        searchInfo.textContent = `${data.count} entreprise(s) trouvée(s)`;
        searchInfo.style.color = 'var(--success-color)';
        
    } catch (error) {
        loadingSpinner.style.display = 'none';
        searchInfo.textContent = `Erreur: ${error.message}`;
        searchInfo.style.color = 'var(--danger-color)';
    }
}

// Afficher les résultats
function displayResults(results) {
    resultsGrid.innerHTML = '';
    resultsSection.style.display = 'block';
    
    results.forEach(entreprise => {
        const card = createEntrepriseCard(entreprise);
        resultsGrid.appendChild(card);
    });
}

// Créer une carte d'entreprise
function createEntrepriseCard(entreprise) {
    const card = document.createElement('div');
    card.className = `entreprise-card ${entreprise.is_scraped ? 'scraped' : 'not-scraped'}`;
    
    // Déterminer la classe de statut
    let statusClass = 'actif';
    const status = entreprise.status_display || entreprise.status || 'Inconnu';
    if (status.toLowerCase().includes('dissous') || 
        status.toLowerCase().includes('radié') || 
        status.toLowerCase().includes('clôturé')) {
        statusClass = 'inactive';
    }
    
    card.innerHTML = `
        <div class="card-header">
            <div>
                <div class="card-title">${entreprise.denomination || 'N/A'}</div>
                <div class="card-numero">${entreprise.numero_entreprise}</div>
            </div>
            <span class="card-status ${statusClass}">${status}</span>
        </div>
        <div class="card-body">
            <p><strong>Adresse:</strong> ${entreprise.adresse || 'N/A'}</p>
            <p><strong>Forme juridique:</strong> ${entreprise.forme_juridique || 'N/A'}</p>
        </div>
        <div class="card-footer">
            ${entreprise.is_scraped ? 
                '✅ Données disponibles' : 
                '⏳ En cours de traitement'
            }
        </div>
    `;
    
    // Ajouter l'événement de clic
    card.addEventListener('click', () => {
        window.location.href = `/entreprise/${entreprise.numero_entreprise}`;
    });
    
    return card;
}

// Event listeners
searchBtn.addEventListener('click', () => {
    const query = searchInput.value.trim();
    if (query) {
        searchEntreprises(query);
    }
});

showAllBtn.addEventListener('click', () => {
    searchInput.value = '';
    searchEntreprises('');
});

searchInput.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
        const query = searchInput.value.trim();
        if (query) {
            searchEntreprises(query);
        }
    }
});

// Charger toutes les entreprises au démarrage
window.addEventListener('load', () => {
    searchEntreprises('');
});

// Focus sur l'input au chargement
searchInput.focus();
