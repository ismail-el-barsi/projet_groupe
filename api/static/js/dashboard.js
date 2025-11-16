// Script pour le dashboard administrateur
const refreshBtn = document.getElementById('refreshBtn');
const lastUpdate = document.getElementById('lastUpdate');

// Éléments des stats
const totalScraped = document.getElementById('totalScraped');
const totalInDB = document.getElementById('totalInDB');
const totalQueue = document.getElementById('totalQueue');
const totalFailed = document.getElementById('totalFailed');

// Performance
const successRate = document.getElementById('successRate');
const avgRequestsPerMin = document.getElementById('avgRequestsPerMin');
const totalRequestsHour = document.getElementById('totalRequestsHour');
const successRequestsHour = document.getElementById('successRequestsHour');

// Tables
const proxiesTableBody = document.getElementById('proxiesTableBody');
const dagsTableBody = document.getElementById('dagsTableBody');
const failuresTableBody = document.getElementById('failuresTableBody');
const failuresByType = document.getElementById('failuresByType');
const validationReport = document.getElementById('validationReport');

// Charger toutes les données
async function loadDashboardData() {
    try {
        // Stats générales
        await loadGeneralStats();
        
        // Stats de validation
        await loadValidationReport();
        
        // Mettre à jour le timestamp
        const now = new Date();
        lastUpdate.textContent = `Dernière mise à jour: ${now.toLocaleString('fr-FR')}`;
        
    } catch (error) {
        console.error('Erreur chargement dashboard:', error);
    }
}

// Charger les statistiques générales
async function loadGeneralStats() {
    try {
        const response = await fetch('/api/dashboard/stats');
        const data = await response.json();
        
        if (!data.success) {
            console.error('Erreur:', data.error);
            return;
        }
        
        const stats = data.data;
        
        // Stats générales
        if (stats.general) {
            totalScraped.textContent = formatNumber(stats.general.total_scraped || 0);
            totalQueue.textContent = formatNumber(stats.general.total_queue || 0);
            totalFailed.textContent = formatNumber(stats.general.total_failed || 0);
        }
        
        // Compter les entreprises en BDD
        const countResponse = await fetch('/api/stats/count');
        const countData = await countResponse.json();
        if (countData.success) {
            totalInDB.textContent = formatNumber(countData.total_in_db || 0);
        }
        
        // Performance
        if (stats.performance) {
            const perf = stats.performance;
            const successRateVal = (perf.success_rate_per_minute !== undefined) ? perf.success_rate_per_minute : 0;
            const avgReqVal = (perf.avg_requests_per_minute !== undefined) ? perf.avg_requests_per_minute : 0;
            const totalWindow = perf.total_requests_window !== undefined ? perf.total_requests_window : (perf.total_requests_last_hour !== undefined ? perf.total_requests_last_hour : 0);
            const successWindow = perf.success_requests_window !== undefined ? perf.success_requests_window : (perf.success_requests_last_hour !== undefined ? perf.success_requests_last_hour : 0);

            successRate.textContent = Number(successRateVal).toFixed(2) + '%';
            avgRequestsPerMin.textContent = Number(avgReqVal).toFixed(2);
            totalRequestsHour.textContent = formatNumber(totalWindow || 0);
            successRequestsHour.textContent = formatNumber(successWindow || 0);
        }
        
        // Proxies
        if (stats.ips) {
            displayProxies(stats.ips);
        }
        
        // DAGs
        if (stats.dags_status) {
            displayDags(stats.dags_status);
        }
        
        // Échecs
        if (stats.failures) {
            displayFailures(stats.failures);
        }
        
    } catch (error) {
        console.error('Erreur loadGeneralStats:', error);
    }
}

// Afficher les proxies
function displayProxies(ips) {
    const ipsList = Object.entries(ips);
    
    if (ipsList.length === 0) {
        proxiesTableBody.innerHTML = '<tr><td colspan="6" class="text-center">Aucun proxy enregistré</td></tr>';
        return;
    }
    
    proxiesTableBody.innerHTML = ipsList.map(([ip, stats]) => {
        const statusClass = stats.status || 'actif';
        return `
            <tr>
                <td>${ip}</td>
                <td>${formatNumber(stats.total_requests || 0)}</td>
                <td>${formatNumber(stats.successful_requests || 0)}</td>
                <td>${formatNumber(stats.failed_requests || 0)}</td>
                <td><span class="proxy-status ${statusClass}">${statusClass}</span></td>
                <td>${stats.last_used ? formatDate(stats.last_used) : 'N/A'}</td>
            </tr>
        `;
    }).join('');
}

// Afficher les DAGs
function displayDags(dags) {
    const dagsList = Object.entries(dags);
    
    if (dagsList.length === 0) {
        dagsTableBody.innerHTML = '<tr><td colspan="5" class="text-center">Aucun DAG enregistré</td></tr>';
        return;
    }
    
    dagsTableBody.innerHTML = dagsList.map(([dagId, stats]) => `
        <tr>
            <td>${dagId}</td>
            <td>${formatNumber(stats.total_scraped || 0)}</td>
            <td>${formatNumber(stats.current_position || 0)}</td>
            <td><span class="proxy-status ${stats.status}">${stats.status || 'unknown'}</span></td>
            <td>${stats.last_scrape ? formatDate(stats.last_scrape) : 'N/A'}</td>
        </tr>
    `).join('');
}

// Afficher les échecs
function displayFailures(failures) {
    // Échecs par type
    if (failures.by_type) {
        const types = Object.entries(failures.by_type);
        
        if (types.length > 0) {
            failuresByType.innerHTML = types.map(([type, count]) => `
                <div class="failure-type">
                    <div class="failure-type-name">${type}</div>
                    <div class="failure-type-count">${formatNumber(count)}</div>
                </div>
            `).join('');
        }
    }
    
    // Échecs récents
    if (failures.recent && failures.recent.length > 0) {
        failuresTableBody.innerHTML = failures.recent.slice(0, 20).map(failure => `
            <tr>
                <td>${formatDate(failure.timestamp)}</td>
                <td>${failure.enterprise_id || 'N/A'}</td>
                <td>${failure.dag_id || 'N/A'}</td>
                <td>${failure.error_type || 'N/A'}</td>
                <td>${truncate(failure.error_msg || 'N/A', 50)}</td>
            </tr>
        `).join('');
    } else {
        failuresTableBody.innerHTML = '<tr><td colspan="5" class="text-center">Aucun échec récent</td></tr>';
    }
}

// Charger le rapport de validation
async function loadValidationReport() {
    try {
        const response = await fetch('/api/dashboard/validation');
        const data = await response.json();
        
        if (!data.success) {
            validationReport.innerHTML = '<p>Aucun rapport de validation disponible</p>';
            return;
        }
        
        const report = data.report;
        const stats = report.statistiques;
        
        validationReport.innerHTML = `
            <div class="validation-stats">
                <div class="validation-stat">
                    <div class="validation-stat-value">${formatNumber(stats.total_entreprises)}</div>
                    <div class="validation-stat-label">Total entreprises</div>
                </div>
                <div class="validation-stat">
                    <div class="validation-stat-value">${formatNumber(stats.entreprises_valides)}</div>
                    <div class="validation-stat-label">Valides</div>
                </div>
                <div class="validation-stat">
                    <div class="validation-stat-value">${stats.pourcentage_valides}%</div>
                    <div class="validation-stat-label">Taux de validité</div>
                </div>
                <div class="validation-stat">
                    <div class="validation-stat-value">${formatNumber(stats.entreprises_invalides)}</div>
                    <div class="validation-stat-label">Invalides</div>
                </div>
            </div>
            <p style="margin-top: 1rem; color: var(--text-secondary);">
                Source: ${report.source}<br>
                Date: ${formatDate(report.date)}
            </p>
        `;
        
    } catch (error) {
        console.error('Erreur loadValidationReport:', error);
        validationReport.innerHTML = '<p>Erreur de chargement du rapport</p>';
    }
}

// Utilitaires
function formatNumber(num) {
    return num.toLocaleString('fr-FR');
}

function formatDate(dateString) {
    if (!dateString) return 'N/A';
    const date = new Date(dateString);
    return date.toLocaleString('fr-FR');
}

function truncate(str, maxLength) {
    if (str.length <= maxLength) return str;
    return str.substring(0, maxLength) + '...';
}

// Event listeners
refreshBtn.addEventListener('click', () => {
    refreshBtn.disabled = true;
    refreshBtn.textContent = '🔄 Actualisation...';
    
    loadDashboardData().finally(() => {
        refreshBtn.disabled = false;
        refreshBtn.textContent = '🔄 Actualiser';
    });
});

// Charger les données au démarrage
loadDashboardData();

// Auto-refresh toutes les 30 secondes
setInterval(loadDashboardData, 30000);
