// Script pour le dashboard administrateur
const refreshBtn = document.getElementById('refreshBtn');
const lastUpdate = document.getElementById('lastUpdate');

// Éléments des stats
const totalScraped = document.getElementById('totalScraped');
const totalInDB = document.getElementById('totalInDB');
const totalQueue = document.getElementById('totalQueue');
const totalRetrying = document.getElementById('totalRetrying');

// Queue stats
const queuePending = document.getElementById('queuePending');
const queueProcessing = document.getElementById('queueProcessing');
const queueHighPriority = document.getElementById('queueHighPriority');
const queueTableBody = document.getElementById('queueTableBody');
const queuePagination = document.getElementById('queuePagination');

// Performance
const successRate = document.getElementById('successRate');
const avgRequestsPerMin = document.getElementById('avgRequestsPerMin');
const totalRequestsHour = document.getElementById('totalRequestsHour');
const successRequestsHour = document.getElementById('successRequestsHour');

// Tables
const proxiesTableBody = document.getElementById('proxiesTableBody');
const proxiesPagination = document.getElementById('proxiesPagination');
const failuresTableBody = document.getElementById('failuresTableBody');
const failuresByType = document.getElementById('failuresByType');
const validationReport = document.getElementById('validationReport');

// Charger toutes les données
async function loadDashboardData() {
    try {
        // Stats générales
        await loadGeneralStats();
        
        // Stats de la queue
        await loadQueueStats();
        
        // Stats de validation
        await loadValidationReport();
        
        // Assignments DAG
        await loadDagAssignments();
        
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
        // Charger les stats Redis directement pour le temps réel
        const queueResponse = await fetch('/api/queue/stats');
        const queueData = await queueResponse.json();
        
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
        }
        
        // Stats de la queue Redis EN TEMPS RÉEL (priorité absolue)
        if (queueData.success && queueData.stats) {
            const qStats = queueData.stats;
            totalQueue.textContent = formatNumber(qStats.total_queue || 0);
            totalRetrying.textContent = formatNumber(qStats.total_retrying || 0);
            
            // Mettre à jour aussi les mini-stats de la queue
            if (queuePending) queuePending.textContent = formatNumber(qStats.total_pending || 0);
            if (queueProcessing) queueProcessing.textContent = formatNumber(qStats.total_processing || 0);
            if (queueHighPriority) queueHighPriority.textContent = formatNumber(qStats.high_priority || 0);
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
        
        // Proxies (paginated table)
        await loadProxiesPage(1, 10);
        
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

// Display paginated proxies result (items array)
function displayProxiesPaginated(items) {
    if (!items || items.length === 0) {
        proxiesTableBody.innerHTML = '<tr><td colspan="6" class="text-center">Aucun proxy enregistré</td></tr>';
        return;
    }

    proxiesTableBody.innerHTML = items.map(stats => {
        const statusClass = stats.status || 'actif';
        return `
            <tr>
                <td>${stats.proxy_ip}</td>
                <td>${formatNumber(stats.total_requests || 0)}</td>
                <td>${formatNumber(stats.successful_requests || 0)}</td>
                <td>${formatNumber(stats.failed_requests || 0)}</td>
                <td><span class="proxy-status ${statusClass}">${statusClass}</span></td>
                <td>${stats.last_used ? formatDate(stats.last_used) : 'N/A'}</td>
            </tr>
        `;
    }).join('');
}

// Load a proxies page from the API and render with pagination controls
async function loadProxiesPage(page = 1, per_page = 10) {
    try {
        const resp = await fetch(`/api/dashboard/proxies?page=${page}&per_page=${per_page}`);
        const data = await resp.json();
        if (!data.success) {
            proxiesTableBody.innerHTML = '<tr><td colspan="6" class="text-center">Erreur chargement proxies</td></tr>';
            return;
        }

        displayProxiesPaginated(data.items || []);
        renderProxiesPagination(data.total || 0, data.page || page, data.per_page || per_page);
    } catch (err) {
        console.error('Erreur loadProxiesPage:', err);
        proxiesTableBody.innerHTML = '<tr><td colspan="6" class="text-center">Erreur chargement proxies</td></tr>';
    }
}

function renderProxiesPagination(total, page, per_page) {
    if (!proxiesPagination) return;
    const totalPages = Math.max(1, Math.ceil((total || 0) / (per_page || 1)));
    let html = '';
    if (page > 1) html += `<button id="proxiesPrev" class="btn">← Précédent</button>`;
    html += ` <span>Page ${page} / ${totalPages}</span> `;
    if (page < totalPages) html += `<button id="proxiesNext" class="btn">Suivant →</button>`;
    proxiesPagination.innerHTML = html;

    const prev = document.getElementById('proxiesPrev');
    const next = document.getElementById('proxiesNext');
    if (prev) prev.addEventListener('click', () => loadProxiesPage(page - 1, per_page));
    if (next) next.addEventListener('click', () => loadProxiesPage(page + 1, per_page));
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

// Initialiser le client WebSocket
realtimeClient.connect();

// S'abonner aux mises à jour en temps réel
realtimeClient.onConnected(() => {
    console.log('✅ WebSocket connecté - Abonnement aux mises à jour temps réel');
    realtimeClient.subscribeToQueue();
    realtimeClient.subscribeToDagAssignments();
});

// Gérer les mises à jour de la queue en temps réel
realtimeClient.onQueueStatsUpdate((stats) => {
    // Mettre à jour les stats en temps réel
    if (totalQueue) totalQueue.textContent = formatNumber(stats.total_queue || 0);
    if (queuePending) queuePending.textContent = formatNumber(stats.total_pending || 0);
    if (queueProcessing) queueProcessing.textContent = formatNumber(stats.total_processing || 0);
    if (queueHighPriority) queueHighPriority.textContent = formatNumber(stats.high_priority || 0);
    
    // Recharger la table de la queue
    loadQueuePage(currentQueuePage, 20, currentQueueStatus, currentQueuePriority);
    
    // Mettre à jour le timestamp
    const now = new Date();
    lastUpdate.textContent = `Dernière mise à jour: ${now.toLocaleString('fr-FR')} (temps réel)`;
});

// Gérer les mises à jour des assignments DAG en temps réel
realtimeClient.onDagAssignmentsUpdate((assignments) => {
    displayDagAssignmentsRealtime(assignments);
});

// Fonction pour afficher les assignments en temps réel (sans recharger depuis l'API)
function displayDagAssignmentsRealtime(assignments) {
    const dagContainer = document.getElementById('dagAssignments');
    if (!dagContainer) return;
    
    const totalAssignments = Object.values(assignments).reduce((sum, enterprises) => sum + enterprises.length, 0);
    
    if (totalAssignments === 0) {
        dagContainer.innerHTML = '<p class="dag-assignment-empty">Aucun DAG n\'est actuellement en cours de scraping</p>';
        return;
    }
    
    const sortedDags = Object.entries(assignments)
        .sort(([, a], [, b]) => b.length - a.length)
        .filter(([, enterprises]) => enterprises.length > 0);
    
    dagContainer.innerHTML = sortedDags.map(([dagId, enterprises]) => `
        <div class="dag-assignment-card">
            <h4>🤖 ${dagId}</h4>
            <p style="color: var(--text-secondary); font-size: 0.85rem; margin-bottom: 0.5rem;">
                ${enterprises.length} entreprise${enterprises.length > 1 ? 's' : ''}
            </p>
            <ul class="dag-assignment-list">
                ${enterprises.map(num => `<li>${num}</li>`).join('')}
            </ul>
        </div>
    `).join('');
}

// Fallback : Si WebSocket ne fonctionne pas, utiliser le polling
realtimeClient.onDisconnected(() => {
    console.warn('⚠️ WebSocket déconnecté - Les mises à jour continueront en mode polling');
});

// Auto-refresh complet toutes les 30 secondes (pour les autres stats non-WebSocket)
setInterval(() => {
    loadGeneralStats();
    loadValidationReport();
}, 30000);


// === Gestion de la file d'attente ===

// Variables pour les filtres de queue
let currentQueueStatus = 'pending';
let currentQueuePriority = 'all';
let currentQueuePage = 1;

async function loadQueueStats() {
    try {
        const response = await fetch('/api/queue/stats');
        const data = await response.json();
        
        if (!data.success) {
            console.error('Erreur:', data.error);
            return;
        }
        
        const stats = data.stats;
        
        // Ces stats sont déjà mises à jour dans loadGeneralStats(), 
        // mais on les remet à jour ici pour être sûr
        if (queuePending) queuePending.textContent = formatNumber(stats.total_pending || 0);
        if (queueProcessing) queueProcessing.textContent = formatNumber(stats.total_processing || 0);
        if (queueHighPriority) queueHighPriority.textContent = formatNumber(stats.high_priority || 0);
        
        // Charger les items de la queue avec filtres actuels
        await loadQueuePage(currentQueuePage, 20, currentQueueStatus, currentQueuePriority);
        
        // Ajouter les event listeners pour les filtres (une seule fois)
        if (!window.queueFiltersInitialized) {
            const statusFilter = document.getElementById('queueStatusFilter');
            const priorityFilter = document.getElementById('queuePriorityFilter');
            
            if (statusFilter) {
                statusFilter.addEventListener('change', (e) => {
                    currentQueueStatus = e.target.value;
                    currentQueuePage = 1;
                    loadQueuePage(1, 20, currentQueueStatus, currentQueuePriority);
                });
            }
            
            if (priorityFilter) {
                priorityFilter.addEventListener('change', (e) => {
                    currentQueuePriority = e.target.value;
                    currentQueuePage = 1;
                    loadQueuePage(1, 20, currentQueueStatus, currentQueuePriority);
                });
            }
            
            window.queueFiltersInitialized = true;
        }
        
    } catch (error) {
        console.error('Erreur loadQueueStats:', error);
    }
}

async function loadQueuePage(page = 1, per_page = 20, status = 'pending', priority = 'all') {
    try {
        // Si status = 'all', on doit récupérer tous les statuts
        if (status === 'all') {
            // Récupérer les 3 statuts en parallèle
            const [pendingResp, processingResp, failedResp] = await Promise.all([
                fetch(`/api/queue/items?status=pending&page=1&per_page=1000`),
                fetch(`/api/queue/items?status=processing&page=1&per_page=1000`),
                fetch(`/api/queue/items?status=failed&page=1&per_page=1000`)
            ]);
            
            const [pendingData, processingData, failedData] = await Promise.all([
                pendingResp.json(),
                processingResp.json(),
                failedResp.json()
            ]);
            
            // Combiner tous les items
            let allItems = [
                ...(pendingData.items || []),
                ...(processingData.items || []),
                ...(failedData.items || [])
            ];
            
            // Filtrer par priorité côté client
            if (priority === 'high') {
                allItems = allItems.filter(item => item.priority >= 2);
            } else if (priority === 'normal') {
                allItems = allItems.filter(item => item.priority < 2);
            }
            
            // Pagination manuelle côté client
            const offset = (page - 1) * per_page;
            const paginatedItems = allItems.slice(offset, offset + per_page);
            
            displayQueueItems(paginatedItems);
            renderQueuePagination(allItems.length, page, per_page, status, priority);
            return;
        }
        
        // Construire l'URL avec les paramètres de filtre
        let url = `/api/queue/items?status=${status}&page=${page}&per_page=${per_page}`;
        
        const resp = await fetch(url);
        const data = await resp.json();
        
        if (!data.success) {
            queueTableBody.innerHTML = '<tr><td colspan="7" class="text-center">Erreur chargement file d\'attente</td></tr>';
            return;
        }
        
        // Filtrer par priorité côté client
        let items = data.items || [];
        if (priority === 'high') {
            items = items.filter(item => item.priority >= 2);
        } else if (priority === 'normal') {
            items = items.filter(item => item.priority < 2);
        }
        
        displayQueueItems(items);
        renderQueuePagination(items.length, data.page || page, data.per_page || per_page, status, priority);
        
    } catch (err) {
        console.error('Erreur loadQueuePage:', err);
        queueTableBody.innerHTML = '<tr><td colspan="7" class="text-center">Erreur chargement file d\'attente</td></tr>';
    }
}

function displayQueueItems(items) {
    if (!items || items.length === 0) {
        queueTableBody.innerHTML = '<tr><td colspan="7" class="text-center">Aucune entreprise en attente</td></tr>';
        return;
    }
    
    queueTableBody.innerHTML = items.map(item => {
        const priorityClass = item.priority >= 2 ? 'high-priority' : 'normal-priority';
        const statusClass = item.status === 'processing' ? 'processing' : item.status === 'failed' ? 'failed' : 'pending';
        
        // Afficher le DAG assigné si en processing
        let dagInfo = '-';
        if (item.status === 'processing' && item.assigned_dag) {
            dagInfo = `<span class="dag-badge">${item.assigned_dag}</span>`;
        }
        
        // Mettre en évidence les priorités hautes
        const rowClass = item.priority >= 2 ? 'high-priority-row' : '';
        
        return `
            <tr class="${rowClass}">
                <td><a href="/entreprise/${item.enterprise_number}" target="_blank">${item.enterprise_number}</a></td>
                <td><span class="priority-badge ${priorityClass}">${item.priority_label}</span></td>
                <td><span class="status-badge ${statusClass}">${item.status}</span></td>
                <td>${formatDate(item.added_at)}</td>
                <td>${item.requested_by || '-'}</td>
                <td>${item.attempts || 0}</td>
                <td>
                    ${item.status === 'processing' ? dagInfo : '<button class="btn-small" onclick="removeFromQueue(\'' + item.enterprise_number + '\')">🗑️ Retirer</button>'}
                </td>
            </tr>
        `;
    }).join('');
}

function renderQueuePagination(total, page, per_page, status, priority = 'all') {
    if (!queuePagination) return;
    const totalPages = Math.max(1, Math.ceil((total || 0) / (per_page || 1)));
    let html = '';
    if (page > 1) html += `<button id="queuePrev" class="btn">← Précédent</button>`;
    html += ` <span>Page ${page} / ${totalPages} (${total} élément${total > 1 ? 's' : ''})</span> `;
    if (page < totalPages) html += `<button id="queueNext" class="btn">Suivant →</button>`;
    queuePagination.innerHTML = html;
    
    const prev = document.getElementById('queuePrev');
    const next = document.getElementById('queueNext');
    if (prev) prev.addEventListener('click', () => loadQueuePage(page - 1, per_page, status, priority));
    if (next) next.addEventListener('click', () => loadQueuePage(page + 1, per_page, status, priority));
}

async function removeFromQueue(enterpriseNumber) {
    if (!confirm(`Retirer ${enterpriseNumber} de la file d'attente ?`)) {
        return;
    }
    
    try {
        const resp = await fetch(`/api/queue/remove/${enterpriseNumber}`, {
            method: 'DELETE'
        });
        const data = await resp.json();
        
        if (data.success) {
            alert('Entreprise retirée de la file d\'attente');
            // Recharger avec les filtres actuels
            await loadQueuePage(currentQueuePage, 20, currentQueueStatus, currentQueuePriority);
            await loadDagAssignments(); // Rafraîchir aussi les assignments
        } else {
            alert('Erreur: ' + (data.error || 'Échec suppression'));
        }
    } catch (err) {
        console.error('Erreur removeFromQueue:', err);
        alert('Erreur lors de la suppression');
    }
}

// === Assignments DAG ===
async function loadDagAssignments() {
    try {
        const response = await fetch('/api/queue/dag-assignments');
        const data = await response.json();
        
        if (!data.success) {
            console.error('Erreur:', data.error);
            document.getElementById('dagAssignments').innerHTML = '<p class="dag-assignment-empty">Erreur de chargement</p>';
            return;
        }
        
        const assignments = data.assignments || {};
        const dagContainer = document.getElementById('dagAssignments');
        
        // Si aucun assignment
        const totalAssignments = Object.values(assignments).reduce((sum, enterprises) => sum + enterprises.length, 0);
        
        if (totalAssignments === 0) {
            dagContainer.innerHTML = '<p class="dag-assignment-empty">Aucun DAG n\'est actuellement en cours de scraping</p>';
            return;
        }
        
        // Trier les DAGs par nombre d'entreprises (décroissant)
        const sortedDags = Object.entries(assignments)
            .sort(([, a], [, b]) => b.length - a.length)
            .filter(([, enterprises]) => enterprises.length > 0);
        
        dagContainer.innerHTML = sortedDags.map(([dagId, enterprises]) => `
            <div class="dag-assignment-card">
                <h4>🤖 ${dagId}</h4>
                <p style="color: var(--text-secondary); font-size: 0.85rem; margin-bottom: 0.5rem;">
                    ${enterprises.length} entreprise${enterprises.length > 1 ? 's' : ''}
                </p>
                <ul class="dag-assignment-list">
                    ${enterprises.map(num => `<li>${num}</li>`).join('')}
                </ul>
            </div>
        `).join('');
        
    } catch (error) {
        console.error('Erreur loadDagAssignments:', error);
        document.getElementById('dagAssignments').innerHTML = '<p class="dag-assignment-empty">Erreur de chargement</p>';
    }
}
