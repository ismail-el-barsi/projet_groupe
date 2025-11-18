// Client WebSocket pour les mises à jour en temps réel
class RealtimeClient {
    constructor() {
        this.socket = null;
        this.connected = false;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 5;
        this.reconnectDelay = 2000;
        this.callbacks = {
            queueStats: [],
            dagAssignments: [],
            connected: [],
            disconnected: []
        };
    }

    connect() {
        try {
            // Connexion au serveur WebSocket
            this.socket = io({
                transports: ['websocket', 'polling'],
                reconnection: true,
                reconnectionDelay: this.reconnectDelay,
                reconnectionAttempts: this.maxReconnectAttempts
            });

            // Event: Connexion établie
            this.socket.on('connect', () => {
                console.log('🔌 WebSocket connecté');
                this.connected = true;
                this.reconnectAttempts = 0;
                this.callbacks.connected.forEach(cb => cb());
            });

            // Event: Déconnexion
            this.socket.on('disconnect', () => {
                console.log('🔌 WebSocket déconnecté');
                this.connected = false;
                this.callbacks.disconnected.forEach(cb => cb());
            });

            // Event: Mise à jour des stats de la queue
            this.socket.on('queue_stats_update', (stats) => {
                console.log('📊 Stats queue reçues:', stats);
                this.callbacks.queueStats.forEach(cb => cb(stats));
            });

            // Event: Mise à jour des assignments DAG
            this.socket.on('dag_assignments_update', (assignments) => {
                console.log('🎯 Assignments DAG reçus:', assignments);
                this.callbacks.dagAssignments.forEach(cb => cb(assignments));
            });

            // Event: Erreur de connexion
            this.socket.on('connect_error', (error) => {
                console.error('❌ Erreur WebSocket:', error);
                this.reconnectAttempts++;
                
                if (this.reconnectAttempts >= this.maxReconnectAttempts) {
                    console.warn('⚠️ Nombre max de tentatives atteint, passage en mode polling');
                    this.fallbackToPolling();
                }
            });

        } catch (error) {
            console.error('❌ Erreur initialisation WebSocket:', error);
            this.fallbackToPolling();
        }
    }

    disconnect() {
        if (this.socket) {
            this.socket.disconnect();
            this.socket = null;
            this.connected = false;
        }
    }

    // S'abonner aux mises à jour de la queue
    subscribeToQueue() {
        if (this.socket && this.connected) {
            this.socket.emit('subscribe_queue');
        }
    }

    // S'abonner aux assignments DAG
    subscribeToDagAssignments() {
        if (this.socket && this.connected) {
            this.socket.emit('subscribe_dag_assignments');
        }
    }

    // Enregistrer un callback pour les stats de queue
    onQueueStatsUpdate(callback) {
        this.callbacks.queueStats.push(callback);
    }

    // Enregistrer un callback pour les assignments DAG
    onDagAssignmentsUpdate(callback) {
        this.callbacks.dagAssignments.push(callback);
    }

    // Enregistrer un callback pour la connexion
    onConnected(callback) {
        this.callbacks.connected.push(callback);
    }

    // Enregistrer un callback pour la déconnexion
    onDisconnected(callback) {
        this.callbacks.disconnected.push(callback);
    }

    // Fallback en mode polling si WebSocket échoue
    fallbackToPolling() {
        console.log('🔄 Passage en mode polling (requêtes HTTP)');
        
        // Polling toutes les 2 secondes
        setInterval(async () => {
            try {
                // Récupérer les stats de la queue
                const queueResp = await fetch('/api/queue/stats');
                const queueData = await queueResp.json();
                if (queueData.success) {
                    this.callbacks.queueStats.forEach(cb => cb(queueData.stats));
                }

                // Récupérer les assignments DAG
                const dagResp = await fetch('/api/queue/dag-assignments');
                const dagData = await dagResp.json();
                if (dagData.success) {
                    this.callbacks.dagAssignments.forEach(cb => cb(dagData.assignments));
                }
            } catch (error) {
                console.error('❌ Erreur polling:', error);
            }
        }, 2000);
    }

    // Vérifier si le client est connecté
    isConnected() {
        return this.connected;
    }
}

// Instance globale
const realtimeClient = new RealtimeClient();
