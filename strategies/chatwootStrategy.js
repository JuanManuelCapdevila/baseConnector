// strategies/chatwootStrategy.js
const BaseStrategy = require('../core/baseStrategy');
const logger = require('../core/logger');
const { of, from, throwError, EMPTY } = require('rxjs');
const { expand, map, reduce, switchMap, tap, catchError } = require('rxjs/operators');

/**
 * Estrategia para extraer conversaciones de Chatwoot
 * Captura conversaciones nuevas mediante polling peri�dico del API
 */
class ChatwootStrategy extends BaseStrategy {
  constructor(config) {
    super(config);
    this.lastExecutionTime = null;
  }

  /**
   * Extrae conversaciones de Chatwoot
   * @returns {Observable<Array>}
   */
  extract() {
    const accountId = this.config.accountId;

    if (!accountId) {
      return throwError(() => new Error('accountId es requerido en la configuración de Chatwoot'));
    }

    // Usamos `expand` para manejar la paginación de forma recursiva
    return this.fetchConversationsPage(accountId, 1).pipe(
      expand(({ conversations, page }) => {
        // Si la última página tuvo menos de 25 resultados, o estaba vacía, paramos.
        if (!conversations || conversations.length < 25) {
          return EMPTY;
        }
        // Si no, pedimos la siguiente página.
        return this.fetchConversationsPage(accountId, page + 1);
      }),
      // Transformamos el flujo de páginas en un flujo de conversaciones individuales
      switchMap(({ conversations }) => from(conversations)),
      // Acumulamos todas las conversaciones en un solo array
      reduce((acc, conversation) => [...acc, conversation], []),
      // Filtramos las conversaciones que ya hemos procesado
      map(allConversations => this.filterNewConversations(allConversations)),
      tap(newConversations => {
        logger.info(`[${this.name}] Extraídas ${newConversations.length} conversaciones nuevas`);
        if (newConversations.length > 0) {
          this.lastExecutionTime = new Date().toISOString();
        }
      })
    );
  }

  /**
   * Obtiene una p�gina de conversaciones del API de Chatwoot
   * @param {number} accountId - ID de la cuenta
   * @param {number} page - N�mero de p�gina
   * @returns {Observable<{conversations: Array, page: number}>}
   */
  fetchConversationsPage(accountId, page) {
    const endpoint = `/api/v1/accounts/${accountId}/conversations`;

    const params = new URLSearchParams({
      status: this.config.status || 'open',
      assignee_type: this.config.assigneeType || 'all',
      page: page.toString()
    });

    // Agregar filtros opcionales si est�n configurados
    if (this.config.inboxId) {
      params.append('inbox_id', this.config.inboxId);
    }

    if (this.config.teamId) {
      params.append('team_id', this.config.teamId);
    }

    if (this.config.labels && Array.isArray(this.config.labels)) {
      this.config.labels.forEach(label => params.append('labels[]', label));
    }

    // makeRequest debería devolver un Observable (asumiendo que usa httpClient)
    return this.makeRequest(`${endpoint}?${params.toString()}`, {
        method: 'GET'
      }).pipe(
        // Chatwoot devuelve { data: { payload: [...conversations] } }
        // Asumimos que makeRequest devuelve el cuerpo de la respuesta.
        map(response => ({
            conversations: response.data?.payload || [],
            page
        })),
        catchError(error => {
            logger.error(`[${this.name}] Error obteniendo página ${page}: ${error.message}`);
            return throwError(() => error);
        })
    );
  }

  /**
   * Filtra conversaciones nuevas desde la �ltima ejecuci�n
   * @param {Array} conversations - Conversaciones a filtrar
   * @returns {Array}
   */
  filterNewConversations(conversations) {
    if (!this.lastExecutionTime) {
      // Primera ejecuci�n: retornar todas las conversaciones
      return conversations;
    }

    const lastExecDate = new Date(this.lastExecutionTime);

    return conversations.filter(conversation => {
      const createdAt = new Date(conversation.created_at * 1000); // Chatwoot usa unix timestamp
      return createdAt > lastExecDate;
    });
  }

  /**
   * Transforma las conversaciones al formato est�ndar
   * @param {Array} rawConversations - Conversaciones crudas de Chatwoot
   * @returns {Array}
   */
  transform(rawConversations) {
    if (!Array.isArray(rawConversations)) {
      logger.warn(`[${this.name}] transform() esperaba un array`);
      return [];
    }

    return rawConversations.map(conversation => {
      // Aplicar normalizaci�n si hay mapping configurado
      const baseData = this.config.mapping
        ? this.normalize(conversation)
        : this.defaultMapping(conversation);

      // Agregar metadata adicional
      return {
        ...baseData,
        _metadata: {
          extracted_at: new Date().toISOString(),
          source: this.name,
          original_id: conversation.id
        }
      };
    });
  }

  /**
   * Mapeo por defecto si no hay configuraci�n de mapping
   * @param {Object} conversation - Conversaci�n de Chatwoot
   * @returns {Object}
   */
  defaultMapping(conversation) {
    return {
      conversationId: conversation.id,
      accountId: conversation.account_id,
      inboxId: conversation.inbox_id,
      status: conversation.status,
      createdAt: new Date(conversation.created_at * 1000).toISOString(),
      updatedAt: conversation.timestamp
        ? new Date(conversation.timestamp * 1000).toISOString()
        : null,
      contact: {
        id: conversation.meta?.sender?.id,
        name: conversation.meta?.sender?.name,
        email: conversation.meta?.sender?.email,
        phone: conversation.meta?.sender?.phone_number,
        avatar: conversation.meta?.sender?.thumbnail
      },
      assignee: conversation.meta?.assignee ? {
        id: conversation.meta.assignee.id,
        name: conversation.meta.assignee.name,
        email: conversation.meta.assignee.email
      } : null,
      team: conversation.meta?.team ? {
        id: conversation.meta.team.id,
        name: conversation.meta.team.name
      } : null,
      messages: conversation.messages?.map(msg => ({
        id: msg.id,
        content: msg.content,
        messageType: msg.message_type,
        contentType: msg.content_type,
        createdAt: new Date(msg.created_at * 1000).toISOString(),
        private: msg.private,
        sender: {
          id: msg.sender?.id,
          name: msg.sender?.name,
          type: msg.sender?.type
        }
      })) || [],
      labels: conversation.labels || [],
      unreadCount: conversation.unread_count || 0,
      additionalAttributes: conversation.additional_attributes || {}
    };
  }

  /**
   * Sobrescribe getAuthHeaders para usar el formato espec�fico de Chatwoot
   * @returns {Object}
   */
  getAuthHeaders() {
    const { auth } = this.config;

    if (!auth || !auth.apiKey) {
      logger.warn(`[${this.name}] No se encontr� apiKey en la configuraci�n`);
      return {};
    }

    return {
      'api_access_token': auth.apiKey
    };
  }
}

module.exports = ChatwootStrategy;
