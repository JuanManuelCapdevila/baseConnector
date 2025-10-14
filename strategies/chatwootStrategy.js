// strategies/chatwootStrategy.js
const BaseStrategy = require('../core/baseStrategy');
const logger = require('../core/logger');

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
   * @returns {Promise<Array>}
   */
  async extract() {
    const accountId = this.config.accountId;

    if (!accountId) {
      throw new Error('accountId es requerido en la configuraci�n de Chatwoot');
    }

    // Obtener conversaciones con paginaci�n
    const allConversations = [];
    let page = 1;
    let hasMorePages = true;

    while (hasMorePages) {
      const conversations = await this.fetchConversationsPage(accountId, page);

      if (!conversations || conversations.length === 0) {
        hasMorePages = false;
        break;
      }

      // Filtrar solo conversaciones nuevas desde �ltima ejecuci�n
      const newConversations = this.filterNewConversations(conversations);
      allConversations.push(...newConversations);

      // Si encontramos menos conversaciones que el l�mite de p�gina,
      // asumimos que no hay m�s p�ginas
      if (conversations.length < 25) {
        hasMorePages = false;
      } else {
        page++;
      }
    }

    logger.info(`[${this.name}] Extra�das ${allConversations.length} conversaciones nuevas`);

    // Actualizar timestamp de �ltima ejecuci�n
    if (allConversations.length > 0) {
      this.lastExecutionTime = new Date().toISOString();
    }

    return allConversations;
  }

  /**
   * Obtiene una p�gina de conversaciones del API de Chatwoot
   * @param {number} accountId - ID de la cuenta
   * @param {number} page - N�mero de p�gina
   * @returns {Promise<Array>}
   */
  async fetchConversationsPage(accountId, page) {
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

    try {
      const response = await this.makeRequest(`${endpoint}?${params.toString()}`, {
        method: 'GET'
      });

      // Chatwoot devuelve { data: { payload: [...conversations] } }
      return response.data?.payload || [];
    } catch (error) {
      logger.error(`[${this.name}] Error obteniendo p�gina ${page}: ${error.message}`);
      throw error;
    }
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
   * @returns {Promise<Array>}
   */
  async transform(rawConversations) {
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
