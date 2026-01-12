import axios from 'axios';
import type { ChatRequest, ChatResponse, Conversation, Settings, User } from '../types';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

const api = axios.create({
  baseURL: API_BASE_URL,
  withCredentials: true,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor to add CSRF token and Authorization header
api.interceptors.request.use((config) => {
  const csrfToken = getCookie('csrftoken');
  if (csrfToken) {
    config.headers['X-CSRFToken'] = csrfToken;
  }

  // Add JWT token from localStorage
  const authStorage = localStorage.getItem('auth-storage');
  if (authStorage) {
    try {
      const { state } = JSON.parse(authStorage);
      if (state?.token) {
        config.headers['Authorization'] = `Bearer ${state.token}`;
      }
    } catch (e) {
      console.error('Error parsing auth token:', e);
    }
  }

  return config;
});

function getCookie(name: string): string | null {
  const value = `; ${document.cookie}`;
  const parts = value.split(`; ${name}=`);
  if (parts.length === 2) {
    return parts.pop()?.split(';').shift() || null;
  }
  return null;
}

export const authApi = {
  login: async (username: string, password: string) => {
    const response = await api.post('/api/auth/login', { username, password });
    return response.data;
  },

  register: async (username: string, email: string, password: string, organization?: string) => {
    const response = await api.post('/api/auth/register', { username, email, password, organization });
    return response.data;
  },

  logout: async () => {
    const response = await api.post('/api/auth/logout');
    return response.data;
  },

  getCurrentUser: async (): Promise<User> => {
    const response = await api.get('/api/auth/me');
    return response.data;
  },
};

export const chatApi = {
  sendMessage: async (request: ChatRequest): Promise<ChatResponse> => {
    const response = await api.post('/api/chat/send', request);
    return response.data;
  },

  getConversations: async (): Promise<Conversation[]> => {
    const response = await api.get('/api/chat/conversations');
    // Transform backend response to frontend format
    return response.data.map((conv: any) => ({
      id: conv.id,
      chat_id: conv.chat_id,
      title: conv.title,
      messages: conv.history?.map((msg: any, idx: number) => ({
        id: `${conv.chat_id}-${idx}`,
        type: msg.type.toLowerCase(),
        content: msg.data.content,
        timestamp: new Date().toISOString(),
      })) || [],
      created_at: conv.created_at,
      updated_at: conv.updated_at,
    }));
  },

  deleteConversation: async (chatId: string) => {
    const response = await api.delete(`/api/chat/conversations/${chatId}`);
    return response.data;
  },

  updateConversation: async (chatId: string, title: string) => {
    const response = await api.patch(`/api/chat/conversations/${chatId}`, { title });
    return response.data;
  },
};

export const settingsApi = {
  getSettings: async (): Promise<Settings> => {
    const response = await api.get('/api/settings');
    return response.data;
  },

  saveSettings: async (settings: Partial<Settings>) => {
    const response = await api.post('/api/settings', settings);
    return response.data;
  },

  // Model Management
  getModels: async () => {
    const response = await api.get('/api/settings/models');
    return response.data;
  },

  addModel: async (model: any) => {
    const response = await api.post('/api/settings/models', model);
    return response.data;
  },

  updateModel: async (id: number, model: any) => {
    const response = await api.put(`/api/settings/models/${id}`, model);
    return response.data;
  },

  deleteModel: async (id: number) => {
    const response = await api.delete(`/api/settings/models/${id}`);
    return response.data;
  },

  // Data Source Management
  getDataSources: async () => {
    const response = await api.get('/api/settings/data-sources');
    return response.data;
  },

  addDataSource: async (dataSource: any) => {
    const response = await api.post('/api/settings/data-sources', dataSource);
    return response.data;
  },

  updateDataSource: async (id: number, dataSource: any) => {
    const response = await api.put(`/api/settings/data-sources/${id}`, dataSource);
    return response.data;
  },

  deleteDataSource: async (id: number) => {
    const response = await api.delete(`/api/settings/data-sources/${id}`);
    return response.data;
  },

  processDataSource: async (id: number) => {
    const response = await api.post(`/api/datasources/${id}/process`);
    return response.data;
  },
};

export const etlApi = {
  // ETL Data Source Management
  getDataSources: async () => {
    const response = await api.get('/api/etl/datasources');
    return response.data;
  },

  linkedServices: {
    list: async () => {
      const response = await api.get('/api/etl/linked-services');
      return response.data;
    },
    create: async (data: any) => {
      const response = await api.post('/api/etl/linked-services', data);
      return response.data;
    },
    test: async (config: {
      service_type: string;
      connection_config: Record<string, string>;
    }) => {
      const response = await api.post('/api/etl/linked-services/test', config);
      return response.data;
    }
  },

  createDataSource: async (dataSource: any) => {
    const response = await api.post('/api/etl/datasources', dataSource);
    return response.data;
  },

  getDataSource: async (id: number) => {
    const response = await api.get(`/api/etl/datasources/${id}`);
    return response.data;
  },

  deleteDataSource: async (id: number) => {
    const response = await api.delete(`/api/etl/datasources/${id}`);
    return response.data;
  },

  getTableSchema: async (id: number) => {
    const response = await api.get(`/api/etl/datasources/${id}/schema`);
    return response.data;
  },

  testConnection: async (config: {
    db_type: string;
    connection_config: Record<string, string>;
    table_name: string;
  }) => {
    const response = await api.post('/api/etl/datasources/test-connection', config);
    return response.data;
  },

  previewTransformation: async (config: {
    sources: Array<{
      datasource_id: number;
      selected_columns: string[];
      table_name: string;
    }>;
    transformation_prompt: string;
    limit?: number;
    model_name?: string;
  }) => {
    const response = await api.post('/api/etl/transformations/preview', config);
    return response.data;
  },

  // ETL Pipeline Management
  getPipelines: async () => {
    const response = await api.get('/api/etl/pipelines');
    return response.data;
  },

  createPipeline: async (pipeline: any) => {
    const response = await api.post('/api/etl/pipelines', pipeline);
    return response.data;
  },

  getPipeline: async (id: number) => {
    const response = await api.get(`/api/etl/pipelines/${id}`);
    return response.data;
  },

  updatePipeline: async (id: number, pipeline: any) => {
    const response = await api.put(`/api/etl/pipelines/${id}`, pipeline);
    return response.data;
  },

  deletePipeline: async (id: number) => {
    const response = await api.delete(`/api/etl/pipelines/${id}`);
    return response.data;
  },

  runPipeline: async (id: number) => {
    const response = await api.post(`/api/etl/pipelines/${id}/run`);
    return response.data;
  },
};

export const fileApi = {
  uploadFile: async (file: File, collectionName: string = 'default') => {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('collection_name', collectionName);

    const response = await api.post('/api/files/upload', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });
    return response.data;
  },
};

export default api;
