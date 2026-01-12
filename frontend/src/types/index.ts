export interface User {
  id: string;
  username: string;
  email: string;
  organization: string;
  is_org_admin: boolean;
}

export interface Message {
  id: string;
  type: 'human' | 'ai';
  content: string;
  timestamp: string;
}

export interface Conversation {
  id: number;
  chat_id: string;
  title: string;
  messages: Message[];
  created_at: string;
  updated_at: string;
}

export interface ModelSetting {
  id: string;
  name: string;
  displayName: string;
  api_key?: string;
  temperature?: number;
  max_tokens?: number;
}

export interface DataSource {
  id: string;
  name: string;
  displayName: string;
  type: string;
  config: Record<string, unknown>;
}

export interface VectorStore {
  id: string;
  name: string;
  type: string;
  url?: string;
}

export interface ChatRequest {
  message: string;
  chatId?: string;
  model: string;
  dataSource: string;
  store?: string;
}

export interface ChatHistoryMessage {
  type: string;
  data: {
    content: string;
  };
}

export interface ChatResponse {
  response: string;
  history: ChatHistoryMessage[];
  id: string;
  error?: boolean;
}

export interface Settings {
  models: Record<string, ModelSetting>;
  data_sources: Record<string, DataSource[]>;
  stores: Record<string, VectorStore>;
}
