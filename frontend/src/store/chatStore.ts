import { create } from 'zustand';
import type { Conversation, Message } from '../types';

interface ChatState {
  conversations: Conversation[];
  currentConversation: Conversation | null;
  selectedModel: string;
  selectedDataSource: string;
  selectedStore: string;
  setConversations: (conversations: Conversation[]) => void;
  setCurrentConversation: (conversation: Conversation | null) => void;
  addMessage: (message: Message) => void;
  setSelectedModel: (model: string) => void;
  setSelectedDataSource: (dataSource: string) => void;
  setSelectedStore: (store: string) => void;
  clearCurrentConversation: () => void;
}

export const useChatStore = create<ChatState>((set) => ({
  conversations: [],
  currentConversation: null,
  selectedModel: '',
  selectedDataSource: '',
  selectedStore: '',
  setConversations: (conversations) => set({ conversations }),
  setCurrentConversation: (conversation) => set({ currentConversation: conversation }),
  addMessage: (message) =>
    set((state) => ({
      currentConversation: state.currentConversation
        ? {
            ...state.currentConversation,
            messages: [...(state.currentConversation.messages || []), message],
          }
        : null,
    })),
  setSelectedModel: (model) => set({ selectedModel: model }),
  setSelectedDataSource: (dataSource) => set({ selectedDataSource: dataSource }),
  setSelectedStore: (store) => set({ selectedStore: store }),
  clearCurrentConversation: () => set({ currentConversation: null }),
}));
