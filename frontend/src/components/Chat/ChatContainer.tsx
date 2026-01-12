import { useEffect, useRef, useState } from 'react';
import { useMutation } from '@tanstack/react-query';
import { Loader2 } from 'lucide-react';
import { ChatMessage } from './ChatMessage';
import { ChatInput } from './ChatInput';
import { chatApi } from '../../services/api';
import { useChatStore } from '../../store/chatStore';
import { useAuthStore } from '../../store/authStore';
import type { Message } from '../../types';

export const ChatContainer = () => {
  const { user } = useAuthStore();
  const {
    currentConversation,
    selectedModel,
    selectedDataSource,
    selectedStore,
    addMessage,
    setCurrentConversation,
  } = useChatStore();

  const [isLoading, setIsLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [currentConversation?.messages]);

  const sendMessageMutation = useMutation({
    mutationFn: chatApi.sendMessage,
    onSuccess: (data) => {
      // Update conversation with new messages
      const newMessages = data.history.map((msg, idx) => ({
        id: `${data.id}-${idx}`,
        type: msg.type.toLowerCase(),
        content: msg.data.content,
        timestamp: new Date().toISOString(),
      })) as Message[];

      if (!currentConversation) {
        setCurrentConversation({
          id: parseInt(data.id) || 0,
          chat_id: data.id,
          title: newMessages[0]?.content?.slice(0, 50) || 'New Chat',
          messages: newMessages,
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
        });
      } else {
        // Add only the AI response (last message in history)
        const aiMessage = newMessages[newMessages.length - 1];
        if (aiMessage && aiMessage.type === 'ai') {
          addMessage(aiMessage);
        }
      }

      setIsLoading(false);
    },
    onError: (error) => {
      console.error('Failed to send message:', error);
      setIsLoading(false);
    },
  });

  const handleSendMessage = (content: string) => {
    if (!user || !selectedModel) {
      return;
    }

    // Add user message immediately
    const userMessage: Message = {
      id: `temp-${Date.now()}`,
      type: 'human',
      content,
      timestamp: new Date().toISOString(),
    };

    addMessage(userMessage);
    setIsLoading(true);

    sendMessageMutation.mutate({
      message: content,
      chatId: currentConversation?.chat_id,
      model: selectedModel,
      dataSource: selectedDataSource || 'default',
      store: selectedStore,
    });
  };

  return (
    <div className="flex flex-col h-full relative">
      {/* Messages Area */}
      <div className="flex-1 overflow-y-auto p-4 space-y-6">
        <div className="max-w-4xl mx-auto space-y-6 pb-4">
          {currentConversation?.messages?.map((message) => (
            <ChatMessage key={message.id} message={message} />
          ))}

          {isLoading && (
            <div className="flex gap-4 p-4 rounded-2xl bg-white/40 backdrop-blur-sm border border-white/20 w-fit animate-pulse shadow-sm">
              <div className="flex-shrink-0">
                <div className="w-8 h-8 rounded-full flex items-center justify-center bg-blue-100 ring-2 ring-white">
                  <Loader2 size={16} className="animate-spin text-blue-600" />
                </div>
              </div>
              <div className="flex items-center">
                <p className="text-slate-600 text-sm font-medium">Buildy is thinking...</p>
              </div>
            </div>
          )}

          <div ref={messagesEndRef} />
        </div>
      </div>

      {/* Input Area */}
      <div className="p-4 border-t border-white/20 backdrop-blur-md">
        <div className="max-w-4xl mx-auto">
          <ChatInput
            onSendMessage={handleSendMessage}
            disabled={isLoading}
            hasModelSelected={!!selectedModel}
          />
        </div>
      </div>
    </div>
  );
};
