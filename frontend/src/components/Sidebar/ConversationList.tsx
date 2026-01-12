import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { MessageSquare, Trash2, Edit2, Check, Plus, Settings } from 'lucide-react';
import { chatApi } from '../../services/api';
import { useChatStore } from '../../store/chatStore';
import { cn } from '../../lib/utils';
import { useState } from 'react';
import { useNavigate } from 'react-router-dom';

export const ConversationList = () => {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { currentConversation, setCurrentConversation, clearCurrentConversation } = useChatStore();
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editTitle, setEditTitle] = useState('');

  const { data: conversations = [], isLoading } = useQuery({
    queryKey: ['conversations'],
    queryFn: chatApi.getConversations,
  });

  const deleteMutation = useMutation({
    mutationFn: chatApi.deleteConversation,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['conversations'] });
      if (currentConversation) {
        clearCurrentConversation();
      }
    },
  });

  const updateConversationMutation = useMutation({
    mutationFn: ({ chatId, title }: { chatId: string; title: string }) =>
      chatApi.updateConversation(chatId, title),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['conversations'] });
    },
  });

  const handleNewChat = () => {
    clearCurrentConversation();
  };

  const handleDelete = (e: React.MouseEvent, chatId: string | undefined) => {
    e.stopPropagation();
    if (!chatId) {
      console.warn('Cannot delete conversation: chat_id is undefined');
      return;
    }
    if (confirm('Are you sure you want to delete this conversation?')) {
      deleteMutation.mutate(chatId);
    }
  };

  const handleEdit = (e: React.MouseEvent, id: string, title: string) => {
    e.stopPropagation();
    setEditingId(id);
    setEditTitle(title);
  };

  // Handle edit save
  const handleSaveEdit = (e: React.MouseEvent) => {
    e.stopPropagation();
    if (editingId && editTitle.trim()) {
      updateConversationMutation.mutate({
        chatId: editingId,
        title: editTitle
      });
      setEditingId(null);
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') handleSaveEdit(e as any);
    if (e.key === 'Escape') setEditingId(null);
  };

  return (
    <div className="flex flex-col h-full bg-muted/30 text-slate-300">
      <div className="p-4 border-b">
        <button
          onClick={handleNewChat}
          className="w-full flex items-center gap-2 px-4 py-2 bg-primary text-primary-foreground rounded-lg hover:bg-primary/90 transition-colors"
        >
          <Plus size={18} />
          New Chat
        </button>
      </div>

      <div className="flex-1 overflow-y-auto p-2">
        {isLoading ? (
          <div className="flex-1 overflow-y-auto p-2 space-y-2">
            {[1, 2, 3].map(i => (
              <div key={i} className="h-12 bg-slate-800/50 rounded-lg animate-pulse" />
            ))}
          </div>
        ) : conversations.length === 0 ? (
          <div className="text-center text-muted-foreground p-4">
            No conversations yet
          </div>
        ) : (
          <div className="space-y-1">
            {conversations.map((conv) => (
              <div
                key={conv.chat_id} // Use chat_id as key
                onClick={() => setCurrentConversation(conv)}
                className={cn(
                  'group flex items-center justify-between gap-2 px-3 py-2 rounded-lg cursor-pointer transition-colors',
                  currentConversation?.chat_id === conv.chat_id
                    ? 'bg-primary/10 border border-primary/20'
                    : 'hover:bg-muted'
                )}
              >
                <div className="flex items-center gap-2 flex-1 min-w-0">
                  <MessageSquare size={16} className="flex-shrink-0" />
                  {editingId === conv.chat_id ? (
                    <input
                      type="text"
                      value={editTitle}
                      onChange={(e) => setEditTitle(e.target.value)}
                      onClick={(e) => e.stopPropagation()}
                      onBlur={() => setEditingId(null)}
                      onKeyDown={handleKeyPress}
                      className="flex-1 px-2 py-1 text-sm bg-background border rounded"
                      autoFocus
                    />
                  ) : (
                    <span className="flex-1 text-sm truncate">{conv.title}</span>
                  )}
                </div>
                <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                  {editingId === conv.chat_id ? (
                    <button
                      onClick={(e) => handleSaveEdit(e)}
                      className="p-1 hover:bg-primary/20 rounded text-primary"
                      title="Save title"
                    >
                      <Check size={14} />
                    </button>
                  ) : (
                    <button
                      onClick={(e) => handleEdit(e, conv.chat_id, conv.title)}
                      className="p-1 hover:bg-background rounded"
                      title="Edit title"
                    >
                      <Edit2 size={14} />
                    </button>
                  )}
                  <button
                    onClick={(e) => handleDelete(e, conv.chat_id)}
                    className="p-1 hover:bg-destructive/20 rounded text-destructive"
                    title="Delete conversation"
                  >
                    <Trash2 size={14} />
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Settings Button */}
      <div className="p-4 border-t text-slate-300">
        <button
          onClick={() => navigate('/settings')}
          className="w-full flex items-center gap-2 px-4 py-2 text-slate-300 hover:scale-[1.01] animate-in fade-in slide-in-from-bottom-2 rounded-lg transition-colors"
        >
          <Settings size={18} />
          Settings
        </button>
      </div>
    </div>
  );
};
