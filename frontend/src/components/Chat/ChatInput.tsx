import { useState, type KeyboardEvent } from 'react';
import { Send, AlertCircle } from 'lucide-react';

interface ChatInputProps {
  onSendMessage: (message: string) => void;
  disabled?: boolean;
  hasModelSelected?: boolean;
}

export const ChatInput = ({ onSendMessage, disabled, hasModelSelected = true }: ChatInputProps) => {
  const [message, setMessage] = useState('');
  const [showTooltip, setShowTooltip] = useState(false);

  const handleSend = () => {
    if (!message.trim() || disabled) {
      return;
    }

    if (!hasModelSelected) {
      setShowTooltip(true);
      setTimeout(() => setShowTooltip(false), 3000);
      return;
    }

    onSendMessage(message.trim());
    setMessage('');
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  return (
    <div className="p-0 bg-transparent">
      <div className="flex gap-3 max-w-4xl mx-auto relative group">
        <textarea
          value={message}
          onChange={(e) => setMessage(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Start typing..."
          disabled={disabled}
          rows={1}
          className="w-full glass-input rounded-2xl px-5 py-4 text-sm text-slate-300 resize-none outline-none ring-offset-0 focus:ring-1 focus:ring-indigo-500/50 min-h-[56px] max-h-32 shadow-lg"
          style={{ height: 'auto', minHeight: '56px' }}
        />
        <div className="absolute right-2 bottom-2">
          <button
            onClick={handleSend}
            disabled={disabled || !message.trim()}
            className="p-2.5 bg-indigo-600 hover:bg-indigo-500 text-slate-300 rounded-xl disabled:opacity-50 disabled:bg-slate-700 transition-all shadow-lg hover:shadow-indigo-500/25 active:scale-95"
          >
            <Send size={18} />
          </button>

          {showTooltip && (
            <div className="absolute bottom-full right-0 mb-3 px-3 py-2 bg-red-500/90 backdrop-blur-md text-slate-300 text-xs font-semibold rounded-lg shadow-xl whitespace-nowrap flex items-center gap-2 animate-in fade-in slide-in-from-bottom-2 duration-200">
              <AlertCircle size={14} />
              Please select a model first
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
