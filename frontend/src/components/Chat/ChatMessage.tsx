import { memo } from 'react';
import ReactMarkdown from 'react-markdown';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism';
import { Copy, User, Bot } from 'lucide-react';
import { cn } from '../../lib/utils';
import type { Message } from '../../types';

interface ChatMessageProps {
  message: Message;
}

export const ChatMessage = memo(({ message }: ChatMessageProps) => {
  const isUser = message.type === 'human';

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  return (
    <div
      className={cn(
        'flex gap-4 p-5 rounded-3xl transition-all duration-300 hover:scale-[1.01] animate-in fade-in slide-in-from-bottom-2',
        isUser
          ? 'bg-white/30 backdrop-blur-md border border-blue-500/30 text-slate-800 ml-12'
          : 'bg-white/30 backdrop-blur-md border border-white/40 text-slate-800 mr-12 shadow-sm'
      )}
    >
      <div className="flex-shrink-0">
        <div
          className={cn(
            'w-8 h-8 rounded-full flex items-center justify-center ring-2 ring-white/10 shadow-lg',
            isUser ? 'bg-slate-600 text-slate-300' : 'bg-slate-700 text-indigo-400'
          )}
        >
          {isUser ? <User size={16} /> : <Bot size={18} />}
        </div>
      </div>
      <div className="flex-1 overflow-hidden prose prose-sm max-w-none prose-slate">
        <ReactMarkdown
          components={{
            code({ node, className, children, ...props }) {
              const match = /language-(\w+)/.exec(className || '');
              const codeString = String(children).replace(/\n$/, '');

              // eslint-disable-next-line @typescript-eslint/no-unused-vars
              const { style, ref, ...restProps } = props;

              return match ? (
                <div className="relative group my-4 rounded-xl overflow-hidden border border-white/20 shadow-lg">
                  <div className="absolute right-2 top-2 z-10">
                    <button
                      onClick={() => copyToClipboard(codeString)}
                      className="p-1.5 rounded-lg bg-white/10 hover:bg-white/20 text-white transition-colors backdrop-blur-sm"
                      title="Copy code"
                    >
                      <Copy size={14} />
                    </button>
                  </div>
                  <SyntaxHighlighter
                    children={codeString}
                    style={vscDarkPlus as any}
                    language={match[1]}
                    PreTag="div"
                    customStyle={{ margin: 0, borderRadius: 0, background: '#1e293b' }}
                  />
                </div>
              ) : (
                <code className={cn('px-1.5 py-0.5 rounded-md bg-white/50 border border-white/20 font-mono text-xs font-semibold text-blue-700', className)} {...restProps}>
                  {children}
                </code>
              );
            },
            p: ({ children }) => <p className="mb-1 last:mb-0 leading-relaxed">{children}</p>,
            a: ({ children, href }) => <a href={href} className="text-blue-600 hover:text-blue-700 underline decoration-blue-300 underline-offset-2 transition-colors" target="_blank" rel="noopener noreferrer">{children}</a>,
            ul: ({ children }) => <ul className="list-disc pl-4 space-y-1 my-2">{children}</ul>,
            ol: ({ children }) => <ol className="list-decimal pl-4 space-y-1 my-2">{children}</ol>,
          }}
        >
          {message.content}
        </ReactMarkdown>
      </div>
    </div>
  );
});

ChatMessage.displayName = 'ChatMessage';
