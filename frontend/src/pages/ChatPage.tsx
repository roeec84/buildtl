import { useNavigate } from 'react-router-dom';
import { ChatContainer } from '../components/Chat/ChatContainer';
import { ConversationList } from '../components/Sidebar/ConversationList';
import { ModelSelector } from '../components/Settings/ModelSelector';
import { Network } from 'lucide-react';

export const ChatPage = () => {
  const navigate = useNavigate();

  return (
    <div className="flex h-screen p-4 gap-4">
      {/* Sidebar */}
      <div className="w-80 glass-panel rounded-2xl overflow-hidden shadow-2xl transition-all duration-300">
        <ConversationList />
      </div>

      {/* Main Chat Area */}
      <div className="flex-1 flex flex-col overflow-hidden glass-panel rounded-2xl shadow-2xl relative text-slate-300">
        <div className="flex items-center justify-between border-b border-white/5 px-4">
          <ModelSelector />
          <button
            onClick={() => navigate('/etl')}
            className="flex items-center gap-2 px-4 py-2 bg-purple-600 hover:bg-purple-500 text-white rounded-xl transition-all shadow-lg shadow-purple-500/25 font-medium"
            title="Open ETL Data Factory"
          >
            <Network className="w-4 h-4" />
            ETL Pipeline
          </button>
        </div>
        <div className="flex-1 overflow-hidden relative">
          <ChatContainer />
        </div>
      </div>
    </div>
  );
};
