import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { ArrowLeft } from 'lucide-react';
import ModelManagement from '../components/Settings/ModelManagement';
import DataSourceManagement from '../components/Settings/DataSourceManagement';

const SettingsPage: React.FC = () => {
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState<'models' | 'dataSources'>('models');

  return (
    <div className="min-h-screen p-6">
      <div className="max-w-7xl mx-auto px-4 py-8">
        <div className="flex items-center gap-4 mb-8">
          <button
            onClick={() => navigate('/')}
            className="p-2 hover:bg-white/10 rounded-lg transition-colors text-slate-200 hover:text-white"
            title="Back to chat"
          >
            <ArrowLeft size={24} />
          </button>
          <h1 className="text-3xl font-bold text-white tracking-tight">Settings</h1>
        </div>

        {/* Tabs */}
        <div className="flex space-x-1 p-1 bg-slate-900/40 backdrop-blur-md rounded-xl mb-6 w-fit border border-white/5">
          <button
            onClick={() => setActiveTab('models')}
            className={`
                py-2.5 px-6 rounded-lg font-medium text-sm transition-all duration-200
                ${activeTab === 'models'
                ? 'bg-indigo-600 text-white shadow-lg shadow-indigo-500/25'
                : 'text-slate-400 hover:text-white hover:bg-white/5'
              }
              `}
          >
            Models
          </button>
          {/* Data Sources tab hidden for now */}
        </div>

        {/* Tab Content */}
        <div className="glass-panel rounded-2xl p-1 shadow-2xl">
          {activeTab === 'models' ? (
            <ModelManagement />
          ) : (
            <DataSourceManagement />
          )}
        </div>
      </div>
    </div>
  );
};

export default SettingsPage;
