import { useQuery } from '@tanstack/react-query';
import { settingsApi } from '../../services/api';
import { useChatStore } from '../../store/chatStore';
import { Bot, Database, FolderOpen } from 'lucide-react';

export const ModelSelector = () => {
  const { selectedModel, selectedDataSource, selectedStore, setSelectedModel, setSelectedDataSource, setSelectedStore } = useChatStore();

  const { data: models } = useQuery({
    queryKey: ['models'],
    queryFn: settingsApi.getModels,
  });

  const { data: dataSources } = useQuery({
    queryKey: ['dataSources'],
    queryFn: settingsApi.getDataSources,
  });

  const { data: settings } = useQuery({
    queryKey: ['settings'],
    queryFn: settingsApi.getSettings,
  });

  return (
    <div className="flex gap-4 p-4 border-b bg-background">
      <div className="flex-1">
        <label className="flex items-center gap-2 text-sm font-medium mb-2">
          <Bot size={16} />
          Model
        </label>
        <select
          value={selectedModel}
          onChange={(e) => setSelectedModel(e.target.value)}
          className="w-full px-3 py-2 bg-background border border-input rounded-lg focus:outline-none focus:ring-2 focus:ring-ring"
        >
          <option value="">Select a model...</option>
          {models?.map((model: any) => (
            <option key={model.id} value={model.name}>
              {model.displayName || model.name}
            </option>
          ))}
        </select>
      </div>

      <div className="flex-1">
        <label className="flex items-center gap-2 text-sm font-medium mb-2">
          <FolderOpen size={16} />
          Data Source
        </label>
        <select
          value={selectedDataSource}
          onChange={(e) => setSelectedDataSource(e.target.value)}
          className="w-full px-3 py-2 bg-background border border-input rounded-lg focus:outline-none focus:ring-2 focus:ring-ring"
        >
          <option value="">Default</option>
          {dataSources?.map((source: any) => (
            <option key={source.id} value={source.name}>
              {source.displayName || source.name}
            </option>
          ))}
        </select>
      </div>

      <div className="flex-1">
        <label className="flex items-center gap-2 text-sm font-medium mb-2">
          <Database size={16} />
          Vector Store
        </label>
        <select
          value={selectedStore}
          onChange={(e) => setSelectedStore(e.target.value)}
          className="w-full px-3 py-2 bg-background border border-input rounded-lg focus:outline-none focus:ring-2 focus:ring-ring"
        >
          <option value="">None</option>
          {settings?.stores &&
            Object.entries(settings.stores).map(([key, store]) => (
              <option key={key} value={key}>
                {store.name}
              </option>
            ))}
        </select>
      </div>
    </div>
  );
};
