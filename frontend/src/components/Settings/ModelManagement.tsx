import React, { useState } from 'react';
import { createPortal } from 'react-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { settingsApi } from '../../services/api';
import { Plus, Edit2, Trash2, Bot, Loader2, Sparkles, Cpu } from 'lucide-react';

interface Model {
  id: number;
  name: string;
  displayName: string;
  provider: 'openai' | 'anthropic';
  api_key?: string;
  temperature?: number;
  max_tokens?: number;
  isActive: boolean;
}

interface ModelFormData {
  name: string;
  displayName: string;
  provider: 'openai' | 'anthropic';
  api_key: string;
  temperature: number;
  max_tokens: number;
}

const ModelManagement: React.FC = () => {
  const queryClient = useQueryClient();
  const [showAddModal, setShowAddModal] = useState(false);
  const [editingModel, setEditingModel] = useState<Model | null>(null);
  const [formData, setFormData] = useState<ModelFormData>({
    name: '',
    displayName: '',
    provider: 'openai',
    api_key: '',
    temperature: 0.7,
    max_tokens: 2000,
  });

  // Fetch models
  const { data: models = [], isLoading } = useQuery({
    queryKey: ['models'],
    queryFn: settingsApi.getModels,
  });

  // Add model mutation
  const addModelMutation = useMutation({
    mutationFn: settingsApi.addModel,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['models'] });
      setShowAddModal(false);
      resetForm();
    },
  });

  // Update model mutation
  const updateModelMutation = useMutation({
    mutationFn: ({ id, data }: { id: number; data: Partial<ModelFormData> }) =>
      settingsApi.updateModel(id, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['models'] });
      setEditingModel(null);
      resetForm();
    },
  });

  // Delete model mutation
  const deleteModelMutation = useMutation({
    mutationFn: settingsApi.deleteModel,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['models'] });
    },
  });

  const resetForm = () => {
    setFormData({
      name: '',
      displayName: '',
      provider: 'openai',
      api_key: '',
      temperature: 0.7,
      max_tokens: 2000,
    });
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (editingModel) {
      updateModelMutation.mutate({ id: editingModel.id, data: formData });
    } else {
      addModelMutation.mutate(formData);
    }
  };

  const handleEdit = (model: Model) => {
    setEditingModel(model);
    setFormData({
      name: model.name,
      displayName: model.displayName,
      provider: model.provider,
      api_key: model.api_key || '',
      temperature: model.temperature || 0.7,
      max_tokens: model.max_tokens || 2000,
    });
    setShowAddModal(true);
  };

  const handleDelete = (id: number) => {
    if (confirm('Are you sure you want to delete this model?')) {
      deleteModelMutation.mutate(id);
    }
  };

  const handleCloseModal = () => {
    setShowAddModal(false);
    setEditingModel(null);
    resetForm();
  };

  if (isLoading) {
    return <div className="p-6">Loading models...</div>;
  }

  return (
    <div className="p-6 max-w-6xl mx-auto">
      {/* Header */}
      <div className="flex justify-between items-center mb-8">
        <div>
          <h2 className="text-2xl font-bold text-white">Model Configuration</h2>
          <p className="text-slate-400 mt-1">Manage AI models and API configurations</p>
        </div>
        <button
          onClick={() => setShowAddModal(true)}
          className="flex items-center gap-2 px-4 py-2.5 bg-indigo-600 text-white rounded-xl hover:bg-indigo-500 transition-all shadow-lg hover:shadow-indigo-500/25 active:scale-95"
        >
          <Plus className="h-5 w-5" />
          Add Model
        </button>
      </div>

      {/* Models List */}
      <div className="grid gap-4">
        {models.length === 0 ? (
          <div className="text-center py-16 rounded-2xl border border-dashed border-white/10 bg-white/5 backdrop-blur-sm">
            <div className="w-16 h-16 bg-slate-800 rounded-full flex items-center justify-center mx-auto mb-4 border border-white/10">
              <Bot className="h-8 w-8 text-slate-400" />
            </div>
            <h3 className="text-lg font-medium text-white">No Models Configured</h3>
            <p className="text-slate-400 mt-2 max-w-sm mx-auto">
              Add OpenAI, Anthropic, or other models to power your chatbot.
            </p>
            <button
              onClick={() => setShowAddModal(true)}
              className="mt-6 text-indigo-400 hover:text-indigo-300 font-medium hover:underline"
            >
              Add your first model
            </button>
          </div>
        ) : (
          models.map((model: Model) => (
            <div
              key={model.id}
              className="glass-card rounded-xl p-5 group"
            >
              <div className="flex justify-between items-start">
                <div className="flex items-start gap-4">
                  <div className="p-3 bg-slate-800/50 rounded-xl border border-white/10">
                    <Sparkles className="h-5 w-5 text-indigo-400" />
                  </div>
                  <div>
                    <h3 className="text-lg font-semibold text-white">{model.displayName}</h3>
                    <p className="text-sm font-medium text-indigo-400 mt-0.5">
                      <span className="capitalize">{model.provider}</span> • {model.name}
                    </p>
                    <div className="mt-3 text-sm text-slate-400 space-y-1 flex gap-4">
                      <div className="flex items-center gap-2">
                        <span className="text-xs bg-slate-900/50 px-2 py-1 rounded border border-white/5 text-slate-300">
                          Temp: {model.temperature || 0.7}
                        </span>
                      </div>
                      <div className="flex items-center gap-2">
                        <span className="text-xs bg-slate-900/50 px-2 py-1 rounded border border-white/5 text-slate-300">
                          Context: {(model.max_tokens || 2000).toLocaleString()}
                        </span>
                      </div>
                    </div>
                  </div>
                </div>

                <div className="flex items-center gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
                  <button
                    onClick={() => handleEdit(model)}
                    className="p-2 text-slate-400 hover:text-white hover:bg-white/10 rounded-lg transition-colors"
                    title="Edit"
                  >
                    <Edit2 className="h-4 w-4" />
                  </button>
                  <button
                    onClick={() => handleDelete(model.id)}
                    className="p-2 text-slate-400 hover:text-red-400 hover:bg-red-500/10 rounded-lg transition-colors"
                    title="Delete"
                  >
                    <Trash2 className="h-4 w-4" />
                  </button>
                </div>
              </div>
            </div>
          ))
        )}
      </div>

      {/* Add/Edit Modal */}
      {showAddModal && createPortal(
        <div className="fixed inset-0 bg-black/60 backdrop-blur-md flex items-center justify-center z-[100] p-4">
          <div className="glass-panel w-full max-w-lg max-h-[90vh] overflow-y-auto rounded-2xl animate-in zoom-in-95 duration-200">
            <div className="p-6 border-b border-white/10 flex justify-between items-center">
              <h3 className="text-xl font-bold text-white">
                {editingModel ? 'Edit Model' : 'Add New Model'}
              </h3>
              <button onClick={handleCloseModal} className="text-slate-400 hover:text-white transition-colors">
                <Plus className="h-6 w-6 rotate-45" />
              </button>
            </div>

            <form onSubmit={handleSubmit} className="p-6 space-y-5">
              {/* Provider */}
              <div>
                <label className="block text-sm font-medium text-slate-300 mb-1">
                  Provider
                </label>
                <div className="relative">
                  <div className="absolute left-3 top-2.5">
                    <Cpu className="h-5 w-5 text-slate-400" />
                  </div>
                  <select
                    value={formData.provider}
                    onChange={(e) => setFormData({ ...formData, provider: e.target.value as 'openai' | 'anthropic' })}
                    className="w-full pl-10 pr-3 py-2.5 glass-input rounded-xl outline-none text-white bg-slate-900/50 appearance-none cursor-pointer"
                    required
                  >
                    <option value="openai" className="bg-slate-900 text-white">OpenAI</option>
                    <option value="anthropic" className="bg-slate-900 text-white">Anthropic</option>
                  </select>
                </div>
              </div>

              {/* Model Name */}
              <div>
                <label className="block text-sm font-medium text-slate-300 mb-1">
                  Model Name
                </label>
                <input
                  type="text"
                  value={formData.name}
                  onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                  placeholder="e.g., gpt-4, claude-3-5-sonnet-20241022"
                  className="w-full px-3 py-2.5 glass-input rounded-xl outline-none text-slate-500 placeholder:text-slate-500"
                  required
                />
              </div>

              {/* Display Name */}
              <div>
                <label className="block text-sm font-medium text-slate-300 mb-1">
                  Display Name
                </label>
                <input
                  type="text"
                  value={formData.displayName}
                  onChange={(e) => setFormData({ ...formData, displayName: e.target.value })}
                  placeholder="e.g., GPT-4, Claude 3.5 Sonnet"
                  className="w-full px-3 py-2.5 glass-input rounded-xl outline-none text-slate-500 placeholder:text-slate-500"
                  required
                />
              </div>

              {/* API Key */}
              <div>
                <label className="block text-sm font-medium text-slate-300 mb-1">
                  API Key
                </label>
                <input
                  type="password"
                  value={formData.api_key}
                  onChange={(e) => setFormData({ ...formData, api_key: e.target.value })}
                  placeholder="sk-..."
                  className="w-full px-3 py-2.5 glass-input rounded-xl outline-none text-slate-500 placeholder:text-slate-500"
                  required={!editingModel}
                />
                {editingModel && (
                  <p className="text-xs text-slate-400 mt-1">Leave empty to keep existing key</p>
                )}
              </div>

              {/* Temperature */}
              <div>
                <label className="block text-sm font-medium text-slate-300 mb-2">
                  Temperature: <span className="text-white font-mono ml-2">{formData.temperature}</span>
                </label>
                <input
                  type="range"
                  min="0"
                  max="2"
                  step="0.1"
                  value={formData.temperature}
                  onChange={(e) => setFormData({ ...formData, temperature: parseFloat(e.target.value) })}
                  className="w-full h-2 bg-slate-700 rounded-lg appearance-none cursor-pointer accent-indigo-500"
                />
                <div className="flex justify-between text-xs text-slate-500 mt-1">
                  <span>Precise (0.0)</span>
                  <span>Creative (2.0)</span>
                </div>
              </div>

              {/* Max Tokens */}
              <div>
                <label className="block text-sm font-medium text-slate-300 mb-1">
                  Max Tokens
                </label>
                <input
                  type="number"
                  value={formData.max_tokens}
                  onChange={(e) => setFormData({ ...formData, max_tokens: parseInt(e.target.value) })}
                  min="1"
                  max="200000"
                  className="w-full px-3 py-2.5 glass-input rounded-xl outline-none text-slate-500 placeholder:text-slate-500"
                  required
                />
              </div>

              {/* Buttons */}
              <div className="flex gap-3 pt-4">
                <button
                  type="button"
                  onClick={handleCloseModal}
                  className="flex-1 px-4 py-2.5 border border-white/10 text-slate-300 rounded-xl hover:bg-white/5 font-medium transition-colors"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={addModelMutation.isPending || updateModelMutation.isPending}
                  className="flex-1 px-4 py-2.5 bg-indigo-600 text-white rounded-xl hover:bg-indigo-500 disabled:bg-indigo-500/50 disabled:cursor-not-allowed font-medium transition-colors flex items-center justify-center gap-2 shadow-lg hover:shadow-indigo-500/25"
                >
                  {addModelMutation.isPending || updateModelMutation.isPending ? (
                    <>
                      <Loader2 className="h-4 w-4 animate-spin" />
                      Saving...
                    </>
                  ) : editingModel ? (
                    'Update Model'
                  ) : (
                    'Add Model'
                  )}
                </button>
              </div>
            </form>
          </div>
        </div>,
        document.body
      )}
    </div>
  );
};

export default ModelManagement;
