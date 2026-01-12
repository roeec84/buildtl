import React, { useState } from 'react';
import { createPortal } from 'react-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { settingsApi, fileApi } from '../../services/api';
import {
  Github,
  FileText,
  Globe,
  Type,
  Plus,
  Trash2,
  Edit2,
  Database,
  Loader2,
  Upload
} from 'lucide-react';

interface DataSource {
  id: number;
  name: string;
  displayName: string;
  type: 'github' | 'file' | 'url' | 'text' | 'sql';
  config: Record<string, any>;
  isActive: boolean;
  createdAt: string;
}

interface DataSourceFormData {
  name: string;
  type: 'github' | 'file' | 'url' | 'text' | 'sql';
  config: {
    repo_url?: string;
    file_path?: string;
    url?: string;
    text?: string;
    token?: string;
    branch?: string;
    collection_name?: string;
    engine?: string;
    host?: string;
    port?: string;
    database?: string;
    username?: string;
    password?: string;
    query?: string;
    credentials_json?: string;
  };
}

const DataSourceManagement: React.FC = () => {
  const queryClient = useQueryClient();
  const [showAddModal, setShowAddModal] = useState(false);
  const [editingSource, setEditingSource] = useState<DataSource | null>(null);
  const [formData, setFormData] = useState<DataSourceFormData>({
    name: '',
    type: 'file',
    config: {},
  });
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [isUploading, setIsUploading] = useState(false);

  // Fetch data sources
  const { data: dataSources = [], isLoading } = useQuery({
    queryKey: ['dataSources'],
    queryFn: settingsApi.getDataSources,
  });

  // Add data source mutation
  const addDataSourceMutation = useMutation({
    mutationFn: settingsApi.addDataSource,
    onSuccess: async (data) => {
      // If it's a non-file data source (github, url, text), process it automatically
      if (formData.type !== 'file') {
        try {
          await settingsApi.processDataSource(data.id);
        } catch (error) {
          console.error('Error processing data source:', error);
          alert('Data source created but processing failed. You can try processing it again from the list.');
        }
      }
      queryClient.invalidateQueries({ queryKey: ['dataSources'] });
      setShowAddModal(false);
      resetForm();
    },
  });

  // Update data source mutation
  const updateDataSourceMutation = useMutation({
    mutationFn: ({ id, data }: { id: number; data: Partial<DataSourceFormData> }) =>
      settingsApi.updateDataSource(id, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dataSources'] });
      setEditingSource(null);
      resetForm();
    },
  });

  // Delete data source mutation
  const deleteDataSourceMutation = useMutation({
    mutationFn: settingsApi.deleteDataSource,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['dataSources'] });
    },
  });

  const resetForm = () => {
    setFormData({
      name: '',
      type: 'file',
      config: {},
    });
    setSelectedFile(null);
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    try {
      let finalFormData = { ...formData };

      // If it's a file type and a file is selected, upload it first
      if (formData.type === 'file' && selectedFile) {
        setIsUploading(true);
        // Upload file with collection name matching the data source name
        const uploadResponse = await fileApi.uploadFile(selectedFile, formData.name);
        finalFormData = {
          ...formData,
          config: {
            ...formData.config,
            file_path: uploadResponse.file_path || uploadResponse.path,
            collection_name: formData.name, // Store the collection name for reference
          },
        };
        setIsUploading(false);
      }
      // Add displayName field for backend compatibility
      const dataSourcePayload = {
        ...finalFormData,
        displayName: finalFormData.name,
      };

      if (editingSource) {
        updateDataSourceMutation.mutate({ id: editingSource.id, data: dataSourcePayload });
      } else {
        addDataSourceMutation.mutate(dataSourcePayload);
      }
    } catch (error) {
      console.error('Error submitting data source:', error);
      setIsUploading(false);
      alert('Failed to upload file. Please try again.');
    }
  };

  const handleEdit = (source: DataSource) => {
    setEditingSource(source);
    setFormData({
      name: source.name,
      type: source.type,
      config: source.config,
    });
    setShowAddModal(true);
  };

  const handleDelete = (id: number) => {
    if (confirm('Are you sure you want to delete this data source?')) {
      deleteDataSourceMutation.mutate(id);
    }
  };

  const handleCloseModal = () => {
    setShowAddModal(false);
    setEditingSource(null);
    resetForm();
  };

  const renderConfigInputs = () => {
    switch (formData.type) {
      case 'github':
        return (
          <>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-1">
                Repository URL
              </label>
              <div className="relative">
                <Github className="absolute left-3 top-2.5 h-5 w-5 text-slate-400" />
                <input
                  type="url"
                  value={formData.config.repo_url || ''}
                  onChange={(e) => setFormData({
                    ...formData,
                    config: { ...formData.config, repo_url: e.target.value }
                  })}
                  placeholder="https://github.com/username/repo"
                  className="w-full pl-10 pr-3 py-2.5 glass-input rounded-xl outline-none"
                  required
                />
              </div>
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-1">
                Access Token (optional)
              </label>
              <input
                type="password"
                value={formData.config.token || ''}
                onChange={(e) => setFormData({
                  ...formData,
                  config: { ...formData.config, token: e.target.value }
                })}
                placeholder="ghp_..."
                className="w-full px-3 py-2.5 glass-input rounded-xl outline-none"
              />
              <p className="text-xs text-slate-500 mt-1">Required for private repositories</p>
            </div>
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-1">
                Branch (optional)
              </label>
              <input
                type="text"
                value={formData.config.branch || ''}
                onChange={(e) => setFormData({
                  ...formData,
                  config: { ...formData.config, branch: e.target.value }
                })}
                placeholder="main"
                className="w-full px-3 py-2.5 glass-input rounded-xl outline-none"
              />
              <p className="text-xs text-slate-500 mt-1">Defaults to 'main' if not specified</p>
            </div>
          </>
        );
      case 'url':
        return (
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-1">
              URL
            </label>
            <div className="relative">
              <Globe className="absolute left-3 top-2.5 h-5 w-5 text-slate-400" />
              <input
                type="url"
                value={formData.config.url || ''}
                onChange={(e) => setFormData({
                  ...formData,
                  config: { ...formData.config, url: e.target.value }
                })}
                placeholder="https://example.com/document"
                className="w-full pl-10 pr-3 py-2.5 glass-input rounded-xl outline-none"
                required
              />
            </div>
          </div>
        );
      case 'text':
        return (
          <div>
            <label className="block text-sm font-medium text-slate-300 mb-1">
              Text Content
            </label>
            <div className="relative">
              <Type className="absolute left-3 top-3 h-5 w-5 text-slate-400" />
              <textarea
                value={formData.config.text || ''}
                onChange={(e) => setFormData({
                  ...formData,
                  config: { ...formData.config, text: e.target.value }
                })}
                placeholder="Enter your text content here..."
                rows={6}
                className="w-full pl-10 pr-3 py-2.5 glass-input rounded-xl outline-none"
                required
              />
            </div>
          </div>
        );
      case 'file':
        return (
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              File Upload
            </label>
            <div className="border-2 border-dashed border-gray-300 rounded-lg p-6 text-center hover:border-blue-500 transition-colors">
              <input
                type="file"
                id="file-upload"
                className="hidden"
                onChange={(e) => {
                  const file = e.target.files?.[0];
                  if (file) {
                    setSelectedFile(file);
                  }
                }}
                accept=".pdf,.txt,.doc,.docx,.ppt,.pptx,.xls,.xlsx"
                required={!editingSource}
              />
              <label htmlFor="file-upload" className="cursor-pointer">
                <Upload className="mx-auto h-12 w-12 text-gray-400 mb-3" />
                <span className="text-gray-600">Click to upload or drag and drop</span>
                <p className="text-xs text-gray-400 mt-2">
                  PDF, TXT, DOC, DOCX, PPT, PPTX, XLS, XLSX
                </p>
              </label>
            </div>
            {selectedFile && (
              <div className="flex items-center gap-2 mt-3 p-2 bg-green-50 text-green-700 rounded-lg">
                <FileText className="h-4 w-4" />
                <span className="text-sm font-medium">{selectedFile.name}</span>
              </div>
            )}
            {editingSource && formData.config.file_path && !selectedFile && (
              <p className="text-sm text-gray-600 mt-2">
                Current file: {formData.config.file_path}
              </p>
            )}
          </div>
        );
      case 'sql':
        return (
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-slate-300 mb-1">
                Database Engine
              </label>
              <select
                value={formData.config.engine || 'postgresql'}
                onChange={(e) => setFormData({
                  ...formData,
                  config: { ...formData.config, engine: e.target.value }
                })}
                className="w-full px-3 py-2.5 glass-input rounded-xl outline-none text-white bg-slate-900/50 appearance-none cursor-pointer"
              >
                <option value="postgresql" className="bg-slate-900 text-white">PostgreSQL</option>
                <option value="mysql" className="bg-slate-900 text-white">MySQL</option>
                <option value="mssql" className="bg-slate-900 text-white">SQL Server</option>
                <option value="sqlite" className="bg-slate-900 text-white">SQLite</option>
                <option value="bigquery" className="bg-slate-900 text-white">BigQuery</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-slate-300 mb-1">
                {formData.config.engine === 'bigquery' ? 'Project ID' : 'Host'}
              </label>
              <input
                type="text"
                value={formData.config.host || ''}
                onChange={(e) => setFormData({
                  ...formData,
                  config: { ...formData.config, host: e.target.value }
                })}
                placeholder={formData.config.engine === 'bigquery' ? 'my-project-id' : 'localhost'}
                className="w-full px-3 py-2.5 glass-input rounded-xl outline-none"
                required
              />
            </div>
            {formData.config.engine !== 'bigquery' && (
              <div>
                <label className="block text-sm font-medium text-slate-300 mb-1">
                  Port
                </label>
                <input
                  type="text"
                  value={formData.config.port || ''}
                  onChange={(e) => setFormData({
                    ...formData,
                    config: { ...formData.config, port: e.target.value }
                  })}
                  placeholder="5432"
                  className="w-full px-3 py-2.5 glass-input rounded-xl outline-none"
                  required
                />
              </div>
            )}

            <div>
              <label className="block text-sm font-medium text-slate-300 mb-1">
                {formData.config.engine === 'bigquery' ? 'Dataset ID' : 'Database Name'}
              </label>
              <input
                type="text"
                value={formData.config.database || ''}
                onChange={(e) => setFormData({
                  ...formData,
                  config: { ...formData.config, database: e.target.value }
                })}
                placeholder="my_database"
                className="w-full px-3 py-2.5 glass-input rounded-xl outline-none"
                required
              />
            </div>

            {formData.config.engine === 'bigquery' ? (
              <div>
                <label className="block text-sm font-medium text-slate-300 mb-1">
                  Service Account JSON
                </label>
                <textarea
                  value={formData.config.credentials_json || ''}
                  onChange={(e) => setFormData({
                    ...formData,
                    config: { ...formData.config, credentials_json: e.target.value }
                  })}
                  placeholder='{ "type": "service_account", "project_id": ... }'
                  rows={5}
                  className="w-full px-3 py-2.5 glass-input rounded-xl outline-none font-mono text-xs"
                  required
                />
                <p className="text-xs text-slate-500 mt-1">Paste your Google Cloud Service Account JSON key here.</p>
              </div>
            ) : (
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-slate-300 mb-1">
                    Username
                  </label>
                  <input
                    type="text"
                    value={formData.config.username || ''}
                    onChange={(e) => setFormData({
                      ...formData,
                      config: { ...formData.config, username: e.target.value }
                    })}
                    placeholder="postgres"
                    className="w-full px-3 py-2.5 glass-input rounded-xl outline-none"
                    required
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-300 mb-1">
                    Password
                  </label>
                  <input
                    type="password"
                    value={formData.config.password || ''}
                    onChange={(e) => setFormData({
                      ...formData,
                      config: { ...formData.config, password: e.target.value }
                    })}
                    placeholder="••••••••"
                    className="w-full px-3 py-2.5 glass-input rounded-xl outline-none"
                    required
                  />
                </div>
              </div>
            )}

            {formData.config.engine !== 'bigquery' && (
              /* Port field - hiding for BQ */
              <div className="grid grid-cols-2 gap-4">
                {/* ... Host and Port logic can be tricky if we want to reuse Host for ProjectID. 
                      Let's conditionally show Port only if NOT bigquery.
                      Host is used as Project ID for BQ, which is fine.
                  */}
              </div>
            )}

            <div>
              <label className="block text-sm font-medium text-slate-300 mb-1">
                Custom Query (Optional)
              </label>
              <textarea
                value={formData.config.query || ''}
                onChange={(e) => setFormData({
                  ...formData,
                  config: { ...formData.config, query: e.target.value }
                })}
                placeholder="SELECT * FROM users WHERE active = true"
                rows={3}
                className="w-full px-3 py-2.5 glass-input rounded-xl outline-none font-mono text-xs"
              />
              <p className="text-xs text-slate-500 mt-1">If blank, all tables will be indexed.</p>
            </div>
          </div>
        );
      default:
        return null;
    }
  };

  const getTypeIcon = (type: string) => {
    switch (type) {
      case 'github': return <Github className="h-5 w-5 text-slate-400" />;
      case 'file': return <FileText className="h-5 w-5 text-slate-400" />;
      case 'url': return <Globe className="h-5 w-5 text-slate-400" />;
      case 'text': return <Type className="h-5 w-5 text-slate-400" />;
      case 'sql': return <Database className="h-5 w-5 text-slate-400" />;
      default: return <Database className="h-5 w-5 text-slate-400" />;
    }
  };

  const getTypeDisplay = (type: string) => {
    const types: Record<string, string> = {
      github: 'GitHub Repository',
      file: 'File Upload',
      url: 'Web URL',
      text: 'Text Content',
      sql: 'SQL Database',
    };
    return types[type] || type;
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-blue-500" />
      </div>
    );
  };

  const isFormSubmitting = addDataSourceMutation.isPending || updateDataSourceMutation.isPending || isUploading;

  return (
    <div className="p-6 max-w-6xl mx-auto">
      {/* Header */}
      <div className="flex justify-between items-center mb-8">
        <div>
          <h2 className="text-2xl font-bold text-white">Data Sources</h2>
          <p className="text-slate-400 mt-1">Manage your knowledge base sources for RAG</p>
        </div>
        <button
          onClick={() => setShowAddModal(true)}
          className="flex items-center gap-2 px-4 py-2.5 bg-indigo-600 text-white rounded-xl hover:bg-indigo-500 transition-all shadow-lg hover:shadow-indigo-500/25 active:scale-95"
        >
          <Plus className="h-5 w-5" />
          Add Data Source
        </button>
      </div>

      {/* Data Sources List */}
      <div className="grid gap-4">
        {dataSources.length === 0 ? (
          <div className="text-center py-16 rounded-2xl border border-dashed border-white/10 bg-white/5 backdrop-blur-sm">
            <div className="w-16 h-16 bg-slate-800 rounded-full flex items-center justify-center mx-auto mb-4 border border-white/10">
              <Database className="h-8 w-8 text-slate-400" />
            </div>
            <h3 className="text-lg font-medium text-white">No Data Sources</h3>
            <p className="text-slate-400 mt-2 max-w-sm mx-auto">
              Start by adding documents, websites, or repositories to enhance your chatbot's knowledge.
            </p>
            <button
              onClick={() => setShowAddModal(true)}
              className="mt-6 text-indigo-400 hover:text-indigo-300 font-medium hover:underline"
            >
              Add your first source
            </button>
          </div>
        ) : (
          dataSources.map((source: DataSource) => (
            <div
              key={source.id}
              className="glass-card rounded-xl p-5 group"
            >
              <div className="flex justify-between items-start">
                <div className="flex items-start gap-4">
                  <div className="p-3 bg-slate-800/50 rounded-xl border border-white/10">
                    {getTypeIcon(source.type)}
                  </div>
                  <div>
                    <h3 className="text-lg font-semibold text-white">{source.displayName}</h3>
                    <p className="text-sm font-medium text-indigo-400 mt-0.5">
                      {getTypeDisplay(source.type)}
                    </p>
                    <div className="mt-3 text-sm text-slate-400 space-y-1">
                      {source.type === 'github' && source.config.repo_url && (
                        <div className="flex items-center gap-2">
                          <Github className="h-3 w-3" />
                          <span className="font-mono text-xs bg-slate-900/50 px-2 py-1 rounded border border-white/5 text-slate-300">
                            {source.config.repo_url.replace('https://github.com/', '')}
                          </span>
                          {source.config.branch && (
                            <span className="text-slate-500 text-xs">• {source.config.branch}</span>
                          )}
                        </div>
                      )}
                      {source.type === 'url' && source.config.url && (
                        <div className="flex items-center gap-2">
                          <Globe className="h-3 w-3" />
                          <span className="truncate max-w-md text-slate-300 hover:text-indigo-400 transition-colors">{source.config.url}</span>
                        </div>
                      )}
                      {source.type === 'file' && source.config.file_path && (
                        <div className="flex items-center gap-2">
                          <FileText className="h-3 w-3" />
                          <span className="font-mono text-xs text-slate-300">{source.config.file_path.split('/').pop()}</span>
                        </div>
                      )}
                      {source.type === 'text' && (
                        <div className="flex items-center gap-2">
                          <Type className="h-3 w-3" />
                          <span>{(source.config.text?.length || 0).toLocaleString()} characters</span>
                        </div>
                      )}
                      {source.type === 'sql' && (
                        <div className="flex items-center gap-2">
                          <Database className="h-3 w-3" />
                          <span className="font-mono text-xs text-slate-300">
                            {source.config.engine}://{source.config.host}/{source.config.database}
                          </span>
                        </div>
                      )}
                    </div>
                  </div>
                </div>

                <div className="flex items-center gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
                  <span className="text-xs text-slate-500 mr-2">
                    {new Date(source.createdAt).toLocaleDateString()}
                  </span>
                  <button
                    onClick={() => handleEdit(source)}
                    className="p-2 text-slate-400 hover:text-white hover:bg-white/10 rounded-lg transition-colors"
                    title="Edit"
                  >
                    <Edit2 className="h-4 w-4" />
                  </button>
                  <button
                    onClick={() => handleDelete(source.id)}
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
                {editingSource ? 'Edit Data Source' : 'Add New Data Source'}
              </h3>
              <button onClick={handleCloseModal} className="text-slate-400 hover:text-white transition-colors">
                <Plus className="h-6 w-6 rotate-45" />
              </button>
            </div>

            <form onSubmit={handleSubmit} className="p-6 space-y-5">
              {/* Type Selection */}
              <div className="grid grid-cols-5 gap-2 mb-6">
                {['file', 'github', 'url', 'text', 'sql'].map((type) => (
                  <button
                    key={type}
                    type="button"
                    onClick={() => {
                      setFormData({
                        ...formData,
                        type: type as any,
                        config: {}
                      });
                      setSelectedFile(null);
                    }}
                    className={`flex flex-col items-center justify-center p-3 rounded-xl border transition-all ${formData.type === type
                      ? 'border-indigo-500/50 bg-indigo-500/10 text-indigo-400 shadow-md shadow-indigo-500/10'
                      : 'border-white/5 hover:border-white/10 text-slate-400 hover:bg-white/5 hover:text-slate-200'
                      }`}
                  >
                    {type === 'file' && <FileText className="h-6 w-6 mb-2" />}
                    {type === 'github' && <Github className="h-6 w-6 mb-2" />}
                    {type === 'url' && <Globe className="h-6 w-6 mb-2" />}
                    {type === 'text' && <Type className="h-6 w-6 mb-2" />}
                    {type === 'sql' && <Database className="h-6 w-6 mb-2" />}
                    <span className="text-xs font-medium capitalize">{type}</span>
                  </button>
                ))}
              </div>

              {/* Name */}
              <div>
                <label className="block text-sm font-medium text-slate-300 mb-1">
                  Name
                </label>
                <input
                  type="text"
                  value={formData.name}
                  onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                  placeholder="e.g., Project Documentation"
                  className="w-full px-3 py-2.5 glass-input rounded-xl outline-none"
                  required
                />
              </div>

              {/* Dynamic Inputs */}
              <div className="bg-slate-950/30 rounded-xl p-4 border border-white/5">
                {renderConfigInputs()}
              </div>

              {/* Action Buttons */}
              <div className="flex gap-3 pt-4">
                <button
                  type="button"
                  onClick={handleCloseModal}
                  disabled={isFormSubmitting}
                  className="flex-1 px-4 py-2.5 border border-white/10 text-slate-300 rounded-xl hover:bg-white/5 font-medium transition-colors"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={isFormSubmitting}
                  className="flex-1 px-4 py-2.5 bg-indigo-600 text-white rounded-xl hover:bg-indigo-500 disabled:bg-indigo-500/50 disabled:cursor-not-allowed font-medium transition-colors flex items-center justify-center gap-2 shadow-lg hover:shadow-indigo-500/25"
                >
                  {isUploading || isFormSubmitting ? (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  ) : null}
                  {isUploading
                    ? 'Uploading...'
                    : isFormSubmitting
                      ? 'Saving...'
                      : editingSource
                        ? 'Update Source'
                        : 'Add Source'}
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

export default DataSourceManagement;
