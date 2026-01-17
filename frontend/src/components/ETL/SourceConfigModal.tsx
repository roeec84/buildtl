import React, { useState } from 'react';
import { createPortal } from 'react-dom';
import { X, Database, Check, AlertCircle, Loader2, Trash2 } from 'lucide-react';
import { etlApi } from '../../services/api';

interface ColumnInfo {
    name: string;
    type: string;
    nullable: boolean;
}

interface ETLDataSource {
    id: number;
    name: string;
    linked_service_id: number;
    table_name: string;
    linked_service?: LinkedService;
}

interface LinkedService {
    id: number;
    name: string;
    description?: string;
    service_type: string;
}

interface SourceConfigModalProps {
    isOpen: boolean;
    onClose: () => void;
    onSave: (config: {
        datasourceId?: number;
        tableName?: string;
        selectedColumns?: string[];
    }) => void;
    existingConfig?: {
        datasourceId?: number;
        tableName?: string;
        selectedColumns?: string[];
    };
}

type Step = 'select-dataset' | 'select-linked-service' | 'create-linked-service' | 'create-dataset' | 'columns';

const DB_TYPES = [
    { value: 'postgresql', label: 'PostgreSQL' },
    { value: 'mysql', label: 'MySQL' },
    { value: 'sql_server', label: 'SQL Server' },
    { value: 'azure_sql', label: 'Azure SQL Database' },
    { value: 'bigquery', label: 'Google BigQuery' },
    { value: 's3', label: 'AWS S3 / MinIO' },
    { value: 'gcs', label: 'Google Cloud Storage' },
    { value: 'adls', label: 'Azure Data Lake Gen2' },
];

export const SourceConfigModal: React.FC<SourceConfigModalProps> = ({
    isOpen,
    onClose,
    onSave,
    existingConfig,
}) => {
    const [step, setStep] = useState<Step>('select-linked-service');
    const [dataSources, setDataSources] = useState<ETLDataSource[]>([]);
    const [selectedSourceId, setSelectedSourceId] = useState<number | null>(
        existingConfig?.datasourceId || null
    );

    // Linked Services
    const [linkedServices, setLinkedServices] = useState<LinkedService[]>([]);
    const [selectedLinkedServiceId, setSelectedLinkedServiceId] = useState<number | null>(null);

    // Create new source form
    const [dbType, setDbType] = useState('postgresql');
    const [serviceName, setServiceName] = useState(''); // Renamed from sourceName for clarity
    const [datasetName, setDatasetName] = useState(''); // Display name for dataset
    const [tableName, setTableName] = useState('');
    const [connectionConfig, setConnectionConfig] = useState<Record<string, string>>({});

    // Connection testing
    const [testingConnection, setTestingConnection] = useState(false);
    const [connectionStatus, setConnectionStatus] = useState<{
        success?: boolean;
        message?: string;
    }>({});

    // Column selection
    const [columns, setColumns] = useState<ColumnInfo[]>([]);
    const [selectedColumns, setSelectedColumns] = useState<string[]>(
        existingConfig?.selectedColumns || []
    );
    const [loadingSchema, setLoadingSchema] = useState(false);

    // Sync state with existingConfig when modal opens
    // Sync state with existingConfig when modal opens
    React.useEffect(() => {
        if (isOpen) {
            if (existingConfig?.datasourceId) {
                // Determine linked service for existing datasource
                const hydrateState = async () => {
                    setLoadingSchema(true);
                    try {
                        // Load Schema
                        const schemaData = await etlApi.getTableSchema(existingConfig.datasourceId!);
                        if (schemaData && Array.isArray(schemaData.columns)) {
                            setColumns(schemaData.columns);
                        }

                        // Load Datasources to find the Linked Service ID
                        const dsData = await etlApi.getDataSources();
                        setDataSources(dsData);

                        const currentDs = dsData.find((d: ETLDataSource) => d.id === existingConfig.datasourceId);
                        if (currentDs) {
                            setSelectedLinkedServiceId(currentDs.linked_service_id);
                        }

                        setSelectedSourceId(existingConfig.datasourceId!);
                        setSelectedColumns(existingConfig.selectedColumns || []);
                        setStep('columns');

                        // Also fetch linked services in background so they are ready if user goes back
                        fetchLinkedServices();
                    } catch (error) {
                        console.error("Failed to hydrate state:", error);
                    } finally {
                        setLoadingSchema(false);
                    }
                };
                hydrateState();
            } else {
                // New node
                resetState();
                fetchLinkedServices();
                // We don't fetch datasources here immediately, we wait for service selection step?
                // Actually resetState sets step to 'select-linked-service', which triggers fetchLinkedServices
            }
        }
    }, [isOpen, existingConfig?.datasourceId]);

    const resetState = () => {
        setSelectedSourceId(null);
        setSelectedColumns([]);
        setColumns([]);
        setColumns([]);
        // Default to service selection for new flow
        setStep('select-linked-service');
        setLinkedServices([]);
        setSelectedLinkedServiceId(null);
        setServiceName('');
        setDatasetName('');
        setTableName('');
        setConnectionConfig({});
        setConnectionStatus({});
    };

    const fetchDataSources = async () => {
        try {
            const data = await etlApi.getDataSources();
            setDataSources(data);
        } catch (error) {
            console.error('Failed to fetch data sources:', error);
        }
    };

    const fetchLinkedServices = async () => {
        try {
            const data = await etlApi.linkedServices.list();
            setLinkedServices(data);
        } catch (error) {
            console.error('Failed to fetch linked services:', error);
        }
    };

    // Load linked services when entering relevant steps
    // Load data when entering relevant steps
    React.useEffect(() => {
        if (step === 'select-linked-service') {
            fetchLinkedServices();
        } else if (step === 'select-dataset') {
            fetchDataSources();
            // We don't fetch linkedServices here anymore as we came from that step
        }
    }, [step]);

    const handleDeleteSource = async (e: React.MouseEvent, sourceId: number) => {
        e.stopPropagation();
        if (confirm('Are you sure you want to delete this dataset?')) {
            try {
                await etlApi.deleteDataSource(sourceId);
                await fetchDataSources();
            } catch (error) {
                console.error('Failed to delete source:', error);
            }
        }
    };

    const handleTestLinkedService = async () => {
        setTestingConnection(true);
        setConnectionStatus({});
        try {
            const result = await etlApi.linkedServices.test({
                service_type: dbType,
                connection_config: connectionConfig,
            });
            setConnectionStatus(result);
        } catch (error) {
            setConnectionStatus({ success: false, message: 'Connection failed' });
        } finally {
            setTestingConnection(false);
        }
    };

    const handleCreateLinkedService = async () => {
        try {
            const newService = await etlApi.linkedServices.create({
                name: serviceName,
                service_type: dbType,
                connection_config: connectionConfig,
            });
            // After creating service, go to create dataset using this service
            setSelectedLinkedServiceId(newService.id);
            setStep('create-dataset');
        } catch (error) {
            console.error('Failed to create linked service:', error);
        }
    };

    const handleCreateDataset = async () => {
        if (!selectedLinkedServiceId) return;
        try {
            // First we might want to test if table exists?
            // Existing 'createDataSource' didn't require test.

            const newSource = await etlApi.createDataSource({
                name: datasetName,
                linked_service_id: selectedLinkedServiceId,
                table_name: tableName,
            });

            setSelectedSourceId(newSource.id);
            await loadSchema(newSource.id);
            setStep('columns');
        } catch (error) {
            console.error('Failed to create dataset:', error);
        }
    };

    const loadSchema = async (sourceId: number) => {
        setLoadingSchema(true);
        try {
            const data = await etlApi.getTableSchema(sourceId);
            if (data && Array.isArray(data.columns)) {
                setColumns(data.columns);
            } else {
                setColumns([]);
            }
        } catch (error) {
            console.error('Failed to load schema:', error);
        } finally {
            setLoadingSchema(false);
        }
    };

    const handleSelectExistingDataset = (sourceId: number) => {
        setSelectedSourceId(sourceId);
        setStep('columns');
        loadSchema(sourceId);
    };

    const handleSaveConfig = () => {
        const selectedSource = dataSources.find(ds => ds.id === selectedSourceId);
        onSave({
            datasourceId: selectedSourceId || undefined,
            tableName: selectedSource?.table_name || tableName,
            selectedColumns,
        });
        onClose();
    };

    const toggleColumn = (columnName: string) => {
        setSelectedColumns(prev =>
            prev.includes(columnName)
                ? prev.filter(c => c !== columnName)
                : [...prev, columnName]
        );
    };

    if (!isOpen) return null;

    const renderLinkedServiceForm = () => {
        const updateConfig = (key: string, value: string) => {
            setConnectionConfig(prev => ({ ...prev, [key]: value }));
        };

        return (
            <div className="space-y-4">
                <div>
                    <label className="block text-sm font-medium text-slate-700 dark:text-slate-200 mb-2">Service Name</label>
                    <input
                        type="text"
                        value={serviceName}
                        onChange={(e) => setServiceName(e.target.value)}
                        className="w-full glass-input p-3 rounded-xl outline-none"
                        placeholder="Production DB"
                    />
                </div>
                <div>
                    <label className="block text-sm font-medium text-slate-700 dark:text-slate-200 mb-2">Database Type</label>
                    <select
                        value={dbType}
                        onChange={(e) => setDbType(e.target.value)}
                        className="w-full glass-input p-3 rounded-xl outline-none"
                    >
                        {DB_TYPES.map(type => (
                            <option key={type.value} value={type.value}>{type.label}</option>
                        ))}
                    </select>
                </div>
                {/* Simplified Config Fields rendering for brevity - same logic as before but updated labels/vars */}
                {['postgresql', 'mysql'].includes(dbType) && (
                    <>
                        <div className="grid grid-cols-2 gap-4">
                            <div>
                                <label className="block text-xs text-slate-400 mb-1">Host</label>
                                <input type="text" value={connectionConfig.host || ''} onChange={e => updateConfig('host', e.target.value)} className="w-full glass-input p-2 rounded-lg" placeholder="localhost" />
                            </div>
                            <div>
                                <label className="block text-xs text-slate-400 mb-1">Port</label>
                                <input type="number" value={connectionConfig.port || ''} onChange={e => updateConfig('port', e.target.value)} className="w-full glass-input p-2 rounded-lg" />
                            </div>
                        </div>
                        <div>
                            <label className="block text-xs text-slate-400 mb-1">Database</label>
                            <input type="text" value={connectionConfig.database || ''} onChange={e => updateConfig('database', e.target.value)} className="w-full glass-input p-2 rounded-lg" />
                        </div>
                        <div className="grid grid-cols-2 gap-4">
                            <div>
                                <label className="block text-xs text-slate-400 mb-1">Username</label>
                                <input type="text" value={connectionConfig.username || ''} onChange={e => updateConfig('username', e.target.value)} className="w-full glass-input p-2 rounded-lg" />
                            </div>
                            <div>
                                <label className="block text-xs text-slate-400 mb-1">Password</label>
                                <input type="password" value={connectionConfig.password || ''} onChange={e => updateConfig('password', e.target.value)} className="w-full glass-input p-2 rounded-lg" />
                            </div>
                        </div>
                    </>
                )}
                {/* Add other DB Types similarly if needed, or stick to simple for now. Pasting BigQuery logic. */}
                {dbType === 'bigquery' && (
                    <>
                        <div>
                            <label className="block text-xs text-slate-400 mb-1">Project ID</label>
                            <input type="text" value={connectionConfig.project_id || ''} onChange={e => updateConfig('project_id', e.target.value)} className="w-full glass-input p-2 rounded-lg" />
                        </div>
                        <div>
                            <label className="block text-xs text-slate-400 mb-1">Dataset ID (Default)</label>
                            <input type="text" value={connectionConfig.dataset_id || ''} onChange={e => updateConfig('dataset_id', e.target.value)} className="w-full glass-input p-2 rounded-lg" />
                        </div>
                        <div>
                            <label className="block text-xs text-slate-400 mb-1">Service Account JSON</label>
                            <textarea value={connectionConfig.credentials_json || ''} onChange={e => updateConfig('credentials_json', e.target.value)} className="w-full glass-input p-2 rounded-lg font-mono text-xs" rows={4} />
                        </div>
                    </>
                )
                }

                {
                    dbType === 's3' && (
                        <>
                            <div className="grid grid-cols-2 gap-4">
                                <div>
                                    <label className="block text-xs text-slate-400 mb-1">Access Key</label>
                                    <input type="text" value={connectionConfig.access_key || ''} onChange={e => updateConfig('access_key', e.target.value)} className="w-full glass-input p-2 rounded-lg" />
                                </div>
                                <div>
                                    <label className="block text-xs text-slate-400 mb-1">Secret Key</label>
                                    <input type="password" value={connectionConfig.secret_key || ''} onChange={e => updateConfig('secret_key', e.target.value)} className="w-full glass-input p-2 rounded-lg" />
                                </div>
                            </div>
                            <div>
                                <label className="block text-xs text-slate-400 mb-1">Endpoint (Optional for AWS, Required for MinIO)</label>
                                <input type="text" value={connectionConfig.endpoint || ''} onChange={e => updateConfig('endpoint', e.target.value)} className="w-full glass-input p-2 rounded-lg" placeholder="https://minio.example.com" />
                            </div>
                            <div>
                                <label className="block text-xs text-slate-400 mb-1">Bucket Name</label>
                                <input type="text" value={connectionConfig.bucket || ''} onChange={e => updateConfig('bucket', e.target.value)} className="w-full glass-input p-2 rounded-lg" placeholder="my-data-bucket" />
                            </div>
                        </>
                    )
                }

                {
                    dbType === 'gcs' && (
                        <>
                            <div>
                                <label className="block text-xs text-slate-400 mb-1">Bucket Name (Default)</label>
                                <input type="text" value={connectionConfig.bucket || ''} onChange={e => updateConfig('bucket', e.target.value)} className="w-full glass-input p-2 rounded-lg" placeholder="my-gcs-bucket" />
                            </div>
                            <div>
                                <label className="block text-xs text-slate-400 mb-1">Service Account JSON</label>
                                <textarea value={connectionConfig.credentials_json || ''} onChange={e => updateConfig('credentials_json', e.target.value)} className="w-full glass-input p-2 rounded-lg font-mono text-xs" rows={4} />
                            </div>
                        </>
                    )
                }

                {
                    dbType === 'adls' && (
                        <>
                            <div className="grid grid-cols-2 gap-4">
                                <div>
                                    <label className="block text-xs text-slate-400 mb-1">Storage Account Name</label>
                                    <input type="text" value={connectionConfig.account_name || ''} onChange={e => updateConfig('account_name', e.target.value)} className="w-full glass-input p-2 rounded-lg" />
                                </div>
                                <div>
                                    <label className="block text-xs text-slate-400 mb-1">Account Key</label>
                                    <input type="password" value={connectionConfig.account_key || ''} onChange={e => updateConfig('account_key', e.target.value)} className="w-full glass-input p-2 rounded-lg" />
                                </div>
                            </div>
                            <div>
                                <label className="block text-xs text-slate-400 mb-1">Container Name</label>
                                <input type="text" value={connectionConfig.container || ''} onChange={e => updateConfig('container', e.target.value)} className="w-full glass-input p-2 rounded-lg" />
                            </div>
                        </>
                    )
                }

                {
                    connectionStatus.message && (
                        <div className={`p-3 rounded-xl flex items-center gap-2 ${connectionStatus.success ? 'bg-green-500/20 text-green-300' : 'bg-red-500/20 text-red-300'}`}>
                            {connectionStatus.success ? <Check className="w-4 h-4" /> : <AlertCircle className="w-4 h-4" />}
                            <span className="text-sm">{connectionStatus.message}</span>
                        </div>
                    )
                }

                <div className="flex gap-3 pt-2">
                    <button onClick={() => setStep('select-linked-service')} className="px-4 py-2 bg-white/10 hover:bg-white/20 text-white rounded-lg">Back</button>
                    <button onClick={handleTestLinkedService} disabled={testingConnection} className="flex-1 px-4 py-2 bg-blue-600 hover:bg-blue-500 text-white rounded-lg flex items-center justify-center gap-2">
                        {testingConnection && <Loader2 className="animate-spin w-4 h-4" />} Test
                    </button>
                    <button onClick={handleCreateLinkedService} disabled={!connectionStatus.success || !serviceName} className="flex-1 px-4 py-2 bg-purple-600 hover:bg-purple-500 disabled:opacity-50 text-white rounded-lg">
                        Create Service
                    </button>
                </div>
            </div >
        );
    };

    return createPortal(
        <div className="fixed inset-0 bg-black/60 backdrop-blur-md flex items-center justify-center z-[200] p-4">
            <div className="glass-panel w-full max-w-2xl max-h-[90vh] overflow-y-auto rounded-2xl animate-in zoom-in-95">
                {/* Header */}
                {/* Header */}
                <div className="sticky top-0 glass-panel border-b border-slate-200 dark:border-white/10 p-6 flex items-center justify-between z-10">
                    <div className="flex items-center gap-3">
                        <Database className="w-6 h-6 text-indigo-500 dark:text-indigo-400" />
                        <h2 className="text-xl font-bold text-slate-900 dark:text-white">
                            {step === 'select-dataset' && 'Select Dataset'}
                            {step === 'select-linked-service' && 'Choose Linked Service'}
                            {step === 'create-linked-service' && 'New Linked Service'}
                            {step === 'create-dataset' && 'Configure Dataset'}
                            {step === 'columns' && 'Select Columns'}
                        </h2>
                    </div>
                    <button onClick={onClose} className="p-2 hover:bg-slate-100 dark:hover:bg-white/10 rounded-lg text-slate-500 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white"><X className="w-5 h-5" /></button>
                </div>

                <div className="p-6">
                    {step === 'select-linked-service' && (
                        <div className="space-y-4">
                            <div className="space-y-2">
                                {linkedServices.length === 0 && <div className="text-center text-slate-400 py-8">No linked services found. Create one to get started.</div>}
                                {linkedServices.map(service => (
                                    <div key={service.id} className="flex items-center gap-3 p-4 glass-panel hover:bg-slate-100 dark:hover:bg-white/10 rounded-xl cursor-pointer" onClick={() => { setSelectedLinkedServiceId(service.id); setStep('select-dataset'); }}>
                                        <div className="flex-1">
                                            <div className="font-medium text-slate-900 dark:text-white">{service.name}</div>
                                            <div className="text-sm text-slate-500 dark:text-slate-400 capitalize">{service.service_type.replace('_', ' ')}</div>
                                        </div>
                                    </div>
                                ))}
                            </div>
                            <button onClick={() => { setConnectionConfig({}); setServiceName(''); setStep('create-linked-service'); }} className="w-full p-4 border-2 border-dashed border-slate-300 dark:border-white/20 hover:border-indigo-500/50 rounded-xl text-slate-500 dark:text-slate-300 hover:text-slate-900 dark:hover:text-white transition-colors flex flex-col items-center justify-center gap-1">
                                <span className="font-medium">+ Create New Service</span>
                            </button>
                        </div>
                    )}

                    {step === 'select-dataset' && selectedLinkedServiceId && (
                        <div className="space-y-4">
                            <div className="flex items-center gap-2 pb-2 border-b border-slate-200 dark:border-white/10">
                                <span className="text-sm text-slate-500 dark:text-slate-400">Context: </span>
                                <span className="text-sm font-medium text-indigo-600 dark:text-indigo-400">
                                    {linkedServices.find(s => s.id === selectedLinkedServiceId)?.name || 'Unknown Service'}
                                </span>
                            </div>

                            <div className="space-y-2">
                                {dataSources.filter(ds => ds.linked_service_id === selectedLinkedServiceId).length === 0 && (
                                    <div className="text-center text-slate-500 dark:text-slate-400 py-8">No datasets configured for this service.</div>
                                )}
                                {dataSources
                                    .filter(ds => ds.linked_service_id === selectedLinkedServiceId)
                                    .map(source => (
                                        <div key={source.id} className="group flex items-center gap-3 p-4 glass-panel hover:bg-slate-100 dark:hover:bg-white/10 rounded-xl transition-colors cursor-pointer" onClick={() => handleSelectExistingDataset(source.id)}>
                                            <div className="flex-1">
                                                <div className="font-medium text-slate-900 dark:text-white">{source.name}</div>
                                                <div className="text-sm text-slate-500 dark:text-slate-400">{source.table_name}</div>
                                            </div>
                                            <button onClick={(e) => handleDeleteSource(e, source.id)} className="opacity-0 group-hover:opacity-100 p-2 hover:bg-red-500/20 text-red-400 rounded-lg"><Trash2 className="w-4 h-4" /></button>
                                        </div>
                                    ))}
                            </div>

                            <div className="flex gap-3 pt-2">
                                <button onClick={() => setStep('select-linked-service')} className="px-4 py-3 bg-slate-100 dark:bg-white/10 hover:bg-slate-200 dark:hover:bg-white/20 text-slate-900 dark:text-white rounded-xl">Back</button>
                                <button onClick={() => setStep('create-dataset')} className="flex-1 p-3 bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl">
                                    + Connect New Table
                                </button>
                            </div>
                        </div>
                    )
                    }

                    {step === 'create-linked-service' && renderLinkedServiceForm()}

                    {
                        step === 'create-dataset' && (
                            <div className="space-y-4">
                                <div>
                                    <label className="block text-sm font-medium text-slate-700 dark:text-slate-200 mb-2">Dataset Display Name</label>
                                    <input type="text" value={datasetName} onChange={e => setDatasetName(e.target.value)} className="w-full glass-input p-3 rounded-xl outline-none" placeholder="Users Table" />
                                </div>
                                <div>
                                    <label className="block text-sm font-medium text-slate-700 dark:text-slate-200 mb-2">
                                        {['s3', 'gcs', 'adls', 'minio'].includes(
                                            linkedServices.find(s => s.id === selectedLinkedServiceId)?.service_type || ''
                                        ) ? "File Path (e.g. data/users.csv or folder/)" : "Source Table Name"}
                                    </label>
                                    <input type="text" value={tableName} onChange={e => setTableName(e.target.value)} className="w-full glass-input p-3 rounded-xl outline-none" placeholder={
                                        ['s3', 'gcs', 'adls', 'minio'].includes(
                                            linkedServices.find(s => s.id === selectedLinkedServiceId)?.service_type || ''
                                        ) ? "folder/file.parquet" : "users"
                                    } />
                                </div>
                                <div className="flex gap-3 pt-4">
                                    <button onClick={() => setStep('select-linked-service')} className="px-4 py-3 bg-slate-100 dark:bg-white/10 hover:bg-slate-200 dark:hover:bg-white/20 text-slate-900 dark:text-white rounded-xl">Back</button>
                                    <button onClick={handleCreateDataset} disabled={!datasetName || !tableName} className="flex-1 px-4 py-3 bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 text-white rounded-xl">
                                        Save Dataset
                                    </button>
                                </div>
                            </div>
                        )
                    }

                    {
                        step === 'columns' && (
                            <div className="space-y-4">
                                {loadingSchema ? (
                                    <div className="flex justify-center py-12"><Loader2 className="animate-spin w-8 h-8 text-indigo-400" /></div>
                                ) : (
                                    <>
                                        <div className="flex gap-2 mb-2">
                                            <button onClick={() => setSelectedColumns(columns.map(c => c.name))} className="px-3 py-1 bg-slate-100 dark:bg-white/10 hover:bg-slate-200 dark:hover:bg-white/20 rounded-lg text-xs text-slate-600 dark:text-slate-300">Select All</button>
                                            <button onClick={() => setSelectedColumns([])} className="px-3 py-1 bg-slate-100 dark:bg-white/10 hover:bg-slate-200 dark:hover:bg-white/20 rounded-lg text-xs text-slate-600 dark:text-slate-300">Deselect All</button>
                                        </div>
                                        <div className="space-y-2 max-h-80 overflow-y-auto">
                                            {columns.map(col => (
                                                <label key={col.name} className="flex items-center gap-3 p-3 glass-panel hover:bg-slate-100 dark:hover:bg-white/10 rounded-lg cursor-pointer">
                                                    <input type="checkbox" checked={selectedColumns.includes(col.name)} onChange={() => toggleColumn(col.name)} className="w-4 h-4 accent-indigo-500" />
                                                    <div>
                                                        <div className="font-medium text-slate-900 dark:text-white">{col.name}</div>
                                                        <div className="text-xs text-slate-500 dark:text-slate-400">{col.type}</div>
                                                    </div>
                                                </label>
                                            ))}
                                        </div>
                                        <div className="flex gap-3 mt-4">
                                            <button onClick={() => setStep('select-dataset')} className="px-4 py-3 bg-slate-100 dark:bg-white/10 hover:bg-slate-200 dark:hover:bg-white/20 text-slate-900 dark:text-white rounded-xl">Back</button>
                                            <button onClick={handleSaveConfig} disabled={selectedColumns.length === 0} className="flex-1 px-4 py-3 bg-indigo-600 text-white rounded-xl">Save Configuration</button>
                                        </div>
                                    </>
                                )}
                            </div>
                        )
                    }
                </div>
            </div>
        </div >,
        document.body
    );
};
