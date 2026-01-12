import React, { useState, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { X, Database, Save } from 'lucide-react';
import { etlApi } from '../../services/api';

interface ETLDataSource {
    id: number;
    name: string;
    linked_service?: {
        service_type: string;
    };
    table_name: string;
}

interface SinkConfigModalProps {
    isOpen: boolean;
    onClose: () => void;
    onSave: (config: {
        datasourceId: number;
        tableName: string;
        writeMode: 'append' | 'overwrite' | 'error';
    }) => void;
    existingConfig?: {
        datasourceId?: number;
        tableName?: string;
        writeMode?: 'append' | 'overwrite' | 'error';
    };
}

const WRITE_MODES = [
    { value: 'append', label: 'Append (Add to existing data)' },
    { value: 'overwrite', label: 'Overwrite (Replace table)' },
    { value: 'error', label: 'Error if exists (Fail if table exists)' }
];

export const SinkConfigModal: React.FC<SinkConfigModalProps> = ({
    isOpen,
    onClose,
    onSave,
    existingConfig,
}) => {
    const [linkedServices, setLinkedServices] = useState<any[]>([]);
    const [dataSources, setDataSources] = useState<ETLDataSource[]>([]);

    // State
    const [selectedServiceId, setSelectedServiceId] = useState<number | null>(null);
    const [tableName, setTableName] = useState(existingConfig?.tableName || '');
    const [writeMode, setWriteMode] = useState<'append' | 'overwrite' | 'error'>(
        existingConfig?.writeMode || 'append'
    );
    const [isLoading, setIsLoading] = useState(false);

    useEffect(() => {
        if (isOpen) {
            fetchInitialData();
            setTableName(existingConfig?.tableName || '');
            setWriteMode(existingConfig?.writeMode || 'append');
        }
    }, [isOpen, existingConfig]);

    const fetchInitialData = async () => {
        try {
            const [services, sources] = await Promise.all([
                etlApi.linkedServices.list(),
                etlApi.getDataSources()
            ]);
            setLinkedServices(services);
            setDataSources(sources);

            // If editing, try to find the linked service ID from the datasource ID
            if (existingConfig?.datasourceId) {
                const source = sources.find((s: any) => s.id === existingConfig.datasourceId);
                if (source && source.linked_service_id) {
                    setSelectedServiceId(source.linked_service_id);
                }
            }
        } catch (error) {
            console.error('Failed to fetch initial data:', error);
        }
    };

    const handleSave = async () => {
        if (!selectedServiceId || !tableName) {
            alert('Please select a linked service and table name');
            return;
        }

        setIsLoading(true);
        try {
            let targetDataSourceId: number;

            // 1. Check if a datasource already exists for this service + table
            const existingSource = dataSources.find(ds =>
                // We need to cast or ensure types match, depending on API response structure
                (ds as any).linked_service_id === selectedServiceId &&
                ds.table_name === tableName
            );

            if (existingSource) {
                targetDataSourceId = existingSource.id;
            } else {
                // 2. Create new datasource
                const selectedService = linkedServices.find(s => s.id === selectedServiceId);
                const serviceName = selectedService?.name || 'Unknown';

                const newSource = await etlApi.createDataSource({
                    name: `Sink - ${serviceName} - ${tableName}`,
                    linked_service_id: selectedServiceId,
                    table_name: tableName,
                    description: 'Auto-generated sink datasource'
                });
                targetDataSourceId = newSource.id;
            }

            onSave({
                datasourceId: targetDataSourceId,
                tableName,
                writeMode
            });
            onClose();
        } catch (error) {
            console.error("Failed to save sink configuration:", error);
            alert("Failed to save configuration. See console for details.");
        } finally {
            setIsLoading(false);
        }
    };

    if (!isOpen) return null;

    return createPortal(
        <div className="fixed inset-0 bg-black/60 backdrop-blur-md flex items-center justify-center z-[200] p-4">
            <div className="glass-panel w-full max-w-lg rounded-2xl animate-in zoom-in-95">
                {/* Header */}
                <div className="border-b border-white/10 p-6 flex items-center justify-between">
                    <div className="flex items-center gap-3">
                        <Database className="w-6 h-6 text-pink-400" />
                        <h2 className="text-xl font-bold text-white">Configure Sink (Destination)</h2>
                    </div>
                    <button
                        onClick={onClose}
                        className="p-2 hover:bg-white/10 rounded-lg transition-colors text-slate-400 hover:text-white"
                    >
                        <X className="w-5 h-5" />
                    </button>
                </div>

                {/* Content */}
                <div className="p-6 space-y-6">
                    {/* Linked Service Selection */}
                    <div>
                        <label className="block text-sm font-medium text-slate-200 mb-2">
                            Destination Service
                        </label>
                        <select
                            value={selectedServiceId || ''}
                            onChange={(e) => setSelectedServiceId(Number(e.target.value))}
                            className="w-full glass-input p-3 rounded-xl outline-none"
                        >
                            <option value="">Select a service...</option>
                            {linkedServices.map(service => (
                                <option key={service.id} value={service.id}>
                                    {service.name} ({service.service_type})
                                </option>
                            ))}
                        </select>
                        <p className="text-xs text-slate-400 mt-2">
                            Select the database service where you want to write data.
                        </p>
                    </div>

                    {/* Table Name */}
                    <div>
                        <label className="block text-sm font-medium text-slate-200 mb-2">
                            Destination Table Name
                        </label>
                        <input
                            type="text"
                            value={tableName}
                            onChange={(e) => setTableName(e.target.value)}
                            className="w-full glass-input p-3 rounded-xl outline-none"
                            placeholder="target_table_name"
                        />
                        <p className="text-xs text-slate-400 mt-2">
                            The table will be created if it doesn't exist (depending on write mode).
                        </p>
                    </div>

                    {/* Write Mode */}
                    <div>
                        <label className="block text-sm font-medium text-slate-200 mb-2">
                            Write Mode
                        </label>
                        <div className="space-y-2">
                            {WRITE_MODES.map(mode => (
                                <label
                                    key={mode.value}
                                    className={`flex items-center gap-3 p-3 glass-panel rounded-lg cursor-pointer transition-colors ${writeMode === mode.value ? 'bg-pink-500/20 border-pink-500/50' : 'hover:bg-white/10'
                                        }`}
                                >
                                    <input
                                        type="radio"
                                        name="writeMode"
                                        value={mode.value}
                                        checked={writeMode === mode.value}
                                        onChange={(e) => setWriteMode(e.target.value as any)}
                                        className="w-4 h-4 accent-pink-500"
                                    />
                                    <div>
                                        <div className="font-medium text-white">{mode.label}</div>
                                    </div>
                                </label>
                            ))}
                        </div>
                    </div>

                    {/* Save Button */}
                    <button
                        onClick={handleSave}
                        disabled={isLoading}
                        className="w-full flex items-center justify-center gap-2 px-6 py-4 bg-pink-600 hover:bg-pink-500 text-white rounded-xl transition-colors font-medium disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                        {isLoading ? (
                            <div className="w-5 h-5 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                        ) : (
                            <Save className="w-5 h-5" />
                        )}
                        {isLoading ? 'Saving...' : 'Save Destination'}
                    </button>
                </div>
            </div>
        </div>,
        document.body
    );
};
