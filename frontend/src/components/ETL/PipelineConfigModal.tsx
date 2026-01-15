import React, { useState, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { X, Workflow } from 'lucide-react';
import { etlApi } from '../../services/api';

interface PipelineConfigModalProps {
    isOpen: boolean;
    onClose: () => void;
    onSave: (config: {
        pipelineId: number;
        pipelineName: string;
    }) => void;
    existingConfig?: {
        pipelineId?: number;
    };
    currentPipelineId?: number | null; // To prevent selecting self
}

export const PipelineConfigModal: React.FC<PipelineConfigModalProps> = ({
    isOpen,
    onClose,
    onSave,
    existingConfig,
    currentPipelineId
}) => {
    const [pipelines, setPipelines] = useState<any[]>([]);
    const [selectedPipelineId, setSelectedPipelineId] = useState<number | null>(existingConfig?.pipelineId || null);
    const [isLoading, setIsLoading] = useState(false);

    useEffect(() => {
        if (isOpen) {
            fetchPipelines();
        }
    }, [isOpen]);

    const fetchPipelines = async () => {
        setIsLoading(true);
        try {
            const data = await etlApi.getPipelines();
            // Filter out current pipeline to avoid recursion
            const validPipelines = data.filter((p: any) => p.id !== currentPipelineId);
            setPipelines(validPipelines);
        } catch (error) {
            console.error('Failed to fetch pipelines:', error);
        } finally {
            setIsLoading(false);
        }
    };

    const handleSave = () => {
        if (!selectedPipelineId) return;
        const selected = pipelines.find(p => p.id === selectedPipelineId);
        if (selected) {
            onSave({
                pipelineId: selected.id,
                pipelineName: selected.name
            });
            onClose();
        }
    };

    if (!isOpen) return null;

    return createPortal(
        <div className="fixed inset-0 bg-black/60 backdrop-blur-md flex items-center justify-center z-[200] p-4">
            <div className="glass-panel w-full max-w-lg rounded-2xl animate-in zoom-in-95">
                {/* Header */}
                <div className="border-b border-slate-200 dark:border-white/10 p-6 flex items-center justify-between">
                    <div className="flex items-center gap-3">
                        <Workflow className="w-6 h-6 text-orange-500 dark:text-orange-400" />
                        <h2 className="text-xl font-bold text-slate-900 dark:text-white">Select Pipeline Component</h2>
                    </div>
                    <button
                        onClick={onClose}
                        className="p-2 hover:bg-slate-100 dark:hover:bg-white/10 rounded-lg transition-colors text-slate-500 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white"
                    >
                        <X className="w-5 h-5" />
                    </button>
                </div>

                <div className="p-6 space-y-6">
                    <div>
                        <label className="block text-sm font-medium text-slate-700 dark:text-slate-200 mb-2">
                            Pipeline to Reuse
                        </label>
                        <select
                            value={selectedPipelineId || ''}
                            onChange={(e) => setSelectedPipelineId(Number(e.target.value))}
                            className="w-full glass-input p-3 rounded-xl outline-none text-slate-900 dark:text-white"
                            disabled={isLoading}
                        >
                            <option value="">Select a pipeline...</option>
                            {pipelines.map(pipeline => (
                                <option key={pipeline.id} value={pipeline.id} className="text-slate-900">
                                    {pipeline.name}
                                </option>
                            ))}
                        </select>
                        {isLoading && <p className="text-xs text-slate-500 mt-1">Loading pipelines...</p>}
                        <p className="text-xs text-slate-500 dark:text-slate-400 mt-2">
                            The selected pipeline will receive input from this node's upstream connection and execute its logic.
                        </p>
                    </div>

                    <div className="flex gap-3 pt-2">
                        <button
                            onClick={onClose}
                            className="px-4 py-3 bg-slate-100 dark:bg-white/10 hover:bg-slate-200 dark:hover:bg-white/20 text-slate-900 dark:text-white rounded-xl transition-colors"
                        >
                            Cancel
                        </button>
                        <button
                            onClick={handleSave}
                            disabled={!selectedPipelineId}
                            className="flex-1 px-4 py-3 bg-orange-600 hover:bg-orange-500 disabled:opacity-50 text-white rounded-xl transition-colors font-medium"
                        >
                            Select Pipeline
                        </button>
                    </div>
                </div>
            </div>
        </div>,
        document.body
    );
};
