import React, { useState } from 'react';
import { createPortal } from 'react-dom';
import { X, Wand2, Code, Table, Loader2, ArrowLeft, Check } from 'lucide-react';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism';
import { etlApi } from '../../services/api';

interface TransformConfigModalProps {
    isOpen: boolean;
    onClose: () => void;
    onSave: (config: {
        prompt: string;
        generatedCode: string;
    }) => void;
    upstreamNodes?: Array<{
        id: string;
        label: string;
        datasourceId?: number;
        selectedColumns?: string[];
        tableName?: string;
    }>;
    existingConfig?: {
        prompt?: string;
        generatedCode?: string;
    };
    selectedModel?: string;
}

type Step = 'prompt' | 'preview';

const EXAMPLE_PROMPTS = [
    "Filter rows where age > 25",
    "Join Table A and Table B on user_id",
    "Group by department and calculate average salary",
    "Remove rows with null values in the email column",
    "Convert all text columns to uppercase",
];

export const TransformConfigModal: React.FC<TransformConfigModalProps> = ({
    isOpen,
    onClose,
    onSave,
    upstreamNodes,
    existingConfig,
    selectedModel,
}) => {
    const [step, setStep] = useState<Step>('prompt');
    const [prompt, setPrompt] = useState(existingConfig?.prompt || '');
    const [generatingPreview, setGeneratingPreview] = useState(false);
    const [previewTab, setPreviewTab] = useState<'data' | 'code'>('data');

    React.useEffect(() => {
        if (isOpen) {
            setPrompt(existingConfig?.prompt || '');
            setPreviewData(null);
            setStep('prompt');
        }
    }, [isOpen, existingConfig?.prompt]);

    // Preview data
    const [previewData, setPreviewData] = useState<{
        columns: string[];
        data: any[][];
        row_count: number;
        generated_code: string;
    } | null>(null);

    const handleGeneratePreview = async () => {
        if (!upstreamNodes || upstreamNodes.length === 0) {
            alert('At least one source node must be connected');
            return;
        }

        const validSources = upstreamNodes.filter(n => n.datasourceId && n.selectedColumns && n.tableName);
        if (validSources.length === 0) {
            alert('Connected source nodes must be configured first');
            return;
        }

        if (!prompt.trim()) {
            alert('Please enter a transformation description');
            return;
        }

        if (!selectedModel) {
            alert('Please select an AI model from the top bar to generate the transformation.');
            return;
        }

        setGeneratingPreview(true);
        try {
            const sourcesPayload = validSources.map(s => ({
                datasource_id: s.datasourceId!,
                selected_columns: s.selectedColumns!,
                table_name: s.tableName!
            }));

            const result = await etlApi.previewTransformation({
                sources: sourcesPayload,
                transformation_prompt: prompt,
                limit: 1000,
                model_name: selectedModel,
            });

            console.log('DEBUG Preview Result:', result); // Debug log
            setPreviewData(result);
            setStep('preview');
            setPreviewTab('data'); // Default to data view on new generation
        } catch (error: any) {
            console.error('Failed to generate preview:', error);
            alert(`Failed to generate preview: ${error.response?.data?.detail || error.message}`);
        } finally {
            setGeneratingPreview(false);
        }
    };

    const handleApprove = () => {
        if (previewData) {
            onSave({
                prompt,
                generatedCode: previewData.generated_code,
            });
            onClose();
        }
    };

    const handleBack = () => {
        setStep('prompt');
    };

    if (!isOpen) return null;

    return createPortal(
        <div className="fixed inset-0 bg-black/60 backdrop-blur-md flex items-center justify-center z-[200] p-4">
            <div className="glass-panel w-full max-w-5xl max-h-[90vh] overflow-y-auto rounded-2xl animate-in zoom-in-95">
                {/* Header */}
                <div className="sticky top-0 glass-panel border-b border-white/10 p-6 flex items-center justify-between">
                    <div className="flex items-center gap-3">
                        <Wand2 className="w-6 h-6 text-purple-400" />
                        <h2 className="text-xl font-bold text-white">
                            {step === 'prompt' && 'Configure Transformation'}
                            {step === 'preview' && 'Preview Results'}
                        </h2>
                    </div>
                    <button
                        onClick={onClose}
                        className="p-2 hover:bg-white/10 rounded-lg transition-colors text-slate-400 hover:text-white"
                    >
                        <X className="w-5 h-5" />
                    </button>
                </div>

                {/* Content */}
                <div className="p-6">
                    {step === 'prompt' && (
                        <div className="space-y-6">
                            {/* Source Info */}
                            <div className="space-y-2">
                                <div className="text-sm font-medium text-slate-300">Available Source Tables:</div>
                                {upstreamNodes && upstreamNodes.length > 0 ? (
                                    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                                        {upstreamNodes.map((node, idx) => (
                                            <div key={idx} className="p-4 bg-indigo-500/10 border border-indigo-500/30 rounded-xl relative overflow-hidden group">
                                                <div className="absolute top-0 right-0 p-2 opacity-50 text-[10px] text-indigo-400 font-mono">
                                                    {node.label}
                                                </div>
                                                <div className="text-sm text-slate-300 mb-1">Table Name</div>
                                                <div className="font-medium text-white truncate" title={node.tableName || 'Unconfigured'}>
                                                    {node.tableName || 'Unconfigured'}
                                                </div>
                                                <div className="text-xs text-slate-400 mt-2">
                                                    {node.selectedColumns?.length || 0} columns selected
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                ) : (
                                    <div className="p-4 border border-dashed border-slate-600 rounded-xl text-center text-slate-500 text-sm">
                                        No source nodes connected yet.
                                    </div>
                                )}
                            </div>

                            {/* Transformation Prompt */}
                            <div>
                                <label className="block text-sm font-medium text-slate-200 mb-2">
                                    Describe your transformation
                                </label>
                                <textarea
                                    value={prompt}
                                    onChange={(e) => setPrompt(e.target.value)}
                                    className="w-full glass-input p-4 rounded-xl outline-none font-mono text-sm min-h-[150px]"
                                    placeholder="Example: Filter rows where age > 25 and add a new column 'category' based on salary ranges"
                                />
                            </div>

                            {/* Example Prompts */}
                            <div>
                                <div className="text-sm font-medium text-slate-300 mb-2">Examples:</div>
                                <div className="flex flex-wrap gap-2">
                                    {EXAMPLE_PROMPTS.map((example, idx) => (
                                        <button
                                            key={idx}
                                            onClick={() => setPrompt(example)}
                                            className="px-3 py-1.5 text-xs bg-white/5 hover:bg-white/10 border border-white/10 rounded-lg transition-colors text-slate-300 hover:text-white"
                                        >
                                            {example}
                                        </button>
                                    ))}
                                </div>
                            </div>

                            {/* Generate Button */}
                            <button
                                onClick={handleGeneratePreview}
                                disabled={generatingPreview || !prompt.trim()}
                                className="w-full flex items-center justify-center gap-2 px-6 py-4 bg-purple-600 hover:bg-purple-500 disabled:bg-purple-600/50 text-white rounded-xl transition-colors font-medium"
                            >
                                {generatingPreview ? (
                                    <>
                                        <Loader2 className="w-5 h-5 animate-spin" />
                                        Generating Preview...
                                    </>
                                ) : (
                                    <>
                                        <Wand2 className="w-5 h-5" />
                                        Generate Preview
                                    </>
                                )}
                            </button>
                        </div>
                    )}

                    {step === 'preview' && previewData && (
                        <div className="space-y-6">
                            <div className="flex items-center justify-between">
                                <button
                                    onClick={handleBack}
                                    className="flex items-center gap-2 px-4 py-2 text-sm bg-white/5 hover:bg-white/10 rounded-lg transition-colors text-slate-300 hover:text-white"
                                >
                                    <ArrowLeft className="w-4 h-4" />
                                    Edit Prompt
                                </button>

                                {/* Tabs */}
                                <div className="flex bg-white/5 p-1 rounded-lg">
                                    <button
                                        onClick={() => setPreviewTab('data')}
                                        className={`px-4 py-1.5 text-sm font-medium rounded-md transition-all ${previewTab === 'data'
                                            ? 'bg-purple-500 text-white shadow-lg'
                                            : 'text-slate-400 hover:text-white'
                                            }`}
                                    >
                                        <div className="flex items-center gap-2">
                                            <Table className="w-4 h-4" />
                                            Data Preview
                                        </div>
                                    </button>
                                    <button
                                        onClick={() => setPreviewTab('code')}
                                        className={`px-4 py-1.5 text-sm font-medium rounded-md transition-all ${previewTab === 'code'
                                            ? 'bg-purple-500 text-white shadow-lg'
                                            : 'text-slate-400 hover:text-white'
                                            }`}
                                    >
                                        <div className="flex items-center gap-2">
                                            <Code className="w-4 h-4" />
                                            PySpark Code
                                        </div>
                                    </button>
                                </div>
                            </div>

                            {/* Data Preview Tab */}
                            {previewTab === 'data' && (
                                <div className="animate-in fade-in slide-in-from-bottom-2 duration-300">
                                    <div className="flex items-center gap-2 mb-3">
                                        <Table className="w-5 h-5 text-blue-400" />
                                        <h3 className="text-lg font-semibold text-white">
                                            Data Preview ({previewData.row_count} rows)
                                        </h3>
                                    </div>
                                    <div className="rounded-xl overflow-hidden border border-white/10">
                                        <div className="overflow-x-auto max-h-[500px]">
                                            <table className="w-full text-sm">
                                                <thead className="sticky top-0 bg-slate-800 border-b border-white/10 text-xs uppercase tracking-wider">
                                                    <tr>
                                                        {previewData.columns.map((col, idx) => (
                                                            <th
                                                                key={idx}
                                                                className="px-4 py-3 text-left font-medium text-slate-300 whitespace-nowrap bg-slate-800"
                                                            >
                                                                {col}
                                                            </th>
                                                        ))}
                                                    </tr>
                                                </thead>
                                                <tbody className="divide-y divide-white/5 bg-slate-900/30">
                                                    {previewData.data.map((row, rowIdx) => (
                                                        <tr key={rowIdx} className="hover:bg-white/5 transition-colors">
                                                            {row.map((cell, cellIdx) => (
                                                                <td
                                                                    key={cellIdx}
                                                                    className="px-4 py-3 text-slate-300 whitespace-nowrap"
                                                                >
                                                                    {cell === null ? (
                                                                        <span className="text-slate-500 italic">null</span>
                                                                    ) : (
                                                                        String(cell)
                                                                    )}
                                                                </td>
                                                            ))}
                                                        </tr>
                                                    ))}
                                                </tbody>
                                            </table>
                                        </div>
                                    </div>
                                </div>
                            )}

                            {/* Code Tab */}
                            {previewTab === 'code' && (
                                <div className="animate-in fade-in slide-in-from-bottom-2 duration-300">
                                    <div className="flex items-center gap-2 mb-3">
                                        <Code className="w-5 h-5 text-green-400" />
                                        <h3 className="text-lg font-semibold text-white">Generated PySpark Code</h3>
                                    </div>
                                    <div className="rounded-xl overflow-hidden border border-white/10 bg-[#1e1e1e]">
                                        <SyntaxHighlighter
                                            language="python"
                                            style={vscDarkPlus as any}
                                            customStyle={{
                                                margin: 0,
                                                padding: '1.5rem',
                                                fontSize: '0.9rem',
                                                lineHeight: '1.5',
                                                maxHeight: '500px',
                                            }}
                                            showLineNumbers={true}
                                        >
                                            {previewData.generated_code}
                                        </SyntaxHighlighter>
                                    </div>
                                </div>
                            )}

                            {/* Approve Button */}
                            <button
                                onClick={handleApprove}
                                className="w-full flex items-center justify-center gap-2 px-6 py-4 bg-green-600 hover:bg-green-500 text-white rounded-xl transition-colors font-medium"
                            >
                                <Check className="w-5 h-5" />
                                Approve & Save Transformation
                            </button>
                        </div>
                    )}
                </div>
            </div>
        </div>,
        document.body
    );
};
