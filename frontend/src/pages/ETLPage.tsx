import React, { useRef, useEffect } from 'react';
import {
    ReactFlow,
    Controls,
    Background,
    useNodesState,
    useEdgesState,
    addEdge,
    type Connection,
    type Node,
    type Edge,
    ReactFlowProvider,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { SourceNode, TransformNode, SinkNode } from '../components/ETL/ETLNodes';
import { SourceConfigModal } from '../components/ETL/SourceConfigModal';
import { TransformConfigModal } from '../components/ETL/TransformConfigModal';
import { SinkConfigModal } from '../components/ETL/SinkConfigModal';
import { Play, Database, Wand2, FileOutput, ArrowLeft, Bot, Loader2, Save, Trash2, FileText, History } from 'lucide-react';
// ... (imports)

// ... (inside component)

import { createPortal } from 'react-dom';
import { useNavigate } from 'react-router-dom';
import { settingsApi, etlApi } from '../services/api';
import { useQuery } from '@tanstack/react-query';

const nodeTypes = {
    source: SourceNode,
    transform: TransformNode,
    sink: SinkNode,
};

type NodeData = {
    label: string;
    sourceType?: string;
    prompt?: string;
    onEdit?: () => void;
    datasourceId?: number;
    tableName?: string;
    selectedColumns?: string[];
    generatedCode?: string;
    writeMode?: string;
    onDelete?: () => void;
};

// Define a custom Node type that extends the basic Node structure from React Flow but with our data
type ETLNode = Node<NodeData>;

const initialNodes: ETLNode[] = [];

const initialEdges: Edge[] = [];

export default function ETLPage() {
    const navigate = useNavigate();
    // Explicitly type the state
    const [nodes, setNodes, onNodesChange] = useNodesState<ETLNode>(initialNodes);
    const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);
    const reactFlowWrapper = useRef<HTMLDivElement>(null);

    // Source configuration modal state
    const [showSourceModal, setShowSourceModal] = React.useState(false);
    const [activeSourceNodeId, setActiveSourceNodeId] = React.useState<string | null>(null);

    // Transform configuration modal state
    const [showTransformConfigModal, setShowTransformConfigModal] = React.useState(false);
    const [activeTransformNodeId, setActiveTransformNodeId] = React.useState<string | null>(null);

    // Sink configuration modal state
    const [showSinkConfigModal, setShowSinkConfigModal] = React.useState(false);
    const [activeSinkNodeId, setActiveSinkNodeId] = React.useState<string | null>(null);

    // Model selection state
    const [selectedModel, setSelectedModel] = React.useState<string>('');

    // Pipeline State
    const [pipelines, setPipelines] = React.useState<any[]>([]);
    const [currentPipelineId, setCurrentPipelineId] = React.useState<number | null>(null);
    const [pipelineName, setPipelineName] = React.useState('Untitled Pipeline');
    const [isSaving, setIsSaving] = React.useState(false);

    // History State
    const [showHistoryModal, setShowHistoryModal] = React.useState(false);
    const [executions, setExecutions] = React.useState<any[]>([]);
    const [selectedExecution, setSelectedExecution] = React.useState<any | null>(null);


    const handleShowHistory = async () => {
        if (!currentPipelineId) {
            alert("Save pipeline first to see history");
            return;
        }
        setShowHistoryModal(true);
        try {
            const data = await etlApi.getExecutions(currentPipelineId);
            setExecutions(data);
        } catch (error) {
            console.error("Failed to fetch history:", error);
        }
    };

    // Fetch models
    const { data: models } = useQuery({
        queryKey: ['models'],
        queryFn: settingsApi.getModels,
    });

    const fetchPipelines = async () => {
        try {
            const data = await etlApi.getPipelines();
            setPipelines(data);
        } catch (error) {
            console.error("Failed to fetch pipelines:", error);
        }
    };

    React.useEffect(() => {
        fetchPipelines();
    }, []);

    // Set default model when models are loaded
    useEffect(() => {
        if (models && models.length > 0 && !selectedModel) {
            // Prefer gpt-4o if available, otherwise first model
            const gpt4 = models.find((m: any) => m.name.includes('gpt-4'));
            setSelectedModel(gpt4 ? gpt4.name : models[0].name);
        }
    }, [models]);

    const onConnect = React.useCallback(
        (params: Connection) => setEdges((eds) => addEdge(params, eds)),
        [setEdges],
    );

    const onDragOver = React.useCallback((event: React.DragEvent) => {
        event.preventDefault();
        event.dataTransfer.dropEffect = 'move';
    }, []);

    // Generic delete handler
    const handleDeleteNode = React.useCallback((nodeId: string) => {
        setNodes((nds) => nds.filter((node) => node.id !== nodeId));
        setEdges((eds) => eds.filter((edge) => edge.source !== nodeId && edge.target !== nodeId));
    }, [setNodes, setEdges]);

    const onDrop = React.useCallback(
        (event: React.DragEvent) => {
            event.preventDefault();

            const type = event.dataTransfer.getData('application/reactflow');
            if (!type) return;

            const position = {
                x: event.clientX - 300,
                y: event.clientY - 100,
            };

            const nodeId = `${type}-${Date.now()}`;

            // Create new node with generic data structure fitting the type
            const newNode: ETLNode = {
                id: nodeId,
                type,
                position,
                data: {
                    label: type === 'source' ? 'New Source' : type === 'sink' ? 'New Sink' : 'AI Transform',
                    sourceType: type === 'source' ? 'SQL' : undefined,
                    prompt: type === 'transform' ? '' : undefined,
                    onEdit: undefined, // Will be handled by click
                    onDelete: () => handleDeleteNode(nodeId),
                },
            };

            setNodes((nds) => nds.concat(newNode));
        },
        [setNodes, handleDeleteNode],
    );

    // Global click handler
    const onNodeClick = (_: React.MouseEvent, node: ETLNode) => {
        if (node.type === 'transform') {
            setActiveTransformNodeId(node.id);
            setShowTransformConfigModal(true);
        } else if (node.type === 'source') {
            setActiveSourceNodeId(node.id);
            setShowSourceModal(true);
        } else if (node.type === 'sink') {
            setActiveSinkNodeId(node.id);
            setShowSinkConfigModal(true);
        }
    };

    const handleSaveTransformConfig = (config: {
        prompt: string;
        generatedCode: string;
    }) => {
        if (activeTransformNodeId) {
            setNodes((nds) => nds.map((node) => {
                if (node.id === activeTransformNodeId) {
                    return {
                        ...node,
                        data: {
                            ...node.data,
                            prompt: config.prompt,
                            generatedCode: config.generatedCode,
                            label: config.prompt.substring(0, 30) + (config.prompt.length > 30 ? '...' : ''),
                        }
                    };
                }
                return node;
            }));
            setActiveTransformNodeId(null);
        }
    };

    const handleSaveSourceConfig = (config: {
        datasourceId?: number;
        tableName?: string;
        selectedColumns?: string[];
    }) => {
        if (activeSourceNodeId) {
            setNodes((nds) => nds.map((node) => {
                if (node.id === activeSourceNodeId) {
                    return {
                        ...node,
                        data: {
                            ...node.data,
                            datasourceId: config.datasourceId,
                            tableName: config.tableName,
                            selectedColumns: config.selectedColumns,
                            label: config.tableName || node.data.label,
                        }
                    };
                }
                return node;
            }));
            setActiveSourceNodeId(null);
        }
    };

    const handleSaveSinkConfig = (config: {
        datasourceId: number;
        tableName: string;
        writeMode: 'append' | 'overwrite' | 'error';
    }) => {
        if (activeSinkNodeId) {
            setNodes((nds) => nds.map((node) => {
                if (node.id === activeSinkNodeId) {
                    return {
                        ...node,
                        data: {
                            ...node.data,
                            datasourceId: config.datasourceId,
                            tableName: config.tableName,
                            writeMode: config.writeMode,
                            label: config.tableName || node.data.label,
                        }
                    };
                }
                return node;
            }));
            setActiveSinkNodeId(null);
        }
    };

    const [isRunning, setIsRunning] = React.useState(false);

    const handleRunPipeline = async () => {
        // Validation
        const sourceNodes = nodes.filter(n => n.type === 'source');
        const sinkNodes = nodes.filter(n => n.type === 'sink');

        if (sourceNodes.length === 0 || sinkNodes.length === 0) {
            alert("Pipeline must have at least one source and one sink.");
            return;
        }

        const unconfiguredSource = sourceNodes.find(n => !n.data.datasourceId);
        if (unconfiguredSource) {
            alert(`Source node "${unconfiguredSource.data.label}" is not configured.`);
            return;
        }

        const unconfiguredSink = sinkNodes.find(n => !n.data.datasourceId);
        if (unconfiguredSink) {
            alert(`Sink node "${unconfiguredSink.data.label}" is not configured.`);
            return;
        }

        setIsRunning(true);
        try {
            let pipelineId = currentPipelineId;

            // If not saved, create ad-hoc or save it?
            // User requested "Save" functionality separately.
            // For now, if currentPipelineId exists, we run that.
            // If not, we SHOULD save it first? Or create ad-hoc?
            // Existing logic created ad-hoc.
            // Let's encourage saving, but support ad-hoc.

            if (!pipelineId) {
                // Ad-hoc run (matches previous behavior but maybe we should auto-save?)
                const pipelineData = {
                    name: `Ad-hoc Run ${new Date().toLocaleString()}`,
                    description: "Ad-hoc execution",
                    nodes: nodes,
                    edges: edges
                };
                const created = await etlApi.createPipeline(pipelineData);
                pipelineId = created.id;
            } else {
                // Ensure current state is saved before running?
                // Or assume user saved? Let's auto-save for convenience.
                await etlApi.updatePipeline(pipelineId, {
                    name: pipelineName,
                    description: "Updated before run",
                    nodes: nodes,
                    edges: edges
                });
            }

            if (pipelineId) {
                // 2. Run Pipeline
                await etlApi.runPipeline(pipelineId);
                alert("Pipeline executed successfully!");
            } else {
                throw new Error("Pipeline ID missing after creation attempt.");
            }

        } catch (error: any) {
            console.error("Pipeline failed:", error);
            alert(`Execution failed: ${error.response?.data?.detail || error.message}`);
        } finally {
            setIsRunning(false);
        }
    };

    const handleSavePipeline = async () => {
        if (nodes.length === 0) return;
        setIsSaving(true);
        try {
            if (currentPipelineId) {
                await etlApi.updatePipeline(currentPipelineId, {
                    name: pipelineName,
                    description: "Updated pipeline",
                    nodes: nodes,
                    edges: edges
                });
            } else {
                const newPipeline = await etlApi.createPipeline({
                    name: pipelineName,
                    description: "New pipeline",
                    nodes: nodes,
                    edges: edges
                });
                setCurrentPipelineId(newPipeline.id);
                // setPipelineName(newPipeline.name); // Already set
            }
            await fetchPipelines();
        } catch (error) {
            console.error("Failed to save pipeline:", error);
            alert("Failed to save pipeline");
        } finally {
            setIsSaving(false);
        }
    };

    const handleLoadPipeline = (pipeline: any) => {
        if (nodes.length > 0 && !confirm("Unsaved changes will be lost. Load pipeline?")) return;

        setCurrentPipelineId(pipeline.id);
        setPipelineName(pipeline.name);

        // Rehydrate nodes with function handlers that were lost during serialization
        const hydratedNodes = (pipeline.nodes || []).map((node: ETLNode) => ({
            ...node,
            data: {
                ...node.data,
                onDelete: () => handleDeleteNode(node.id),
            }
        }));

        setNodes(hydratedNodes);
        setEdges(pipeline.edges || []);
    };

    const handleNewPipeline = () => {
        if (nodes.length > 0 && !confirm("Unsaved changes will be lost. Create new?")) return;

        setCurrentPipelineId(null);
        setPipelineName('Untitled Pipeline');
        setNodes([]);
        setEdges([]);
    };

    const handleDeletePipeline = async (e: React.MouseEvent, id: number) => {
        e.stopPropagation();
        if (!confirm("Are you sure you want to delete this pipeline?")) return;

        try {
            await etlApi.deletePipeline(id);
            if (currentPipelineId === id) {
                handleNewPipeline(); // Reset if deleted current
            }
            await fetchPipelines();
        } catch (error) {
            console.error("Failed to delete pipeline:", error);
        }
    };

    return (
        <ReactFlowProvider>
            <div className="h-screen w-full bg-slate-950 text-white flex flex-col">
                {/* Header */}
                <div className="h-16 border-b border-white/10 flex items-center justify-between px-6 bg-slate-900/50 backdrop-blur-md z-10">
                    <div className="flex items-center gap-4">
                        <button
                            onClick={() => navigate('/')}
                            className="p-2 hover:bg-white/5 rounded-lg transition-colors text-slate-400 hover:text-white"
                            title="Back to Chat"
                        >
                            <ArrowLeft className="w-5 h-5" />
                        </button>
                        <div>
                            <div className="flex items-center gap-2">
                                <h1 className="text-xl font-bold bg-gradient-to-r from-purple-400 to-indigo-400 bg-clip-text text-transparent">
                                    ETL Data Factory
                                </h1>
                            </div>
                            <input
                                type="text"
                                value={pipelineName}
                                onChange={(e) => setPipelineName(e.target.value)}
                                className="bg-transparent border-none outline-none text-sm text-slate-400 focus:text-white focus:bg-white/5 rounded px-1 -ml-1 w-64"
                                placeholder="Untitled Pipeline"
                            />
                        </div>
                    </div>

                    <div className="flex items-center gap-4">
                        {/* Model Selector */}
                        <div className="flex items-center gap-2 px-3 py-1.5 bg-slate-800/50 rounded-lg border border-white/10">
                            <Bot className="w-4 h-4 text-slate-400" />
                            <select
                                value={selectedModel}
                                onChange={(e) => setSelectedModel(e.target.value)}
                                className="bg-transparent border-none outline-none text-sm text-slate-200 cursor-pointer min-w-[150px]"
                            >
                                <option value="" disabled className="bg-slate-900 text-slate-500">Select Model</option>
                                {models?.map((model: any) => (
                                    <option key={model.id} value={model.name} className="bg-slate-900 text-slate-200">
                                        {model.displayName || model.name}
                                    </option>
                                ))}
                                {!models?.length && <option value="gpt-4o">GPT-4o (Default)</option>}
                            </select>
                        </div>

                        <button
                            onClick={handleShowHistory}
                            className="px-3 py-2 hover:bg-white/10 text-slate-300 hover:text-white rounded-lg transition-colors flex items-center gap-2"
                            title="Execution History"
                        >
                            <History className="w-5 h-5" />
                        </button>

                        <button
                            onClick={handleSavePipeline}
                            disabled={isSaving}
                            className="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-lg text-sm font-medium transition-colors flex items-center gap-2"
                        >
                            {isSaving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
                            Save
                        </button>

                        <button
                            onClick={handleNewPipeline}
                            className="px-4 py-2 bg-white/10 hover:bg-white/20 text-white rounded-lg text-sm font-medium transition-colors"
                        >
                            New
                        </button>

                        <button
                            onClick={handleRunPipeline}
                            disabled={isRunning}
                            className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors font-medium text-sm shadow-lg shadow-green-500/20 ${isRunning
                                ? 'bg-green-600/50 cursor-not-allowed'
                                : 'bg-green-600 hover:bg-green-500'
                                }`}
                        >
                            <Play className={`w-4 h-4 fill-current ${isRunning ? 'animate-pulse' : ''}`} />
                            {isRunning ? 'Running...' : 'Run Pipeline'}
                        </button>
                    </div>
                </div>

                {/* Main Content */}
                <div className="flex-1 flex overflow-hidden">
                    {/* Sidebar */}
                    <div className="w-64 border-r border-white/10 flex flex-col z-10 glass-panel">
                        {/* Components Section */}
                        <div className="p-4 border-b border-white/10">
                            <h2 className="text-xs font-bold text-slate-400 uppercase tracking-wider mb-3">Components</h2>
                            <div className="space-y-2">
                                <div
                                    className="bg-slate-800/50 p-2.5 rounded-lg border border-white/5 cursor-grab hover:border-indigo-500/50 hover:bg-slate-800 transition-all flex items-center gap-3 group"
                                    onDragStart={(event) => event.dataTransfer.setData('application/reactflow', 'source')}
                                    draggable
                                >
                                    <div className="p-1.5 bg-indigo-500/20 rounded-md group-hover:bg-indigo-500/30 transition-colors">
                                        <Database className="w-4 h-4 text-indigo-400" />
                                    </div>
                                    <span className="text-sm font-medium text-slate-200">Source Table</span>
                                </div>

                                <div
                                    className="bg-slate-800/50 p-2.5 rounded-lg border border-white/5 cursor-grab hover:border-purple-500/50 hover:bg-slate-800 transition-all flex items-center gap-3 group"
                                    onDragStart={(event) => event.dataTransfer.setData('application/reactflow', 'transform')}
                                    draggable
                                >
                                    <div className="p-1.5 bg-purple-500/20 rounded-md group-hover:bg-purple-500/30 transition-colors">
                                        <Wand2 className="w-4 h-4 text-purple-400" />
                                    </div>
                                    <span className="text-sm font-medium text-slate-200">AI Transform</span>
                                </div>

                                <div
                                    className="bg-slate-800/50 p-2.5 rounded-lg border border-white/5 cursor-grab hover:border-emerald-500/50 hover:bg-slate-800 transition-all flex items-center gap-3 group"
                                    onDragStart={(event) => event.dataTransfer.setData('application/reactflow', 'sink')}
                                    draggable
                                >
                                    <div className="p-1.5 bg-emerald-500/20 rounded-md group-hover:bg-emerald-500/30 transition-colors">
                                        <FileOutput className="w-4 h-4 text-emerald-400" />
                                    </div>
                                    <span className="text-sm font-medium text-slate-200">Sink Table</span>
                                </div>
                            </div>
                        </div>

                        {/* Saved Pipelines Section */}
                        <div className="flex-1 overflow-y-auto p-4">
                            <h2 className="text-xs font-bold text-slate-400 uppercase tracking-wider mb-3">Saved Pipelines</h2>
                            <div className="space-y-1">
                                {pipelines.length === 0 && (
                                    <p className="text-xs text-slate-500 text-center py-4">No pipelines saved</p>
                                )}
                                {pipelines.map(pipeline => (
                                    <div
                                        key={pipeline.id}
                                        onClick={() => handleLoadPipeline(pipeline)}
                                        className={`group p-2 rounded-lg cursor-pointer flex items-center justify-between transition-colors ${currentPipelineId === pipeline.id
                                            ? 'bg-indigo-600/20 text-indigo-300 border border-indigo-500/30'
                                            : 'hover:bg-white/5 text-slate-300 border border-transparent'
                                            }`}
                                    >
                                        <div className="flex items-center gap-2 overflow-hidden">
                                            <FileText className="w-3.5 h-3.5 flex-shrink-0 opacity-70" />
                                            <span className="text-sm truncate">{pipeline.name}</span>
                                        </div>
                                        <button
                                            onClick={(e) => handleDeletePipeline(e, pipeline.id)}
                                            className="opacity-0 group-hover:opacity-100 p-1 hover:bg-red-500/20 hover:text-red-400 rounded transition-all"
                                            title="Delete Pipeline"
                                        >
                                            <Trash2 className="w-3.5 h-3.5" />
                                        </button>
                                    </div>
                                ))}
                            </div>
                        </div>
                    </div>

                    {/* Canvas */}
                    <div className="flex-1 h-full bg-slate-950 relative" ref={reactFlowWrapper}>
                        <ReactFlow
                            nodes={nodes}
                            edges={edges}
                            onNodesChange={onNodesChange}
                            onEdgesChange={onEdgesChange}
                            onConnect={onConnect}
                            onDrop={onDrop}
                            onDragOver={onDragOver}
                            onNodeClick={onNodeClick}
                            nodeTypes={nodeTypes}
                            fitView
                            className="bg-slate-950"
                        >
                            <Background color="#334155" gap={16} size={1} />
                            <Controls className="bg-slate-800 border-white/10 fill-white" />
                            {/* <MiniMap
                                className="!bg-slate-800 !border-white/10"
                                maskColor="rgba(15, 23, 42, 0.7)"
                                nodeColor={(n) => {
                                    if (n.type === 'source') return '#6366f1';
                                    if (n.type === 'sink') return '#10b981';
                                    return '#a855f7';
                                }}
                            /> */}
                        </ReactFlow>
                    </div>
                </div>

                {/* Loading Overlay */}
                {isRunning && (
                    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm z-[100] flex flex-col items-center justify-center animate-in fade-in duration-200">
                        <div className="bg-slate-900/80 p-8 rounded-2xl border border-white/10 flex flex-col items-center shadow-2xl">
                            <Loader2 className="w-12 h-12 text-indigo-500 animate-spin mb-4" />
                            <h2 className="text-xl font-bold text-white mb-2">Running Pipeline</h2>
                            <p className="text-slate-400 text-sm">Processing your data transformations...</p>
                        </div>
                    </div>
                )}

                {/* Source Configuration Modal */}
                {createPortal(
                    <SourceConfigModal
                        isOpen={showSourceModal}
                        onClose={() => {
                            setShowSourceModal(false);
                            setActiveSourceNodeId(null);
                        }}
                        onSave={handleSaveSourceConfig}
                        existingConfig={
                            activeSourceNodeId
                                ? nodes.find(n => n.id === activeSourceNodeId)?.data
                                : undefined
                        }
                    />,
                    document.body
                )}

                {/* Transform Configuration Modal */}
                {createPortal(
                    <TransformConfigModal
                        isOpen={showTransformConfigModal}
                        onClose={() => {
                            setShowTransformConfigModal(false);
                            setActiveTransformNodeId(null);
                        }}
                        onSave={handleSaveTransformConfig}
                        upstreamNodes={(() => {
                            // Find all source nodes connected to this transform node
                            if (activeTransformNodeId) {
                                const incomingEdges = edges.filter(e => e.target === activeTransformNodeId);
                                const sourceNodes = incomingEdges.map(edge => nodes.find(n => n.id === edge.source)).filter(n => n && n.type === 'source');

                                return sourceNodes.map(node => ({
                                    id: node!.id,
                                    label: node!.data.label,
                                    datasourceId: node!.data.datasourceId,
                                    selectedColumns: node!.data.selectedColumns,
                                    tableName: node!.data.tableName,
                                }));
                            }
                            return undefined;
                        })()}
                        existingConfig={
                            activeTransformNodeId
                                ? nodes.find(n => n.id === activeTransformNodeId)?.data
                                : undefined
                        }
                        selectedModel={selectedModel}
                    />,
                    document.body
                )}

                {/* Sink Configuration Modal */}
                {createPortal(
                    <SinkConfigModal
                        isOpen={showSinkConfigModal}
                        onClose={() => {
                            setShowSinkConfigModal(false);
                            setActiveSinkNodeId(null);
                        }}
                        onSave={handleSaveSinkConfig}
                        existingConfig={
                            activeSinkNodeId
                                ? (() => {
                                    const data = nodes.find(n => n.id === activeSinkNodeId)?.data;
                                    return data ? {
                                        datasourceId: data.datasourceId,
                                        tableName: data.tableName,
                                        writeMode: data.writeMode as any
                                    } : undefined;
                                })()
                                : undefined
                        }
                    />,
                    document.body
                )}
            </div>

            {/* History Modal */}
            {createPortal(
                showHistoryModal && (
                    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm z-[200] flex items-center justify-end">
                        <div className="w-96 h-full bg-slate-900 border-l border-white/10 p-6 shadow-2xl animate-in slide-in-from-right duration-200">
                            <div className="flex items-center justify-between mb-6">
                                <h2 className="text-lg font-bold text-white flex items-center gap-2">
                                    <History className="w-5 h-5 text-indigo-400" />
                                    Execution History
                                </h2>
                                <button
                                    onClick={() => setShowHistoryModal(false)}
                                    className="text-slate-400 hover:text-white"
                                >
                                    ✕
                                </button>
                            </div>

                            <div className="space-y-4 overflow-y-auto h-[calc(100vh-100px)]">
                                {executions.length === 0 && (
                                    <p className="text-slate-500 text-center py-4">No executions found.</p>
                                )}
                                {executions.map((exec) => (
                                    <div
                                        key={exec.id}
                                        className="bg-slate-800/50 p-4 rounded-lg border border-white/5 cursor-pointer hover:bg-slate-800 transition-colors group"
                                        onClick={() => setSelectedExecution(exec)}
                                    >
                                        <div className="flex items-center justify-between mb-2">
                                            <span className={`text-xs font-bold px-2 py-1 rounded-full uppercase ${exec.status === 'completed' ? 'bg-green-500/20 text-green-400' :
                                                exec.status === 'failed' ? 'bg-red-500/20 text-red-400' :
                                                    'bg-blue-500/20 text-blue-400'
                                                }`}>
                                                {exec.status}
                                            </span>
                                            <span className="text-xs text-slate-500">
                                                {new Date(exec.started_at).toLocaleString()}
                                            </span>
                                        </div>
                                        {exec.status === 'completed' && exec.finished_at && (
                                            <div className="mt-2 text-xs text-slate-400">
                                                Duration: {((new Date(exec.finished_at).getTime() - new Date(exec.started_at).getTime()) / 1000).toFixed(1)}s
                                            </div>
                                        )}
                                        {exec.error_message && (
                                            <div className="mt-2 text-xs text-red-400 bg-red-950/30 p-2 rounded max-h-32 overflow-y-auto font-mono whitespace-pre-wrap">
                                                {exec.error_message}
                                            </div>
                                        )}
                                    </div>
                                ))}
                            </div>
                        </div>
                    </div>
                ),
                document.body
            )}

            {/* Execution Details JSON Modal */}
            {createPortal(
                selectedExecution && (
                    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm z-[210] flex items-center justify-center p-4">
                        <div className="bg-slate-900 border border-white/10 rounded-xl shadow-2xl w-full max-w-2xl max-h-[80vh] flex flex-col animate-in zoom-in-95 duration-200">
                            <div className="flex items-center justify-between p-4 border-b border-white/10">
                                <h2 className="text-lg font-bold text-white flex items-center gap-2">
                                    <FileText className="w-5 h-5 text-indigo-400" />
                                    Execution Details
                                </h2>
                                <button
                                    onClick={() => setSelectedExecution(null)}
                                    className="text-slate-400 hover:text-white"
                                >
                                    ✕
                                </button>
                            </div>
                            <div className="p-4 overflow-auto font-mono text-xs text-slate-300 bg-slate-950/50 flex-1">
                                <pre>{JSON.stringify(selectedExecution, null, 2)}</pre>
                            </div>
                            <div className="p-4 border-t border-white/10 flex justify-end">
                                <button
                                    onClick={() => setSelectedExecution(null)}
                                    className="px-4 py-2 bg-white/10 hover:bg-white/20 text-white rounded-lg text-sm transition-colors"
                                >
                                    Close
                                </button>
                            </div>
                        </div>
                    </div>
                ),
                document.body
            )}
        </ReactFlowProvider>
    );
};
