import React, { useState, useRef, useEffect, forwardRef, useImperativeHandle } from 'react';
import { Box, TextField, IconButton, Paper, Typography, Button, CircularProgress, Select, MenuItem, FormControl, InputLabel, Dialog, DialogTitle, DialogContent, DialogActions } from '@mui/material';
import { Send, Plus, Sparkles, Eye, Pencil } from 'lucide-react';
import { ChartWidget } from './ChartWidget'; // Reuse for preview
import { chatApi, etlApi, settingsApi } from '../../services/api'; // Import settingsApi

interface Message {
    id: string;
    type: 'human' | 'ai';
    content?: string;
    chartConfig?: any; // The generated chart config
}

interface BuilderChatProps {
    onAddChart: (config: any) => void;
    dashboardId?: number;
}

export interface BuilderChatRef {
    startEdit: (config: any) => void;
}

export const BuilderChat = forwardRef<BuilderChatRef, BuilderChatProps>(({ onAddChart, dashboardId }, ref) => {
    const [messages, setMessages] = useState<Message[]>([
        { id: '0', type: 'ai', content: 'Hi! I can help you build charts. Just ask, e.g., "Show me sales by region".' }
    ]);
    const [input, setInput] = useState('');
    const [isLoading, setIsLoading] = useState(false);
    const [dataSources, setDataSources] = useState<any[]>([]);
    const [selectedDataSource, setSelectedDataSource] = useState<number | ''>('');
    const [models, setModels] = useState<any[]>([]); // Dynamic models
    const [selectedModel, setSelectedModel] = useState('');
    const [previewConfig, setPreviewConfig] = useState<any>(null);
    const [isPreviewOpen, setIsPreviewOpen] = useState(false);
    const messagesEndRef = useRef<HTMLDivElement>(null);
    const inputRef = useRef<HTMLInputElement>(null); // For auto-focus

    // Context from Canvas Edit
    const [contextConfig, setContextConfig] = useState<any>(null);

    useImperativeHandle(ref, () => ({
        startEdit: (config: any) => {
            console.log("Starting edit for:", config);
            setContextConfig(config);
            setInput("Change this chart: ");
            // Focus input
            setTimeout(() => {
                inputRef.current?.focus();
            }, 100);
        }
    }));

    const scrollToBottom = () => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    };

    useEffect(() => {
        scrollToBottom();
    }, [messages]);

    useEffect(() => {
        const fetchData = async () => {
            // 1. Fetch Data Sources
            try {
                const sources = await etlApi.getDataSources();
                setDataSources(sources);
                if (sources.length > 0) {
                    setSelectedDataSource(sources[0].id);
                }
            } catch (e) {
                console.error("Failed to fetch data sources:", e);
            }

            // 2. Fetch User Models
            try {
                const userModels = await settingsApi.getModels();
                setModels(userModels);
                if (userModels.length > 0) {
                    setSelectedModel(userModels[0].name); // Use 'name' as the ID/value for the API
                } else {
                    // Fallback if no models configured
                    setSelectedModel('gpt-4o');
                }
            } catch (e) {
                console.error("Failed to fetch models:", e);
                setSelectedModel('gpt-4o'); // Fallback
            }
        };
        fetchData();
    }, []);

    // Load Chat History for Dashboard
    useEffect(() => {
        const loadHistory = async () => {
            if (!dashboardId) {
                setMessages([{ id: '0', type: 'ai', content: 'Hi! I can help you build charts. Just ask, e.g., "Show me sales by region".' }]);
                return;
            }

            try {
                const conversations = await chatApi.getConversations();
                // Find conversation linked to this dashboard
                const dashboardChat = conversations.find((c: any) => c.dashboard_id === dashboardId);

                if (dashboardChat && dashboardChat.messages) {
                    setMessages(dashboardChat.messages.map((msg: any) => ({
                        id: msg.id,
                        type: msg.type === 'human' ? 'human' : 'ai',
                        content: msg.content,
                        chartConfig: msg.chartConfig
                    })));
                } else {
                    setMessages([{ id: '0', type: 'ai', content: 'Hi! I can help you build charts. Just ask, e.g., "Show me sales by region".' }]);
                }
            } catch (e) {
                console.error("Failed to load chat history", e);
            }
        };
        loadHistory();
    }, [dashboardId]);

    const handleSend = async () => {
        if (!input.trim()) return;
        if (!selectedDataSource) {
            setMessages(prev => [...prev, { id: Date.now().toString(), type: 'ai', content: 'Please select a data source first.' }]);
            return;
        }

        const userMsg: Message = { id: Date.now().toString(), type: 'human', content: input };

        // Optimistically show the user message
        // If we have contextConfig, we might want to indicate it visually?
        // Ideally the user knows because they clicked "Edit".

        setMessages(prev => [...prev, userMsg]);
        setInput('');
        setIsLoading(true);

        // Capture current context and clear it immediately so future messages don't reuse it accidentally
        const currentContext = contextConfig;
        setContextConfig(null);

        try {
            const response = await chatApi.generateChart({
                message: userMsg.content || '',
                dataSourceId: Number(selectedDataSource),
                model: selectedModel,
                dashboardId: dashboardId,
                chartContext: currentContext // Pass explicit context
            });

            if (response.error) {
                setMessages(prev => [...prev, {
                    id: (Date.now() + 1).toString(),
                    type: 'ai',
                    content: response.message || "An error occurred."
                }]);
            } else {
                setMessages(prev => [...prev, {
                    id: (Date.now() + 1).toString(),
                    type: 'ai',
                    content: response.message,
                    chartConfig: response.chartConfig
                }]);

                // Auto-open preview for the new chart
                if (response.chartConfig) {
                    setPreviewConfig(response.chartConfig);
                    setIsPreviewOpen(true);
                }
            }
        } catch (e: any) {
            setMessages(prev => [...prev, {
                id: (Date.now() + 1).toString(),
                type: 'ai',
                content: `Error: ${e.message || "Failed to generate chart."} `
            }]);
        } finally {
            setIsLoading(false);
        }
    };

    return (
        <Paper className="glass-panel" sx={{ height: '100%', display: 'flex', flexDirection: 'column', bgcolor: 'transparent !important', borderRadius: 0, boxShadow: 'none !important', borderLeft: '1px solid rgba(255,255,255,0.1)' }}>
            <Box sx={{ p: 2, borderBottom: '1px solid rgba(255,255,255,0.1)', display: 'flex', alignItems: 'center', gap: 1 }}>
                <Sparkles size={20} className="text-indigo-400" />
                <Typography variant="h6" sx={{ fontWeight: 600, background: 'linear-gradient(to right, #a78bfa, #818cf8)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>
                    AI Builder
                </Typography>
            </Box>

            <Box sx={{ p: 2, borderBottom: '1px solid rgba(255,255,255,0.1)', display: 'flex', gap: 2 }}>
                <FormControl fullWidth size="small">
                    <InputLabel>Data Source</InputLabel>
                    <Select
                        value={selectedDataSource}
                        label="Data Source"
                        onChange={(e) => setSelectedDataSource(Number(e.target.value))}
                        sx={{ bgcolor: 'rgba(255,255,255,0.05)' }}
                    >
                        {dataSources.map((ds) => (
                            <MenuItem key={ds.id} value={ds.id}>{ds.name}</MenuItem>
                        ))}
                    </Select>
                </FormControl>

                <FormControl fullWidth size="small">
                    <InputLabel>Model</InputLabel>
                    <Select
                        value={selectedModel}
                        label="Model"
                        onChange={(e) => setSelectedModel(e.target.value)}
                        sx={{ bgcolor: 'rgba(255,255,255,0.05)' }}
                    >
                        {models.length > 0 ? (
                            models.map((model) => (
                                <MenuItem key={model.id} value={model.name}>{model.display_name || model.name}</MenuItem>
                            ))
                        ) : (
                            <MenuItem value="gpt-4o">GPT-4o (Default)</MenuItem>
                        )}
                    </Select>
                </FormControl>
            </Box>

            <Box sx={{ flexGrow: 1, overflow: 'auto', p: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
                {messages.map((msg) => (
                    <Box key={msg.id} sx={{ alignSelf: msg.type === 'human' ? 'flex-end' : 'flex-start', maxWidth: '85%' }}>
                        <Paper
                            sx={{
                                p: 2,
                                bgcolor: msg.type === 'human' ? 'transparent' : 'rgba(255,255,255,0.8)',
                                color: msg.type === 'human' ? 'white' : 'text.primary',
                                background: msg.type === 'human' ? 'linear-gradient(135deg, #6366f1 0%, #a855f7 100%)' : undefined,
                                borderRadius: 3,
                                boxShadow: '0 4px 6px rgba(0,0,0,0.1)'
                            }}
                        >
                            {msg.content && <Typography variant="body2">{msg.content}</Typography>}

                            {msg.chartConfig && (
                                <Box sx={{ mt: 2, display: 'flex', gap: 1 }}>
                                    <Button
                                        variant="outlined"
                                        size="small"
                                        startIcon={<Eye size={16} />}
                                        onClick={() => {
                                            setPreviewConfig(msg.chartConfig);
                                            setIsPreviewOpen(true);
                                        }}
                                        sx={{ bgcolor: 'rgba(255,255,255,0.2)', color: 'inherit', borderColor: 'rgba(255,255,255,0.3)', '&:hover': { bgcolor: 'rgba(255,255,255,0.3)' } }}
                                    >
                                        View
                                    </Button>
                                    <Button
                                        variant="outlined"
                                        size="small"
                                        startIcon={<Pencil size={16} />}
                                        onClick={() => {
                                            setInput("Change this chart: ");
                                            setContextConfig(msg.chartConfig);
                                            // Focus input
                                            setTimeout(() => {
                                                inputRef.current?.focus();
                                            }, 100);
                                        }}
                                        sx={{ bgcolor: 'rgba(255,255,255,0.2)', color: 'inherit', borderColor: 'rgba(255,255,255,0.3)', '&:hover': { bgcolor: 'rgba(255,255,255,0.3)' } }}
                                    >
                                        Edit
                                    </Button>
                                </Box>
                            )}
                        </Paper>
                    </Box>
                ))}
                {isLoading && (
                    <Box sx={{ alignSelf: 'flex-start' }}>
                        <CircularProgress size={20} />
                    </Box>
                )}
                <div ref={messagesEndRef} />
            </Box>

            <Box sx={{ p: 2, borderTop: '1px solid rgba(255,255,255,0.1)', display: 'flex', gap: 1 }}>
                <TextField
                    inputRef={inputRef}
                    fullWidth
                    size="small"
                    placeholder={contextConfig ? `Refining chart: ${contextConfig.title}` : "Describe a chart..."}
                    value={input}
                    onChange={(e) => setInput(e.target.value)}
                    onKeyPress={(e) => e.key === 'Enter' && handleSend()}
                    disabled={isLoading}
                    className="glass-input" // Use global class
                    sx={{
                        '& .MuiOutlinedInput-root': {
                            bgcolor: 'rgba(255,255,255,0.4)',
                            borderRadius: 2
                        }
                    }}
                />
                <IconButton
                    color="primary"
                    onClick={handleSend}
                    disabled={isLoading || !input.trim()}
                    sx={{ bgcolor: '#4f46e5', color: 'white', '&:hover': { bgcolor: '#4338ca' } }}
                >
                    <Send size={20} />
                </IconButton>
            </Box>

            {/* Chart Preview Modal */}
            <Dialog
                open={isPreviewOpen}
                onClose={() => setIsPreviewOpen(false)}
                maxWidth="md"
                fullWidth
            >
                <DialogTitle>Chart Preview</DialogTitle>
                <DialogContent>
                    {previewConfig && (
                        <Box sx={{ height: 400, width: '100%', mt: 2 }}>
                            <ChartWidget config={previewConfig} height={350} />
                        </Box>
                    )}
                </DialogContent>
                <DialogActions>
                    <Button onClick={() => setIsPreviewOpen(false)}>Close</Button>
                    <Button
                        onClick={() => {
                            if (previewConfig) {
                                onAddChart(previewConfig);
                                setIsPreviewOpen(false);
                            }
                        }}
                        variant="contained"
                        startIcon={<Plus size={16} />}
                    >
                        Add to Dashboard
                    </Button>
                </DialogActions>
            </Dialog>
        </Paper >
    );
});
