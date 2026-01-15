import { useState, useRef, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
// @ts-ignore
import * as ReactGridLayout from 'react-grid-layout';
import 'react-grid-layout/css/styles.css';
import 'react-resizable/css/styles.css';
import { Box, Paper, IconButton, Typography, Fab, Button, Dialog, DialogTitle, DialogContent, DialogActions, TextField, List, ListItem, ListItemText, ListItemButton } from '@mui/material';
import { ChartWidget } from '../components/Dashboard/ChartWidget';
import { BuilderChat, type BuilderChatRef } from '../components/Dashboard/BuilderChat';
import { GripVertical, X, MessageSquare, ChevronRight, Save, FolderOpen, Plus, Pencil, Database, Settings } from 'lucide-react';
import { dashboardApi } from '../services/api';

// Robustly get Responsive component
const Responsive = ReactGridLayout.Responsive || (ReactGridLayout.default ? ReactGridLayout.default.Responsive : undefined);

// Removed WidthProvider usage due to import issues. Using manual ResizeObserver instead.

// Example layout and widgets for dev
const initialLayouts = {
    lg: []
};

const initialWidgets = {};

export const DashboardPage = () => {
    const navigate = useNavigate();
    const [layouts, setLayouts] = useState<any>(initialLayouts);
    const [widgets, setWidgets] = useState<any>(initialWidgets);
    const [isDraggable] = useState(true);
    const [isChatOpen, setIsChatOpen] = useState(true);

    // Persistence State
    const [currentDashboard, setCurrentDashboard] = useState<any>(null);
    const [dashboardsList, setDashboardsList] = useState<any[]>([]);
    const [isSaveDialogOpen, setIsSaveDialogOpen] = useState(false);
    const [isLoadDialogOpen, setIsLoadDialogOpen] = useState(false);
    const [newDashboardTitle, setNewDashboardTitle] = useState('');

    // Manual Width Management
    const [width, setWidth] = useState(1200);
    const containerRef = useRef<HTMLDivElement>(null);
    const builderChatRef = useRef<BuilderChatRef>(null);

    // Title Editing State
    const [isEditingTitle, setIsEditingTitle] = useState(false);
    const [titleInput, setTitleInput] = useState('My Dashboard');

    useEffect(() => {
        if (currentDashboard) {
            setTitleInput(currentDashboard.title);
            localStorage.setItem('lastDashboardId', currentDashboard.id.toString());
        } else {
            setTitleInput('My Dashboard');
        }
    }, [currentDashboard]);

    // Load Last Dashboard on Mount
    useEffect(() => {
        const lastId = localStorage.getItem('lastDashboardId');
        if (lastId) {
            handleLoadDashboard(Number(lastId));
        }
    }, []);

    useEffect(() => {
        const resizeObserver = new ResizeObserver((entries) => {
            if (entries[0]) {
                setWidth(entries[0].contentRect.width);
            }
        });

        if (containerRef.current) {
            resizeObserver.observe(containerRef.current);
        }

        return () => resizeObserver.disconnect();
    }, []);

    // Load Dashboards List
    const fetchDashboards = async () => {
        try {
            const list = await dashboardApi.getDashboards();
            setDashboardsList(list);
        } catch (e) {
            console.error("Failed to fetch dashboards", e);
        }
    };

    const handleOpenLoadDialog = () => {
        fetchDashboards();
        setIsLoadDialogOpen(true);
    };

    const handleLoadDashboard = async (id: number) => {
        try {
            const dashboard = await dashboardApi.getDashboard(id);
            setCurrentDashboard(dashboard);
            if (dashboard.layout_config) {
                // Assuming layout_config stores {layouts, widgets}
                if (dashboard.layout_config.layouts) setLayouts(dashboard.layout_config.layouts);
                if (dashboard.layout_config.widgets) setWidgets(dashboard.layout_config.widgets);
            }
            setIsLoadDialogOpen(false);
        } catch (e) {
            console.error("Failed to load dashboard", e);
        }
    };

    const handleSaveDashboard = async () => {
        if (currentDashboard) {
            // Update existing
            try {
                await dashboardApi.updateDashboard(currentDashboard.id, {
                    title: currentDashboard.title,
                    layout_config: { layouts, widgets }
                });
                alert('Dashboard saved!');
            } catch (e) {
                console.error("Failed to save", e);
                alert('Failed to save dashboard');
            }
        } else {
            // Open dialog for new
            setNewDashboardTitle(titleInput !== 'My Dashboard' ? titleInput : '');
            setIsSaveDialogOpen(true);
        }
    };

    const handleCreateDashboard = async () => {
        if (!newDashboardTitle.trim()) return;
        try {
            const newDash = await dashboardApi.createDashboard({
                title: newDashboardTitle,
                layout_config: { layouts, widgets }
            });
            setCurrentDashboard(newDash);
            setIsSaveDialogOpen(false);
            alert('Dashboard created!');
        } catch (e) {
            console.error("Failed to create", e);
            alert('Failed to create dashboard');
        }
    };

    const handleNewDashboard = () => {
        setCurrentDashboard(null);
        setLayouts({ lg: [] }); // Reset
        setWidgets({});
    };

    const onLayoutChange = (_currentLayout: any, allLayouts: any) => {
        setLayouts(allLayouts);
    };

    const handleAddChart = (chartConfig: any) => {
        const id = Date.now().toString();

        // Add widget to state
        setWidgets((prev: any) => ({
            ...prev,
            [id]: chartConfig
        }));

        // Add item to layout
        const newItem = { i: id, x: 0, y: Infinity, w: 6, h: 4 }; // y: Infinity puts it at bottom
        setLayouts((prev: any) => ({
            ...prev,
            lg: [...(prev.lg || []), newItem]
        }));
    };

    const handleDeleteWidget = (id: string) => {
        const { [id]: removed, ...remainingWidgets } = widgets;
        setWidgets(remainingWidgets);

        setLayouts((prev: any) => ({
            ...prev,
            lg: prev.lg.filter((item: any) => item.i !== id)
        }));
    };

    const handleEditChart = (config: any) => {
        setIsChatOpen(true);
        // Allow time for sidebar to open if it was closed
        setTimeout(() => {
            builderChatRef.current?.startEdit(config);
        }, 100);
    };

    return (
        <Box sx={{ display: 'flex', height: '100vh', width: '100%', overflow: 'hidden' }} className="mesh-gradient">
            {/* Main Canvas Area */}
            <Box sx={{ flexGrow: 1, height: '100%', display: 'flex', flexDirection: 'column', transition: 'margin 0.3s' }}>
                <Box sx={{ p: 2, display: 'flex', justifyContent: 'space-between', alignItems: 'center', backdropFilter: 'blur(10px)', bgcolor: 'rgba(255,255,255,0.1)', borderBottom: '1px solid rgba(255,255,255,0.1)' }}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                        {/* Matched ETL Data Factory Gradient: from-purple-400 to-indigo-400 (approximated in CSS) */}
                        <Box
                            sx={{ display: 'flex', alignItems: 'center', gap: 1, cursor: 'text' }}
                            onClick={() => !isEditingTitle && setIsEditingTitle(true)}
                        >
                            {isEditingTitle ? (
                                <TextField
                                    autoFocus
                                    value={titleInput}
                                    onChange={(e) => setTitleInput(e.target.value)}
                                    onBlur={() => {
                                        setIsEditingTitle(false);
                                        if (currentDashboard) {
                                            setCurrentDashboard({ ...currentDashboard, title: titleInput });
                                        }
                                    }}
                                    onKeyDown={(e) => {
                                        if (e.key === 'Enter') {
                                            setIsEditingTitle(false);
                                            if (currentDashboard) {
                                                setCurrentDashboard({ ...currentDashboard, title: titleInput });
                                            }
                                        }
                                    }}
                                    variant="standard"
                                    InputProps={{
                                        disableUnderline: true,
                                        sx: {
                                            fontSize: '2.125rem',
                                            fontWeight: 'bold',
                                            color: '#6366f1', // Indigo-500
                                            minWidth: '200px'
                                        }
                                    }}
                                />
                            ) : (
                                <Typography
                                    variant="h4"
                                    fontWeight="bold"
                                    sx={{
                                        background: 'linear-gradient(to right, #a78bfa, #818cf8)',
                                        WebkitBackgroundClip: 'text',
                                        WebkitTextFillColor: 'transparent',
                                        borderBottom: '1px dashed transparent',
                                        '&:hover': {
                                            borderBottom: '1px dashed rgba(129, 140, 248, 0.5)'
                                        }
                                    }}
                                >
                                    {titleInput}
                                </Typography>
                            )}
                            {!isEditingTitle && <Pencil size={16} className="text-slate-400 opacity-50" />}
                        </Box>
                        <Box sx={{ display: 'flex', gap: 1 }}>
                            <Button startIcon={<Settings size={16} />} size="small" variant="outlined" onClick={() => navigate('/settings')} sx={{ borderRadius: 2, textTransform: 'none', borderColor: '#4f46e5', color: '#4f46e5', '&:hover': { borderColor: '#4338ca', bgcolor: 'rgba(79, 70, 229, 0.08)' } }}>
                                Settings
                            </Button>
                            <Button startIcon={<Database size={16} />} size="small" variant="outlined" onClick={() => navigate('/etl')} sx={{ borderRadius: 2, textTransform: 'none', borderColor: '#4f46e5', color: '#4f46e5', '&:hover': { borderColor: '#4338ca', bgcolor: 'rgba(79, 70, 229, 0.08)' } }}>
                                ETL
                            </Button>
                            <Button startIcon={<Plus size={16} />} size="small" variant="outlined" onClick={handleNewDashboard} sx={{ borderRadius: 2, textTransform: 'none', borderColor: '#4f46e5', color: '#4f46e5', '&:hover': { borderColor: '#4338ca', bgcolor: 'rgba(79, 70, 229, 0.08)' } }}>
                                New
                            </Button>
                            <Button startIcon={<FolderOpen size={16} />} size="small" variant="outlined" onClick={handleOpenLoadDialog} sx={{ borderRadius: 2, textTransform: 'none', borderColor: '#4f46e5', color: '#4f46e5', '&:hover': { borderColor: '#4338ca', bgcolor: 'rgba(79, 70, 229, 0.08)' } }}>
                                Open
                            </Button>
                            <Button startIcon={<Save size={16} />} size="small" variant="contained" onClick={handleSaveDashboard} sx={{ borderRadius: 2, textTransform: 'none', bgcolor: '#4f46e5', '&:hover': { bgcolor: '#4338ca' } }}>
                                {/* Indigo-600 to Indigo-700 */}
                                Save
                            </Button>
                        </Box>
                    </Box>
                    {!isChatOpen && (
                        <Fab color="primary" size="medium" onClick={() => setIsChatOpen(true)} sx={{ bgcolor: '#4f46e5', '&:hover': { bgcolor: '#4338ca' } }}>
                            <MessageSquare />
                        </Fab>
                    )}
                </Box>

                <Box ref={containerRef} sx={{ flexGrow: 1, overflow: 'auto', p: 2 }}>
                    {Responsive && (
                        <Responsive
                            className="layout"
                            width={width}
                            layouts={layouts}
                            breakpoints={{ lg: 1200, md: 996, sm: 768, xs: 480, xxs: 0 }}
                            cols={{ lg: 12, md: 10, sm: 6, xs: 4, xxs: 2 }}
                            rowHeight={100}
                            onLayoutChange={onLayoutChange}
                            isDraggable={isDraggable}
                            isResizable={isDraggable}
                            draggableHandle=".drag-handle"
                        >
                            {layouts.lg.map((item: any) => (
                                <Paper
                                    key={item.i}
                                    className="glass-card"
                                    sx={{
                                        height: '100%',
                                        display: 'flex',
                                        flexDirection: 'column',
                                        overflow: 'hidden',
                                        position: 'relative',
                                        borderRadius: 3,
                                        bgcolor: 'transparent !important', // Override default paper
                                        boxShadow: 'none !important'     // Handled by CSS
                                    }}
                                >
                                    <Box
                                        className="drag-handle"
                                        sx={{
                                            p: 1.5,
                                            display: 'flex',
                                            justifyContent: 'space-between',
                                            alignItems: 'center',
                                            cursor: 'grab',
                                            borderBottom: '1px solid rgba(255,255,255,0.1)',
                                            bgcolor: 'rgba(255,255,255,0.1)'
                                        }}
                                    >
                                        <GripVertical size={16} color="#999" />
                                        <IconButton size="small" onClick={() => handleDeleteWidget(item.i)}>
                                            <X size={16} />
                                        </IconButton>
                                    </Box>
                                    <Box sx={{ flexGrow: 1, overflow: 'hidden' }}>
                                        {widgets[item.i] && (
                                            <ChartWidget
                                                config={widgets[item.i]}
                                                onEdit={() => handleEditChart(widgets[item.i])}
                                            />
                                        )}
                                    </Box>
                                </Paper>
                            ))}
                        </Responsive>
                    )}
                </Box>
            </Box>

            {/* Builder Chat Sidebar */}
            <Paper
                elevation={4}
                sx={{
                    width: isChatOpen ? 350 : 0,
                    transition: 'width 0.3s ease-in-out',
                    overflow: 'hidden',
                    display: 'flex',
                    flexDirection: 'column',
                    borderLeft: '1px solid #ddd',
                    position: 'relative',
                    height: '100%'
                }}
            >
                {isChatOpen && (
                    <Box sx={{ position: 'absolute', top: 10, right: 10, zIndex: 10 }}>
                        <IconButton size="small" onClick={() => setIsChatOpen(false)}>
                            <ChevronRight />
                        </IconButton>
                    </Box>
                )}

                <Box sx={{ width: 350, height: '100%', opacity: isChatOpen ? 1 : 0, transition: 'opacity 0.2s' }}>
                    <BuilderChat ref={builderChatRef} onAddChart={handleAddChart} dashboardId={currentDashboard?.id} />
                </Box>
            </Paper>

            {/* Save Dialog */}
            <Dialog open={isSaveDialogOpen} onClose={() => setIsSaveDialogOpen(false)}>
                <DialogTitle>Save Dashboard</DialogTitle>
                <DialogContent>
                    <TextField
                        autoFocus
                        margin="dense"
                        label="Dashboard Title"
                        fullWidth
                        value={newDashboardTitle}
                        onChange={(e) => setNewDashboardTitle(e.target.value)}
                    />
                </DialogContent>
                <DialogActions>
                    <Button onClick={() => setIsSaveDialogOpen(false)}>Cancel</Button>
                    <Button onClick={handleCreateDashboard} variant="contained">Save</Button>
                </DialogActions>
            </Dialog>

            {/* Load Dialog */}
            <Dialog open={isLoadDialogOpen} onClose={() => setIsLoadDialogOpen(false)} maxWidth="sm" fullWidth>
                <DialogTitle>Open Dashboard</DialogTitle>
                <DialogContent>
                    <List>
                        {dashboardsList.map((dash) => (
                            <ListItem key={dash.id} disablePadding>
                                <ListItemButton onClick={() => handleLoadDashboard(dash.id)}>
                                    <ListItemText primary={dash.title} secondary={`Last updated: ${new Date(dash.updated_at).toLocaleDateString()}`} />
                                </ListItemButton>
                            </ListItem>
                        ))}
                    </List>
                </DialogContent>
                <DialogActions>
                    <Button onClick={() => setIsLoadDialogOpen(false)}>Cancel</Button>
                </DialogActions>
            </Dialog>
        </Box>
    );
};
