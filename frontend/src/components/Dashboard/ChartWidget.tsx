import React from 'react';
import { BarChart } from '@mui/x-charts/BarChart';
import { LineChart } from '@mui/x-charts/LineChart';
import { PieChart } from '@mui/x-charts/PieChart';
import { ScatterChart } from '@mui/x-charts/ScatterChart';
import { Card, CardContent, Typography, IconButton } from '@mui/material';
import { Pencil } from 'lucide-react';

// Define the shape of chart config (matching backend schema)
interface ChartConfig {
    title: string;
    chartType: 'bar' | 'line' | 'pie' | 'scatter';
    series: any[];
    xAxis?: any[];
    yAxis?: any[];
    dataset?: any[]; // For dataset-based charts
}

interface ChartWidgetProps {
    config: ChartConfig;
    height?: number;
    onEdit?: () => void;
}

// Simple Error Boundary Component
class ErrorBoundary extends React.Component<{ children: React.ReactNode }, { hasError: boolean; error: Error | null }> {
    constructor(props: { children: React.ReactNode }) {
        super(props);
        this.state = { hasError: false, error: null };
    }

    static getDerivedStateFromError(error: Error) {
        return { hasError: true, error };
    }

    componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
        console.error("ChartWidget Error:", error, errorInfo);
    }

    render() {
        if (this.state.hasError) {
            return (
                <Card sx={{ height: '100%', width: '100%', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', p: 2, bgcolor: '#fff0f0' }}>
                    <Typography color="error" variant="subtitle2" gutterBottom>
                        Cannot render chart
                    </Typography>
                    <Typography variant="caption" color="text.secondary" sx={{ textAlign: 'center' }}>
                        {this.state.error?.message || "Unknown error"}
                    </Typography>
                </Card>
            );
        }

        return this.props.children;
    }
}

export const ChartWidget: React.FC<ChartWidgetProps> = ({ config, height = 300, onEdit }) => {
    const { chartType, series, xAxis, yAxis, title, dataset } = config || {};

    // Deep validation and cleaning
    const validSeries = Array.isArray(series)
        ? series
            .filter(s => s && Array.isArray(s.data) && s.data.length > 0)
            .map((s, i) => ({
                ...s,
                id: s.id || `series-${i}`, // Ensure ID
                type: s.type || chartType, // FORCE the type to match the chart component
                data: s.data.map((d: any) => d === null || d === undefined ? 0 : d) // No nulls in data for simple charts
            }))
        : [];

    // Validate xAxis exists and has data if it's required (mostly for Bar/Line)
    const validXAxis = Array.isArray(xAxis) && xAxis.length > 0
        ? xAxis.map((ax, i) => ({
            ...ax,
            id: ax.id || `xaxis-${i}`,
            // Ensure data is array
            data: Array.isArray(ax.data) ? ax.data : []
        }))
        : [{ data: [], scaleType: 'band' }]; // Fallback default xAxis

    if (!config || validSeries.length === 0) {
        return (
            <Card sx={{ height: '100%', width: '100%', display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
                <Typography color="text.secondary">No valid chart data available</Typography>
            </Card>
        );
    }

    // Common props for charts
    const commonProps = {
        height,
        slotProps: { legend: { hidden: true } } as any,
        margin: { left: 70, bottom: 40, right: 20, top: 20 },
    };

    const renderChart = () => {
        // Only pass dataset if it's actually valid and non-empty, otherwise undefined
        const effectiveDataset = Array.isArray(dataset) && dataset.length > 0 ? dataset : undefined;

        switch (chartType) {
            case 'bar':
                return (
                    <BarChart
                        xAxis={validXAxis}
                        series={validSeries}
                        dataset={effectiveDataset}
                        {...commonProps}
                    />
                );
            case 'line':
                return (
                    <LineChart
                        xAxis={validXAxis}
                        series={validSeries}
                        dataset={effectiveDataset}
                        {...commonProps}
                    />
                );
            case 'pie':
                // Pie chart handles series differently
                const pieSourceSeries = validSeries[0];
                if (!pieSourceSeries || !pieSourceSeries.data) return <Typography color="error">Invalid Pie Data</Typography>;

                return (
                    <PieChart
                        series={[
                            {
                                data: pieSourceSeries.data.map((value: number, index: number) => ({
                                    id: index,
                                    value: Number(value) || 0,
                                    label: pieSourceSeries.label || `Item ${index}`,
                                })),
                            },
                        ]}
                        height={height}
                    />
                );
            case 'scatter':
                return (
                    <ScatterChart
                        series={validSeries}
                        xAxis={validXAxis}
                        yAxis={yAxis}
                        {...commonProps}
                    />
                );
            default:
                return <Typography color="error">Unsupported Chart Type: {chartType}</Typography>;
        }
    };

    return (
        <ErrorBoundary>
            <Card sx={{ height: '100%', width: '100%', display: 'flex', flexDirection: 'column' }}>
                <CardContent sx={{ flexGrow: 1, p: 2 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 8 }}>
                        <Typography variant="h6">
                            {title}
                        </Typography>
                        {onEdit && (
                            <IconButton size="small" onClick={onEdit} sx={{ mt: -0.5, mr: -0.5 }}>
                                <Pencil size={16} />
                            </IconButton>
                        )}
                    </div>
                    <div style={{ width: '100%', height: '100%', minHeight: 250 }}>
                        {renderChart()}
                    </div>
                </CardContent>
            </Card>
        </ErrorBoundary>
    );
};
