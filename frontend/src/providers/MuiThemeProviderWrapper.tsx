import React, { useMemo } from 'react';
import { createTheme, ThemeProvider as MuiThemeProvider } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';
import { useTheme } from '../contexts/ThemeContext';

export const MuiThemeProviderWrapper = ({ children }: { children: React.ReactNode }) => {
    const { theme } = useTheme();

    const muiTheme = useMemo(() => {
        return createTheme({
            palette: {
                mode: theme,
                ...(theme === 'dark' ? {
                    background: {
                        default: '#020617', // slate-950
                        paper: '#1e293b',   // slate-800
                    },
                    primary: {
                        main: '#6366f1', // Indigo-500
                    },
                    text: {
                        primary: '#f8fafc', // slate-50
                        secondary: '#94a3b8', // slate-400
                    }
                } : {
                    background: {
                        default: '#f8fafc', // slate-50
                        paper: '#ffffff',
                    },
                    primary: {
                        main: '#4f46e5', // Indigo-600
                    },
                }),
            },
            components: {
                MuiPaper: {
                    styleOverrides: {
                        root: {
                            backgroundImage: 'none', // Remove default gradient overlay in dark mode
                        },
                    },
                },
            },
        });
    }, [theme]);

    return (
        <MuiThemeProvider theme={muiTheme}>
            <CssBaseline />
            {children}
        </MuiThemeProvider>
    );
};
