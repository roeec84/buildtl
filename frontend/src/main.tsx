import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'
import { ThemeProvider } from './contexts/ThemeContext'

import { MuiThemeProviderWrapper } from './providers/MuiThemeProviderWrapper'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <ThemeProvider defaultTheme="dark" storageKey="vite-ui-theme">
      <MuiThemeProviderWrapper>
        <App />
      </MuiThemeProviderWrapper>
    </ThemeProvider>
  </StrictMode>,
)
