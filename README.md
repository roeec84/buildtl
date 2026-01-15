# AI-Powered BI Platform

A next-generation Business Intelligence platform integrating **GenAI** for natural language analytics, automated **ETL pipelines**, and dynamic **Chart generation**.

## Key Features

### Generative AI Analytics
*   **Natural Language to SQL**: Ask questions in plain English (e.g., "What were the sales last month?").
*   **Automated Visualization**: The AI automatically selects and configures the best chart (Bar, Line, Pie, etc.) for your data.
*   **Strategy Pattern Architecture**: Modular LLM backend supporting **OpenAI (GPT-4)** and **Anthropic (Claude)**, easily extensible to other providers.

### Intelligent ETL Pipelines
*   **Visual Pipeline Builder**: Create complex data workflows with a node-based interface.
*   **GenAI Transformations**: Describe your data transformation in English, and the system generates the PySpark code for you.
*   **Multi-Source Support**: Connect to PostgreSQL, SQLServer, BigQuery, and more.

![ETL Pipeline](docs/images/etl_pipeline_ui.png)

### Interactive Dashboard
   **Dynamic Layouts**: Drag-and-drop widgets to arrange your workspace.


![Chat Analytics](docs/images/ai_chat_analytics.png)

## Technology Stack

*   **Backend**: Python, FastAPI, LangChain, LangGraph, SQLAlchemy, Pydantic.
*   **Frontend**: React, TypeScript, Vite, TailwindCSS, MUI.
*   **Database**: PostgreSQL (Relational), ChromaDB (Vector Store).
*   **Containerization**: Docker, Docker Compose.

## Getting Started

### Prerequisites
*   Docker & Docker Compose
*   OpenAI/Anthropic API Key (To create a model in the settings)

### Installation

1.  **Clone the repository**
    ```bash
    git clone https://github.com/roeec84/buildtl.git
    ```

2.  **Environment Setup**
    Create a `.env` file in the `backend` directory:
    ```bash
    DATABASE_URL=postgresql+asyncpg://user:password@postgres:5432/db
    SECRET_KEY=your-secret-key-use-openssl-rand-hex-32
    ```

3.  **Run with Docker**
    ```bash
    docker-compose up -d --build
    ```

4.  **Access the App**
    *   Frontend: `http://localhost:5173`
    *   Backend API: `http://localhost:8000/`
