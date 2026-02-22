import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import './app/globals.css';

// ─── Page Imports ───
import Home from './app/page';
import DataPage from './app/data/page';
import ComputePage from './app/compute/page';
import LineagePage from './app/lineage/page';
import StoragePage from './app/storage/page';
import WorkflowsPage from './app/workflows/page';
import CicdPage from './app/cicd/page';
import AgentPage from './app/agent/page';
import VisualizePage from './app/visualize/page';
import NotebooksPage from './app/notebooks/page';
import DockerCliPage from './app/docker-cli/page';

ReactDOM.createRoot(document.getElementById('root')!).render(
    <React.StrictMode>
        <BrowserRouter>
            <Routes>
                <Route path="/" element={<Home />} />
                <Route path="/data" element={<DataPage />} />
                <Route path="/compute" element={<ComputePage />} />
                <Route path="/lineage" element={<LineagePage />} />
                <Route path="/storage" element={<StoragePage />} />
                <Route path="/workflows" element={<WorkflowsPage />} />
                <Route path="/cicd" element={<CicdPage />} />
                <Route path="/agent" element={<AgentPage />} />
                <Route path="/visualize" element={<VisualizePage />} />
                <Route path="/notebooks" element={<NotebooksPage />} />
                <Route path="/docker-cli" element={<DockerCliPage />} />
            </Routes>
        </BrowserRouter>
    </React.StrictMode>
);
