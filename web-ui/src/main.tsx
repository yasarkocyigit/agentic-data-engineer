import React, { Suspense, lazy } from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import './app/globals.css';

const Home = lazy(() => import('./app/page'));
const DataPage = lazy(() => import('./app/data/page'));
const ComputePage = lazy(() => import('./app/compute/page'));
const LineagePage = lazy(() => import('./app/lineage/page'));
const StoragePage = lazy(() => import('./app/storage/page'));
const WorkflowsPage = lazy(() => import('./app/workflows/page'));
const CicdPage = lazy(() => import('./app/cicd/page'));
const AgentPage = lazy(() => import('./app/agent/page'));
const VisualizePage = lazy(() => import('./app/visualize/page'));
const NotebooksPage = lazy(() => import('./app/notebooks/page'));
const DockerCliPage = lazy(() => import('./app/docker-cli/page'));

ReactDOM.createRoot(document.getElementById('root')!).render(
    <React.StrictMode>
        <BrowserRouter>
            <Suspense fallback={<div className="h-screen w-screen bg-[#09090b]" />}>
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
            </Suspense>
        </BrowserRouter>
    </React.StrictMode>
);
