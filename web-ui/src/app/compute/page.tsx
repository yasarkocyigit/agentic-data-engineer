
import React, { useState, useEffect, useCallback } from 'react';
import { Sidebar } from '@/components/Sidebar';
import clsx from 'clsx';
import {
    Server, Cpu, HardDrive, Network, Activity, RefreshCw,
    Circle, Database, Boxes,
    Clock, Zap, ChevronDown, ChevronRight,
    Workflow, BarChart3, GitBranch, PieChart,
    Loader2, CheckCircle2, XCircle, AlertTriangle,
    Globe, ArrowUpRight, Container, GitFork, Play, Square
} from 'lucide-react';

// ─── Types ───
type ServiceStatus = 'healthy' | 'unhealthy' | 'checking' | 'unknown';

type ServiceInfo = {
    name: string;
    containerName: string;
    image: string;
    port: string | null;
    externalPort: string | null;
    category: 'orchestration' | 'compute' | 'storage' | 'data-catalog' | 'reporting' | 'devops';
    description: string;
    uiUrl?: string;
    healthEndpoint?: string;
    status: ServiceStatus;
    responseTime?: number;
    icon: React.ReactNode;
};

type CategoryInfo = {
    label: string;
    icon: React.ReactNode;
    color: string;
    bgColor: string;
    borderColor: string;
};

// ─── Constants ───
const CATEGORIES: Record<string, CategoryInfo> = {
    'orchestration': {
        label: 'Orchestration',
        icon: <Workflow className="w-4 h-4" />,
        color: 'text-obsidian-warning',
        bgColor: 'bg-obsidian-warning/10',
        borderColor: 'border-obsidian-warning/20',
    },
    'compute': {
        label: 'Compute Engine',
        icon: <Cpu className="w-4 h-4" />,
        color: 'text-obsidian-danger',
        bgColor: 'bg-obsidian-danger/10',
        borderColor: 'border-obsidian-danger/20',
    },
    'storage': {
        label: 'Storage & Lakehouse',
        icon: <HardDrive className="w-4 h-4" />,
        color: 'text-obsidian-success',
        bgColor: 'bg-obsidian-success/10',
        borderColor: 'border-obsidian-success/20',
    },
    'data-catalog': {
        label: 'Data Catalog & Query',
        icon: <Database className="w-4 h-4" />,
        color: 'text-obsidian-info',
        bgColor: 'bg-obsidian-info/10',
        borderColor: 'border-obsidian-info/20',
    },
    'reporting': {
        label: 'Reporting & BI',
        icon: <PieChart className="w-4 h-4" />,
        color: 'text-obsidian-purple',
        bgColor: 'bg-obsidian-purple/10',
        borderColor: 'border-obsidian-purple/20',
    },
    'devops': {
        label: 'DevOps & Version Control',
        icon: <GitFork className="w-4 h-4" />,
        color: 'text-cyan-400',
        bgColor: 'bg-cyan-900/20',
        borderColor: 'border-cyan-800/30',
    },
};

const INITIAL_SERVICES: ServiceInfo[] = [
    // Orchestration
    {
        name: 'Airflow API Server',
        containerName: 'airflow_webserver',
        image: 'custom/airflow:3.x',
        port: '8080',
        externalPort: '8081',
        category: 'orchestration',
        description: 'DAG orchestration, REST API & Web UI',
        uiUrl: 'http://localhost:8081',
        healthEndpoint: 'http://localhost:8081/api/v2/monitor/health',
        status: 'checking',
        icon: <Workflow className="w-4 h-4" />,
    },
    {
        name: 'Airflow Scheduler',
        containerName: 'airflow_scheduler',
        image: 'custom/airflow:3.x',
        port: null,
        externalPort: null,
        category: 'orchestration',
        description: 'Task scheduling & execution',
        healthEndpoint: 'http://localhost:8081/api/v2/monitor/health',
        status: 'checking',
        icon: <Clock className="w-4 h-4" />,
    },
    {
        name: 'DAG Processor',
        containerName: 'airflow_dag_processor',
        image: 'custom/airflow:3.x',
        port: null,
        externalPort: null,
        category: 'orchestration',
        description: 'DAG file parsing & serialization',
        healthEndpoint: 'http://localhost:8081/api/v2/monitor/health',
        status: 'checking',
        icon: <GitBranch className="w-4 h-4" />,
    },
    // Compute
    {
        name: 'Spark Master',
        containerName: 'spark_master',
        image: 'apache/spark:4.1.1',
        port: '8080',
        externalPort: '8082',
        category: 'compute',
        description: 'Spark cluster manager & job submission',
        uiUrl: 'http://localhost:8082',
        healthEndpoint: 'http://localhost:8082',
        status: 'checking',
        icon: <Zap className="w-4 h-4" />,
    },
    {
        name: 'Spark Worker',
        containerName: 'spark_worker',
        image: 'apache/spark:4.1.1',
        port: null,
        externalPort: null,
        category: 'compute',
        description: '2 cores · 2 GB memory',
        healthEndpoint: 'http://localhost:8082',
        status: 'checking',
        icon: <Cpu className="w-4 h-4" />,
    },
    // Storage
    {
        name: 'MinIO (S3)',
        containerName: 'minio_lakehouse',
        image: 'minio/minio:latest',
        port: '9000',
        externalPort: '9001',
        category: 'storage',
        description: 'S3-compatible object storage for Iceberg',
        uiUrl: 'http://localhost:9001',
        healthEndpoint: 'http://localhost:9000/minio/health/live',
        status: 'checking',
        icon: <HardDrive className="w-4 h-4" />,
    },
    // Data Catalog & Query
    {
        name: 'Trino Coordinator',
        containerName: 'trino_sql',
        image: 'trinodb/trino:latest',
        port: '8080',
        externalPort: '8083',
        category: 'data-catalog',
        description: 'Distributed SQL query engine for Iceberg',
        uiUrl: 'http://localhost:8083',
        healthEndpoint: 'http://localhost:8083/v1/info',
        status: 'checking',
        icon: <Database className="w-4 h-4" />,
    },
    {
        name: 'Marquez API',
        containerName: 'marquez_lineage',
        image: 'marquezproject/marquez:latest',
        port: '5000',
        externalPort: '5002',
        category: 'data-catalog',
        description: 'OpenLineage data lineage tracking',
        uiUrl: 'http://localhost:5002/api/v1/namespaces',
        healthEndpoint: 'http://localhost:5002/api/v1/namespaces',
        status: 'checking',
        icon: <GitBranch className="w-4 h-4" />,
    },
    {
        name: 'Marquez Web UI',
        containerName: 'marquez_web',
        image: 'marquezproject/marquez-web:latest',
        port: '3000',
        externalPort: '8085',
        category: 'data-catalog',
        description: 'Lineage visualization dashboard',
        uiUrl: 'http://localhost:8085',
        healthEndpoint: 'http://localhost:8085',
        status: 'checking',
        icon: <BarChart3 className="w-4 h-4" />,
    },
    {
        name: 'Marquez DB',
        containerName: 'marquez_db',
        image: 'postgres:17',
        port: '5432',
        externalPort: null,
        category: 'data-catalog',
        description: 'PostgreSQL metadata store',
        healthEndpoint: 'http://localhost:5002/api/v1/namespaces',
        status: 'checking',
        icon: <Database className="w-4 h-4" />,
    },
    // Reporting & BI
    {
        name: 'Apache Superset',
        containerName: 'superset_app',
        image: 'custom/superset:latest',
        port: '8088',
        externalPort: '8089',
        category: 'reporting',
        description: 'Business intelligence & dashboards',
        uiUrl: 'http://localhost:8089',
        healthEndpoint: 'http://localhost:8089/health',
        status: 'checking',
        icon: <PieChart className="w-4 h-4" />,
    },
    {
        name: 'Superset Redis',
        containerName: 'superset_redis',
        image: 'redis:7',
        port: '6379',
        externalPort: null,
        category: 'reporting',
        description: 'Cache & message broker for Superset',
        healthEndpoint: 'http://localhost:8089/health',
        status: 'checking',
        icon: <Database className="w-4 h-4" />,
    },
    // DevOps & Version Control
    {
        name: 'Gitea Server',
        containerName: 'gitea_server',
        image: 'gitea/gitea:latest',
        port: '3000',
        externalPort: '3030',
        category: 'devops',
        description: 'Self-hosted Git repository & CI/CD',
        uiUrl: 'http://localhost:3030',
        healthEndpoint: 'http://localhost:3030',
        status: 'checking',
        icon: <GitFork className="w-4 h-4" />,
    },
    {
        name: 'Gitea Runner',
        containerName: 'gitea_runner',
        image: 'gitea/act_runner:latest',
        port: null,
        externalPort: null,
        category: 'devops',
        description: 'CI/CD pipeline execution agent',
        healthEndpoint: 'http://localhost:3030',
        status: 'checking',
        icon: <Container className="w-4 h-4" />,
    },
];

// ─── Main Component ───
export default function ComputePage() {
    const [services, setServices] = useState<ServiceInfo[]>(INITIAL_SERVICES);
    const [isRefreshing, setIsRefreshing] = useState(false);
    const [lastRefresh, setLastRefresh] = useState<Date | null>(null);
    const [expandedCategories, setExpandedCategories] = useState<Record<string, boolean>>({
        'orchestration': true,
        'compute': true,
        'storage': true,
        'data-catalog': true,
        'reporting': true,
        'devops': true,
    });
    const [selectedService, setSelectedService] = useState<string | null>(null);
    const [loadingAction, setLoadingAction] = useState<string | null>(null);

    // ─── Docker Actions ───
    const handleDockerAction = async (action: 'start' | 'stop' | 'restart', containerName: string) => {
        setLoadingAction(containerName);
        try {
            const response = await fetch(`/api/docker/${action}/${containerName}`, { method: 'POST' });
            if (response.ok) {
                // Wait briefly for container to process state, then check health
                setTimeout(() => checkHealth(), 1500);
            } else {
                console.error(`Failed to ${action} ${containerName}`);
            }
        } catch (error) {
            console.error(error);
        } finally {
            setLoadingAction(null);
        }
    };

    // ─── Health Check ───
    const checkHealth = useCallback(async () => {
        setIsRefreshing(true);

        const updatedServices = await Promise.all(
            services.map(async (service) => {
                if (!service.healthEndpoint) {
                    return { ...service, status: 'unknown' as ServiceStatus };
                }

                const startTime = performance.now();
                try {
                    const response = await fetch('/api/health-check', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ url: service.healthEndpoint }),
                    });
                    const endTime = performance.now();
                    const data = await response.json();

                    return {
                        ...service,
                        status: data.healthy ? 'healthy' as ServiceStatus : 'unhealthy' as ServiceStatus,
                        responseTime: Math.round(endTime - startTime),
                    };
                } catch {
                    return {
                        ...service,
                        status: 'unhealthy' as ServiceStatus,
                        responseTime: undefined,
                    };
                }
            })
        );

        setServices(updatedServices);
        setIsRefreshing(false);
        setLastRefresh(new Date());
    }, [services]);

    // Initial health check
    useEffect(() => {
        checkHealth();
        // Auto-refresh every 30s
        const interval = setInterval(checkHealth, 30000);
        return () => clearInterval(interval);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    // ─── Computed Stats ───
    const healthyCount = services.filter(s => s.status === 'healthy').length;
    const unhealthyCount = services.filter(s => s.status === 'unhealthy').length;
    const unknownCount = services.filter(s => s.status === 'unknown' || s.status === 'checking').length;
    const totalCount = services.length;

    const categoryGroups = Object.entries(CATEGORIES).map(([key, info]) => ({
        key,
        info,
        services: services.filter(s => s.category === key),
    }));

    const toggleCategory = (key: string) => {
        setExpandedCategories(prev => ({ ...prev, [key]: !prev[key] }));
    };

    const statusIcon = (status: ServiceStatus) => {
        switch (status) {
            case 'healthy': return <CheckCircle2 className="w-3.5 h-3.5 text-obsidian-success" />;
            case 'unhealthy': return <XCircle className="w-3.5 h-3.5 text-obsidian-danger" />;
            case 'checking': return <Loader2 className="w-3.5 h-3.5 text-obsidian-warning animate-spin" />;
            default: return <AlertTriangle className="w-3.5 h-3.5 text-obsidian-muted" />;
        }
    };

    const statusLabel = (status: ServiceStatus, hasPort: boolean) => {
        switch (status) {
            case 'healthy': return <span className="text-obsidian-success">{hasPort ? 'Healthy' : 'Running'}</span>;
            case 'unhealthy': return <span className="text-obsidian-danger">Down</span>;
            case 'checking': return <span className="text-obsidian-warning">Checking...</span>;
            default: return <span className="text-obsidian-muted">Unknown</span>;
        }
    };

    return (
        <div className="flex h-screen bg-obsidian-bg text-foreground font-sans overflow-hidden">
            <Sidebar />
            <main className="flex-1 flex flex-col min-w-0 overflow-hidden">

                {/* ─── Top Header ─── */}
                <header className="h-9 bg-obsidian-panel border-b border-obsidian-border flex items-center justify-between px-4 shrink-0">
                    <div className="flex items-center gap-2">
                        <Server className="w-3.5 h-3.5 text-obsidian-info" />
                        <span className="text-[12px] font-bold text-foreground">Compute & Infrastructure</span>
                        <span className="text-[10px] text-obsidian-muted ml-2">Docker Compose Stack</span>
                    </div>
                    <div className="flex items-center gap-3">
                        {lastRefresh && (
                            <span className="text-[10px] text-obsidian-muted">
                                Updated {lastRefresh.toLocaleTimeString()}
                            </span>
                        )}
                        <button
                            onClick={checkHealth}
                            disabled={isRefreshing}
                            className={clsx(
                                "flex items-center gap-1.5 px-2.5 py-1 rounded text-[11px] font-medium transition-all",
                                isRefreshing
                                    ? "bg-obsidian-muted/20 text-obsidian-muted cursor-not-allowed"
                                    : "bg-obsidian-success/20 text-obsidian-success hover:bg-obsidian-panel-hover"
                            )}
                        >
                            <RefreshCw className={clsx("w-3 h-3", isRefreshing && "animate-spin")} />
                            {isRefreshing ? 'Checking...' : 'Refresh'}
                        </button>
                    </div>
                </header>

                {/* ─── Overview Cards ─── */}
                <div className="bg-obsidian-panel border-b border-obsidian-border shrink-0">
                    <div className="grid grid-cols-4 divide-x divide-obsidian-border">
                        {/* Total Services */}
                        <div className="p-4">
                            <div className="flex items-center gap-2 text-[10px] text-obsidian-muted uppercase tracking-wider font-bold mb-2">
                                <Boxes className="w-3.5 h-3.5" /> Total Services
                            </div>
                            <div className="text-3xl font-mono font-bold text-foreground">{totalCount}</div>
                            <div className="text-[10px] text-obsidian-muted mt-1">containers managed</div>
                        </div>

                        {/* Healthy */}
                        <div className="p-4">
                            <div className="flex items-center gap-2 text-[10px] text-obsidian-success uppercase tracking-wider font-bold mb-2">
                                <CheckCircle2 className="w-3.5 h-3.5" /> Healthy
                            </div>
                            <div className="text-3xl font-mono font-bold text-obsidian-success">{healthyCount}</div>
                            <div className="w-full bg-obsidian-panel-hover h-1.5 mt-2 rounded-full overflow-hidden">
                                <div
                                    className="bg-obsidian-success h-full rounded-full transition-all duration-500"
                                    style={{ width: `${totalCount > 0 ? (healthyCount / totalCount) * 100 : 0}%` }}
                                />
                            </div>
                        </div>

                        {/* Unhealthy */}
                        <div className="p-4">
                            <div className="flex items-center gap-2 text-[10px] text-obsidian-danger uppercase tracking-wider font-bold mb-2">
                                <XCircle className="w-3.5 h-3.5" /> Unhealthy
                            </div>
                            <div className="text-3xl font-mono font-bold text-obsidian-danger">{unhealthyCount}</div>
                            <div className="w-full bg-obsidian-panel-hover h-1.5 mt-2 rounded-full overflow-hidden">
                                <div
                                    className="bg-obsidian-danger h-full rounded-full transition-all duration-500"
                                    style={{ width: `${totalCount > 0 ? (unhealthyCount / totalCount) * 100 : 0}%` }}
                                />
                            </div>
                        </div>

                        {/* Network */}
                        <div className="p-4">
                            <div className="flex items-center gap-2 text-[10px] text-obsidian-muted uppercase tracking-wider font-bold mb-2">
                                <Network className="w-3.5 h-3.5" /> Docker Network
                            </div>
                            <div className="text-xl font-mono font-bold text-foreground flex items-center gap-2">
                                <Circle className="w-2.5 h-2.5 fill-obsidian-success text-obsidian-success" />
                                Active
                            </div>
                            <div className="text-[10px] text-obsidian-muted mt-2 font-mono">bridge: agentic-network</div>
                        </div>
                    </div>
                </div>

                {/* ─── Service Groups ─── */}
                <div className="flex-1 overflow-auto">
                    {categoryGroups.map(({ key, info, services: catServices }) => (
                        <div key={key} className="border-b border-obsidian-border">
                            {/* Category Header */}
                            <div
                                onClick={() => toggleCategory(key)}
                                className="h-8 bg-obsidian-panel border-b border-obsidian-border flex items-center px-3 gap-2 cursor-pointer hover:bg-obsidian-panel-header transition-colors select-none group"
                            >
                                {expandedCategories[key] ? (
                                    <ChevronDown className="w-3.5 h-3.5 text-obsidian-muted" />
                                ) : (
                                    <ChevronRight className="w-3.5 h-3.5 text-obsidian-muted" />
                                )}
                                <div className={clsx("flex items-center gap-1.5", info.color)}>
                                    {info.icon}
                                    <span className="text-[11px] font-bold uppercase tracking-wider">{info.label}</span>
                                </div>
                                <div className="flex items-center gap-1.5 ml-2">
                                    <span className={clsx(
                                        "text-[9px] font-mono px-1.5 py-0.5 rounded",
                                        info.bgColor, info.borderColor, "border", info.color
                                    )}>
                                        {catServices.filter(s => s.status === 'healthy').length}/{catServices.length}
                                    </span>
                                </div>
                                <div className="ml-auto flex items-center gap-2">
                                    {catServices.map(s => (
                                        <div key={s.containerName} title={s.name}>
                                            <Circle className={clsx(
                                                "w-2 h-2",
                                                s.status === 'healthy' ? "fill-obsidian-success text-obsidian-success" :
                                                    s.status === 'unhealthy' ? "fill-obsidian-danger text-obsidian-danger" :
                                                        s.status === 'checking' ? "fill-obsidian-warning text-obsidian-warning animate-pulse" :
                                                            "fill-obsidian-muted text-obsidian-muted"
                                            )} />
                                        </div>
                                    ))}
                                </div>
                            </div>

                            {/* Services Table */}
                            {expandedCategories[key] && (
                                <table className="w-full text-left border-collapse">
                                    <thead className="bg-obsidian-panel-header">
                                        <tr>
                                            <th className="p-1.5 px-3 border-r border-b border-obsidian-border w-8 text-center text-[10px] text-obsidian-muted">
                                                <Activity className="w-3.5 h-3.5 inline" />
                                            </th>
                                            <th className="p-1.5 px-3 border-r border-b border-obsidian-border text-[10px] text-obsidian-muted font-medium uppercase tracking-wider">Service</th>
                                            <th className="p-1.5 px-3 border-r border-b border-obsidian-border text-[10px] text-obsidian-muted font-medium uppercase tracking-wider">Container</th>
                                            <th className="p-1.5 px-3 border-r border-b border-obsidian-border text-[10px] text-obsidian-muted font-medium uppercase tracking-wider">Image</th>
                                            <th className="p-1.5 px-3 border-r border-b border-obsidian-border text-[10px] text-obsidian-muted font-medium uppercase tracking-wider w-20">Port</th>
                                            <th className="p-1.5 px-3 border-r border-b border-obsidian-border text-[10px] text-obsidian-muted font-medium uppercase tracking-wider w-24">Status</th>
                                            <th className="p-1.5 px-3 border-r border-b border-obsidian-border text-[10px] text-obsidian-muted font-medium uppercase tracking-wider w-16">Latency</th>
                                            <th className="p-1.5 px-3 border-b border-obsidian-border text-[10px] text-obsidian-muted font-medium uppercase tracking-wider w-16 text-center">Actions</th>
                                        </tr>
                                    </thead>
                                    <tbody className="font-mono text-[12px]">
                                        {catServices.map((service) => (
                                            <tr
                                                key={service.containerName}
                                                onClick={() => setSelectedService(
                                                    selectedService === service.containerName ? null : service.containerName
                                                )}
                                                className={clsx(
                                                    "hover:bg-obsidian-panel transition-colors cursor-pointer group",
                                                    selectedService === service.containerName && "bg-obsidian-panel"
                                                )}
                                            >
                                                <td className="p-1.5 border-r border-b border-obsidian-border text-center">
                                                    {statusIcon(service.status)}
                                                </td>
                                                <td className="p-1.5 px-3 border-r border-b border-obsidian-border">
                                                    <div className="flex items-center gap-2">
                                                        <span className={clsx(info.color)}>{service.icon}</span>
                                                        <div>
                                                            <div className="text-foreground font-medium text-[12px]">{service.name}</div>
                                                            <div className="text-[10px] text-obsidian-muted">{service.description}</div>
                                                        </div>
                                                    </div>
                                                </td>
                                                <td className="p-1.5 px-3 border-r border-b border-obsidian-border text-foreground text-[11px]">
                                                    {service.containerName}
                                                </td>
                                                <td className="p-1.5 px-3 border-r border-b border-obsidian-border text-obsidian-muted text-[11px]">
                                                    {service.image}
                                                </td>
                                                <td className="p-1.5 px-3 border-r border-b border-obsidian-border">
                                                    {service.externalPort ? (
                                                        <span className="text-foreground text-[11px]">:{service.externalPort}</span>
                                                    ) : (
                                                        <span className="text-obsidian-muted text-[10px] italic">internal</span>
                                                    )}
                                                </td>
                                                <td className="p-1.5 px-3 border-r border-b border-obsidian-border text-[11px]">
                                                    {statusLabel(service.status, !!service.externalPort)}
                                                </td>
                                                <td className="p-1.5 px-3 border-r border-b border-obsidian-border text-[11px]">
                                                    {service.responseTime ? (
                                                        <span className={clsx(
                                                            service.responseTime < 100 ? "text-obsidian-success" :
                                                                service.responseTime < 500 ? "text-obsidian-warning" :
                                                                    "text-obsidian-danger"
                                                        )}>
                                                            {service.responseTime}ms
                                                        </span>
                                                    ) : (
                                                        <span className="text-obsidian-muted">—</span>
                                                    )}
                                                </td>
                                                <td className="p-1.5 px-3 border-b border-obsidian-border">
                                                    <div className="flex items-center justify-end gap-1">
                                                        {service.uiUrl && (
                                                            <a
                                                                href={service.uiUrl}
                                                                target="_blank"
                                                                rel="noopener noreferrer"
                                                                onClick={(e) => e.stopPropagation()}
                                                                className="btn-icon w-6 h-6 hover:bg-obsidian-muted/20 text-obsidian-muted hover:text-foreground active:scale-95"
                                                                title={`Open ${service.name} UI`}
                                                            >
                                                                <ArrowUpRight className="w-3.5 h-3.5" />
                                                            </a>
                                                        )}
                                                        <button
                                                            onClick={(e) => { e.stopPropagation(); handleDockerAction(service.status === 'healthy' ? 'restart' : 'start', service.containerName); }}
                                                            disabled={loadingAction === service.containerName}
                                                            className="btn-icon w-6 h-6 hover:bg-obsidian-muted/20 text-obsidian-muted hover:text-foreground active:scale-95"
                                                            title={service.status === 'healthy' ? 'Restart Container' : 'Start Container'}
                                                        >
                                                            {loadingAction === service.containerName ? (
                                                                <Loader2 className="w-3.5 h-3.5 animate-spin text-obsidian-info" />
                                                            ) : service.status === 'healthy' ? (
                                                                <RefreshCw className="w-3.5 h-3.5" />
                                                            ) : (
                                                                <Play className="w-3.5 h-3.5 text-obsidian-success fill-obsidian-success/20" />
                                                            )}
                                                        </button>
                                                        {service.status === 'healthy' && (
                                                            <button
                                                                onClick={(e) => { e.stopPropagation(); handleDockerAction('stop', service.containerName); }}
                                                                disabled={loadingAction === service.containerName}
                                                                className="btn-icon w-6 h-6 hover:bg-obsidian-danger/20 text-obsidian-muted hover:text-obsidian-danger active:scale-95"
                                                                title="Stop Container"
                                                            >
                                                                <Square className="w-3.5 h-3.5" />
                                                            </button>
                                                        )}
                                                    </div>
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            )}
                        </div>
                    ))}
                </div>

                {/* ─── Status Bar ─── */}
                <div className="h-6 bg-obsidian-panel border-t border-obsidian-border flex items-center px-3 text-[10px] text-obsidian-muted gap-4 shrink-0">
                    <div className="flex items-center gap-1">
                        <Boxes className="w-3.5 h-3.5" />
                        <span className="text-foreground">{totalCount}</span> services
                    </div>
                    <div className="flex items-center gap-1">
                        <CheckCircle2 className="w-3.5 h-3.5 text-obsidian-success" />
                        <span className="text-obsidian-success">{healthyCount}</span> healthy
                    </div>
                    {unhealthyCount > 0 && (
                        <div className="flex items-center gap-1">
                            <XCircle className="w-3.5 h-3.5 text-obsidian-danger" />
                            <span className="text-obsidian-danger">{unhealthyCount}</span> down
                        </div>
                    )}
                    {unknownCount > 0 && (
                        <div className="flex items-center gap-1">
                            <AlertTriangle className="w-3.5 h-3.5" />
                            <span>{unknownCount}</span> unknown
                        </div>
                    )}
                    <div className="ml-auto flex items-center gap-1 text-obsidian-muted">
                        <Globe className="w-3.5 h-3.5" />
                        <span>agentic-network</span>
                        <span className="mx-1">·</span>
                        <span>Auto-refresh: 30s</span>
                    </div>
                </div>
            </main>
        </div>
    );
}
