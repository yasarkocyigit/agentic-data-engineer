
import React, { useState, useEffect, useCallback } from 'react';
import { Sidebar } from '@/components/Sidebar';
import clsx from 'clsx';
import {
    Server, Cpu, HardDrive, Network, Activity, RefreshCw,
    Circle, Database, Boxes,
    Clock, Zap, ChevronDown, ChevronRight,
    Workflow, BarChart3, GitBranch, PieChart,
    Loader2, CheckCircle2, XCircle, AlertTriangle,
    Globe, ArrowUpRight, Container, GitFork, Play, Square, LayoutPanelLeft
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
        color: 'text-foreground/80',
        bgColor: 'bg-white/[0.02]',
        borderColor: 'border-white/5',
    },
    'compute': {
        label: 'Compute Engine',
        icon: <Cpu className="w-4 h-4" />,
        color: 'text-foreground/80',
        bgColor: 'bg-white/[0.02]',
        borderColor: 'border-white/5',
    },
    'storage': {
        label: 'Storage & Lakehouse',
        icon: <HardDrive className="w-4 h-4" />,
        color: 'text-foreground/80',
        bgColor: 'bg-white/[0.02]',
        borderColor: 'border-white/5',
    },
    'data-catalog': {
        label: 'Data Catalog & Query',
        icon: <Database className="w-4 h-4" />,
        color: 'text-foreground/80',
        bgColor: 'bg-white/[0.02]',
        borderColor: 'border-white/5',
    },
    'reporting': {
        label: 'Reporting & BI',
        icon: <PieChart className="w-4 h-4" />,
        color: 'text-foreground/80',
        bgColor: 'bg-white/[0.02]',
        borderColor: 'border-white/5',
    },
    'devops': {
        label: 'DevOps & Version Control',
        icon: <GitFork className="w-4 h-4" />,
        color: 'text-foreground/80',
        bgColor: 'bg-white/[0.02]',
        borderColor: 'border-white/5',
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
        healthEndpoint: 'http://airflow-webserver:8080/api/v2/monitor/health',
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
        healthEndpoint: 'docker://airflow_scheduler',
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
        healthEndpoint: 'docker://airflow_dag_processor',
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
        healthEndpoint: 'http://spark-master:8080',
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
        healthEndpoint: 'docker://spark_worker',
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
        healthEndpoint: 'http://minio:9000/minio/health/live',
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
        healthEndpoint: 'http://trino:8080/v1/info',
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
        healthEndpoint: 'http://marquez:5000/api/v1/namespaces',
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
        healthEndpoint: 'http://marquez-web:3000',
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
        healthEndpoint: 'docker://marquez_db',
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
        healthEndpoint: 'http://superset:8088/health',
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
        healthEndpoint: 'docker://superset_redis',
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
        healthEndpoint: 'http://gitea:3000',
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
        healthEndpoint: 'docker://gitea_runner',
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

    // Stats State
    const [dockerStats, setDockerStats] = useState<{
        cpu: { used_percent: number, total_percent: number, cores: number },
        memory: { used_gb: number, total_gb: number }
    } | null>(null);

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

        try {
            const statsRes = await fetch('/api/docker/stats');
            if (statsRes.ok) {
                const statsData = await statsRes.json();
                if (statsData.status === 'success') {
                    setDockerStats({
                        cpu: statsData.cpu,
                        memory: statsData.memory
                    });
                }
            }
        } catch (e) {
            console.error("Failed to fetch docker stats", e);
        }

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
            case 'healthy': return <CheckCircle2 className="w-3.5 h-3.5 text-white/40" />;
            case 'unhealthy': return <XCircle className="w-3.5 h-3.5 text-obsidian-danger/80" />;
            case 'checking': return <Loader2 className="w-3.5 h-3.5 text-white/40 animate-spin" />;
            default: return <AlertTriangle className="w-3.5 h-3.5 text-obsidian-muted/50" />;
        }
    };

    const statusLabel = (status: ServiceStatus, hasPort: boolean) => {
        switch (status) {
            case 'healthy': return <span className="text-white/60">{hasPort ? 'Healthy' : 'Running'}</span>;
            case 'unhealthy': return <span className="text-obsidian-danger/80">Down</span>;
            case 'checking': return <span className="text-white/40">Checking...</span>;
            default: return <span className="text-white/30">Unknown</span>;
        }
    };

    return (
        <div className="flex h-screen bg-[#09090b] text-foreground font-sans overflow-hidden relative" style={{ fontFamily: "'Inter', -apple-system, sans-serif" }}>

            {/* Ambient Lighting */}
            <div className="absolute top-0 left-0 w-[800px] h-[800px] bg-purple-500/5 rounded-full blur-[120px] pointer-events-none -translate-x-1/4 -translate-y-1/4 z-0" />
            <div className="absolute bottom-0 right-0 w-[600px] h-[600px] bg-sky-500/5 rounded-full blur-[100px] pointer-events-none translate-x-1/4 -translate-y-1/4 z-0" />

            <div className="relative z-10 shrink-0">
                <Sidebar />
            </div>

            <main className="flex-1 flex flex-col min-w-0 bg-transparent relative z-10">

                <header className="flex items-center px-4 justify-between shrink-0 h-10 bg-black/40 backdrop-blur-md border-b border-white/5 z-10 w-full relative">
                    <div className="absolute top-0 left-0 right-0 h-[1px] bg-gradient-to-r from-transparent via-obsidian-info/30 to-transparent opacity-50"></div>
                    <div className="flex items-center gap-3">
                        <button
                            onClick={() => window.dispatchEvent(new CustomEvent('openclaw:toggle-sidebar'))}
                            className="p-1.5 hover:bg-white/10 rounded-md text-obsidian-muted hover:text-white transition-all active:scale-95 border border-transparent hover:border-obsidian-border/50"
                            title="Toggle Explorer"
                        >
                            <LayoutPanelLeft className="w-4 h-4 drop-shadow-[0_0_8px_rgba(255,255,255,0.2)]" />
                        </button>
                        <div className="w-[1px] h-4 bg-obsidian-border/50"></div>
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
                                    ? "bg-white/[0.02] text-obsidian-muted cursor-not-allowed"
                                    : "bg-white/[0.02] border border-white/5 text-obsidian-muted hover:text-white hover:bg-white/[0.05]"
                            )}
                        >
                            <RefreshCw className={clsx("w-3 h-3", isRefreshing && "animate-spin")} />
                            {isRefreshing ? 'Checking...' : 'Refresh'}
                        </button>
                    </div>
                </header>

                {/* ─── Overview Cards ─── */}
                <div className="bg-black/20 backdrop-blur-xl border-b border-white/5 shrink-0 z-10 relative">
                    <div className="grid grid-cols-6 divide-x divide-white/5">
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
                            <div className="flex items-center gap-2 text-[10px] text-obsidian-muted uppercase tracking-wider font-bold mb-2">
                                <CheckCircle2 className="w-3.5 h-3.5 text-white/40" /> Healthy
                            </div>
                            <div className="text-3xl font-mono font-bold text-foreground">{healthyCount}</div>
                        </div>

                        {/* Unhealthy */}
                        <div className="p-4">
                            <div className="flex items-center gap-2 text-[10px] text-obsidian-muted uppercase tracking-wider font-bold mb-2">
                                <XCircle className="w-3.5 h-3.5 text-obsidian-danger/60" /> Unhealthy
                            </div>
                            <div className="text-3xl font-mono font-bold text-foreground">{unhealthyCount}</div>
                        </div>

                        {/* Network */}
                        <div className="p-4">
                            <div className="flex items-center gap-2 text-[10px] text-obsidian-muted uppercase tracking-wider font-bold mb-2">
                                <Network className="w-3.5 h-3.5" /> Docker Network
                            </div>
                            <div className="text-xl font-mono font-bold text-foreground flex items-center gap-2">
                                <Circle className="w-2.5 h-2.5 fill-white/20 text-white/20" />
                                Active
                            </div>
                            <div className="text-[10px] text-obsidian-muted mt-2 font-mono">bridge: agentic-network</div>
                        </div>

                        {/* Container CPU Usage */}
                        <div className="p-4 flex flex-col justify-between">
                            <div>
                                <div className="flex items-center gap-2 text-[10px] text-obsidian-muted uppercase tracking-wider font-bold mb-3 whitespace-nowrap">
                                    <Cpu className="w-3.5 h-3.5 shrink-0" /> CPU usage
                                </div>
                                <div className="text-3xl font-mono font-medium">
                                    {dockerStats ? (
                                        <div className="flex items-baseline gap-1.5">
                                            <span className="text-foreground">
                                                {dockerStats.cpu.used_percent}%
                                            </span>
                                            <span className="text-sm text-obsidian-muted font-normal">
                                                / {dockerStats.cpu.total_percent}%
                                            </span>
                                        </div>
                                    ) : (
                                        <span className="text-obsidian-muted text-sm font-normal">Loading...</span>
                                    )}
                                </div>
                                <div className="text-[10px] text-obsidian-muted mt-2 font-mono">
                                    {dockerStats ? `(${dockerStats.cpu.cores} CPUs available)` : '---'}
                                </div>
                            </div>

                            {/* CPU Tracking Bar (Minimal) */}
                            {dockerStats && (
                                <div className="w-full bg-black/40 border border-white/5 h-[3px] mt-4 overflow-hidden rounded-full">
                                    <div
                                        className="bg-white/40 h-full transition-all duration-1000 rounded-full"
                                        style={{ width: `${Math.min(100, (dockerStats.cpu.used_percent / dockerStats.cpu.total_percent) * 100)}%` }}
                                    />
                                </div>
                            )}
                        </div>

                        {/* Container Memory Usage */}
                        <div className="p-4 flex flex-col justify-between">
                            <div>
                                <div className="flex items-center gap-2 text-[10px] text-obsidian-muted uppercase tracking-wider font-bold mb-3 whitespace-nowrap">
                                    <Activity className="w-3.5 h-3.5 shrink-0" /> Memory usage
                                </div>
                                <div className="text-3xl font-mono font-medium">
                                    {dockerStats ? (
                                        <div className="flex items-baseline gap-1.5">
                                            <span className="text-foreground">
                                                {dockerStats.memory.used_gb}
                                                <span className="text-[16px] ml-0.5 text-obsidian-muted">GB</span>
                                            </span>
                                            <span className="text-sm text-obsidian-muted font-normal">
                                                / {dockerStats.memory.total_gb}GB
                                            </span>
                                        </div>
                                    ) : (
                                        <span className="text-obsidian-muted text-sm font-normal">Loading...</span>
                                    )}
                                </div>
                                <div className="text-[10px] text-obsidian-muted mt-2 font-mono hover:text-foreground transition-colors cursor-default" title="Refresh stats to update">
                                    {dockerStats && dockerStats.memory.total_gb > 0 ? `${((dockerStats.memory.used_gb / dockerStats.memory.total_gb) * 100).toFixed(1)}% utilized` : '---'}
                                </div>
                            </div>

                            {/* Memory Tracking Bar (Minimal) */}
                            {dockerStats && dockerStats.memory.total_gb > 0 && (
                                <div className="w-full bg-black/40 border border-white/5 h-[3px] mt-4 overflow-hidden rounded-full">
                                    <div
                                        className="bg-white/40 h-full transition-all duration-1000 rounded-full"
                                        style={{ width: `${Math.min(100, (dockerStats.memory.used_gb / dockerStats.memory.total_gb) * 100)}%` }}
                                    />
                                </div>
                            )}
                        </div>
                    </div>
                </div>

                {/* ─── Service Groups ─── */}
                <div className="flex-1 overflow-auto bg-transparent z-10 relative custom-scrollbar">
                    {categoryGroups.map(({ key, info, services: catServices }) => (
                        <div key={key} className="border-b border-white/5">
                            {/* Category Header */}
                            <div
                                onClick={() => toggleCategory(key)}
                                className="h-10 bg-transparent hover:bg-white/[0.02] transition-colors flex items-center px-4 gap-3 cursor-pointer select-none group"
                            >
                                {expandedCategories[key] ? (
                                    <ChevronDown className="w-4 h-4 text-obsidian-muted group-hover:text-white transition-colors" />
                                ) : (
                                    <ChevronRight className="w-4 h-4 text-obsidian-muted group-hover:text-white transition-colors" />
                                )}
                                <div className={clsx("flex items-center gap-2", info.color)}>
                                    {info.icon}
                                    <span className="text-xs font-medium uppercase tracking-widest">{info.label}</span>
                                </div>
                                <div className="flex items-center gap-1.5 ml-3">
                                    <span className="text-[10px] font-mono px-2 py-0.5 rounded-full bg-white/[0.02] border border-white/5 text-obsidian-muted">
                                        {catServices.filter(s => s.status === 'healthy').length}/{catServices.length}
                                    </span>
                                </div>
                                <div className="ml-auto flex items-center gap-1.5 opacity-60 mix-blend-screen">
                                    {catServices.map(s => (
                                        <div key={s.containerName} title={s.name}>
                                            <Circle className={clsx(
                                                "w-1.5 h-1.5",
                                                s.status === 'healthy' ? "fill-white/30 text-white/30" :
                                                    s.status === 'unhealthy' ? "fill-obsidian-danger/60 text-obsidian-danger/60" :
                                                        s.status === 'checking' ? "fill-white/20 text-white/20 animate-pulse" :
                                                            "fill-white/10 text-white/10"
                                            )} />
                                        </div>
                                    ))}
                                </div>
                            </div>

                            {/* Services Table */}
                            {expandedCategories[key] && (
                                <table className="w-full text-left border-collapse">
                                    <thead className="bg-black/20 border-b border-white/5">
                                        <tr>
                                            <th className="p-2 px-4 border-b border-white/5 w-10 text-center text-[10px] text-obsidian-muted/50">
                                                <Activity className="w-3.5 h-3.5 inline" />
                                            </th>
                                            <th className="p-2 px-4 border-b border-white/5 text-[10px] text-obsidian-muted/70 font-medium uppercase tracking-wider">Service</th>
                                            <th className="p-2 px-4 border-b border-white/5 text-[10px] text-obsidian-muted/70 font-medium uppercase tracking-wider">Container</th>
                                            <th className="p-2 px-4 border-b border-white/5 text-[10px] text-obsidian-muted/70 font-medium uppercase tracking-wider">Image</th>
                                            <th className="p-2 px-4 border-b border-white/5 text-[10px] text-obsidian-muted/70 font-medium uppercase tracking-wider w-24">Port</th>
                                            <th className="p-2 px-4 border-b border-white/5 text-[10px] text-obsidian-muted/70 font-medium uppercase tracking-wider w-24">Status</th>
                                            <th className="p-2 px-4 border-b border-white/5 text-[10px] text-obsidian-muted/70 font-medium uppercase tracking-wider w-20">Latency</th>
                                            <th className="p-2 px-4 border-b border-white/5 text-[10px] text-obsidian-muted/70 font-medium uppercase tracking-wider w-20 text-center">Actions</th>
                                        </tr>
                                    </thead>
                                    <tbody className="font-mono text-xs">
                                        {catServices.map((service) => (
                                            <tr
                                                key={service.containerName}
                                                onClick={() => setSelectedService(
                                                    selectedService === service.containerName ? null : service.containerName
                                                )}
                                                className={clsx(
                                                    "hover:bg-white/[0.02] transition-colors cursor-pointer group border-b border-white/5 last:border-none",
                                                    selectedService === service.containerName ? "bg-white/[0.04]" : "bg-transparent"
                                                )}
                                            >
                                                <td className="p-2 border-r border-white/5 text-center">
                                                    {statusIcon(service.status)}
                                                </td>
                                                <td className="p-2 px-4 border-r border-white/5">
                                                    <div className="flex items-center gap-3">
                                                        <span className={clsx(info.color, "opacity-80 group-hover:opacity-100 transition-opacity")}>{service.icon}</span>
                                                        <div>
                                                            <div className="text-foreground/90 font-medium text-xs tracking-wide">{service.name}</div>
                                                            <div className="text-[10px] text-obsidian-muted font-sans mt-0.5">{service.description}</div>
                                                        </div>
                                                    </div>
                                                </td>
                                                <td className="p-2 px-4 border-r border-white/5 text-foreground/70">
                                                    {service.containerName}
                                                </td>
                                                <td className="p-2 px-4 border-r border-white/5 text-obsidian-muted/80">
                                                    {service.image}
                                                </td>
                                                <td className="p-2 px-4 border-r border-white/5">
                                                    {service.externalPort ? (
                                                        <span className="text-foreground/80">:{service.externalPort}</span>
                                                    ) : (
                                                        <span className="text-obsidian-muted/50 text-[10px] italic">internal</span>
                                                    )}
                                                </td>
                                                <td className="p-2 px-4 border-r border-white/5">
                                                    {statusLabel(service.status, !!service.externalPort)}
                                                </td>
                                                <td className="p-2 px-4 border-r border-white/5">
                                                    {service.responseTime ? (
                                                        <span className={clsx(
                                                            service.responseTime < 100 ? "text-obsidian-success/80" :
                                                                service.responseTime < 500 ? "text-obsidian-warning/80" :
                                                                    "text-obsidian-danger/80"
                                                        )}>
                                                            {service.responseTime}ms
                                                        </span>
                                                    ) : (
                                                        <span className="text-obsidian-muted/40">—</span>
                                                    )}
                                                </td>
                                                <td className="p-2 px-4">
                                                    <div className="flex items-center justify-end gap-1.5 opacity-60 group-hover:opacity-100 transition-opacity">
                                                        {service.uiUrl && (
                                                            <a
                                                                href={service.uiUrl}
                                                                target="_blank"
                                                                rel="noopener noreferrer"
                                                                onClick={(e) => e.stopPropagation()}
                                                                className="btn-icon w-7 h-7 hover:bg-white/10 text-obsidian-muted hover:text-white rounded-md flex items-center justify-center transition-all active:scale-95 border border-transparent hover:border-white/10"
                                                                title={`Open ${service.name} UI`}
                                                            >
                                                                <ArrowUpRight className="w-3.5 h-3.5" />
                                                            </a>
                                                        )}
                                                        <button
                                                            onClick={(e) => { e.stopPropagation(); handleDockerAction(service.status === 'healthy' ? 'restart' : 'start', service.containerName); }}
                                                            disabled={loadingAction === service.containerName}
                                                            className="btn-icon w-7 h-7 hover:bg-white/10 text-obsidian-muted hover:text-white rounded-md flex items-center justify-center transition-all active:scale-95 border border-transparent hover:border-white/10"
                                                            title={service.status === 'healthy' ? 'Restart Container' : 'Start Container'}
                                                        >
                                                            {loadingAction === service.containerName ? (
                                                                <Loader2 className="w-3.5 h-3.5 animate-spin text-white/50" />
                                                            ) : service.status === 'healthy' ? (
                                                                <RefreshCw className="w-3.5 h-3.5" />
                                                            ) : (
                                                                <Play className="w-3.5 h-3.5 text-white/50 fill-white/20" />
                                                            )}
                                                        </button>
                                                        {service.status === 'healthy' && (
                                                            <button
                                                                onClick={(e) => { e.stopPropagation(); handleDockerAction('stop', service.containerName); }}
                                                                disabled={loadingAction === service.containerName}
                                                                className="btn-icon w-7 h-7 hover:bg-obsidian-danger/20 text-obsidian-muted hover:text-obsidian-danger rounded-md flex items-center justify-center transition-all active:scale-95 border border-transparent hover:border-obsidian-danger/30"
                                                                title="Stop Container"
                                                            >
                                                                <Square className="w-3 h-3 fill-current" />
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
                <div className="h-8 bg-black/40 backdrop-blur-xl border-t border-white/5 flex items-center px-4 text-[10px] text-obsidian-muted gap-5 shrink-0 z-10 relative">
                    <div className="flex items-center gap-1.5">
                        <Boxes className="w-3.5 h-3.5 text-foreground/50" />
                        <span className="text-foreground/80 font-medium">{totalCount}</span> services
                    </div>
                    <div className="flex items-center gap-1.5">
                        <CheckCircle2 className="w-3 h-3 text-white/40" />
                        <span className="text-foreground/80 font-medium">{healthyCount}</span> healthy
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
