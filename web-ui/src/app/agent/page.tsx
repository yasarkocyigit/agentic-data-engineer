
import React from 'react';
import { Sidebar } from '@/components/Sidebar';
import { Bot, Send, Terminal, Trash2 } from 'lucide-react';

export default function AgentPage() {
    return (
        <div className="flex h-screen bg-obsidian-bg text-foreground font-sans overflow-hidden">
            <Sidebar />
            <main className="flex-1 flex flex-col min-w-0 overflow-hidden">
                {/* Header */}
                <header className="h-9 bg-obsidian-panel border-b border-obsidian-border flex items-center justify-between px-4 shrink-0">
                    <div className="flex items-center gap-2">
                        <Terminal className="w-3.5 h-3.5 text-obsidian-muted" />
                        <span className="text-[12px] font-bold text-foreground">Agent Control Center</span>
                    </div>
                    <div className="flex items-center px-2 py-0.5 bg-obsidian-panel-hover border border-obsidian-border rounded text-[11px] text-foreground gap-2">
                        <span className="w-1.5 h-1.5 rounded-full bg-obsidian-success"></span>
                        ClawdBot: ONLINE
                    </div>
                </header>

                <div className="flex-1 flex min-h-0 bg-obsidian-bg">

                    {/* Chat / Terminal Area */}
                    <div className="flex-[3] flex flex-col border-r border-obsidian-border">
                        <div className="flex-1 p-4 font-mono text-[13px] overflow-auto space-y-4">
                            {/* System Message */}
                            <div className="flex items-start gap-4">
                                <Bot className="w-5 h-5 text-obsidian-info mt-1 shrink-0" />
                                <div className="flex flex-col gap-1 max-w-[80%]">
                                    <span className="text-[11px] text-obsidian-muted font-bold">SYSTEM</span>
                                    <div className="text-foreground">Hello! I am ready to manage your data platform. I can deploy pipelines, optimize Trino queries, or debug Airflow DAGs.</div>
                                </div>
                            </div>

                            {/* User Message */}
                            <div className="flex items-start gap-4 flex-row-reverse">
                                <div className="w-5 h-5 rounded-full bg-obsidian-success flex items-center justify-center text-[10px] text-white font-bold shrink-0">YK</div>
                                <div className="flex flex-col gap-1 max-w-[80%] items-end">
                                    <span className="text-[11px] text-obsidian-muted font-bold">YOU</span>
                                    <div className="text-foreground bg-obsidian-panel p-2 rounded border border-obsidian-border">Check the status of the bronze ingestion pipeline.</div>
                                </div>
                            </div>

                            {/* Response */}
                            <div className="flex items-start gap-4">
                                <Bot className="w-5 h-5 text-obsidian-info mt-1 shrink-0" />
                                <div className="flex flex-col gap-1 max-w-[80%]">
                                    <span className="text-[11px] text-obsidian-muted font-bold">CLAWDBOT</span>
                                    <div className="text-foreground">
                                        Checking Airflow DAG <span className="text-obsidian-purple">data_ingestion_bronze</span>...<br />
                                        Status: <span className="text-obsidian-info">Running</span><br />
                                        Duration: 4m 32s
                                    </div>
                                </div>
                            </div>
                        </div>

                        {/* Input Area */}
                        <div className="h-12 bg-obsidian-panel border-t border-obsidian-border flex items-center px-2 gap-2">
                            <div className="text-obsidian-info font-mono">{'>'}</div>
                            <input type="text" className="flex-1 bg-transparent border-none outline-none text-foreground font-mono text-[13px]" placeholder="Type a command..." />
                            <Send className="w-4 h-4 text-obsidian-muted hover:text-foreground cursor-pointer" />
                        </div>
                    </div>

                    {/* Right Panel: Context/Logs */}
                    <div className="flex-1 flex flex-col bg-obsidian-panel">
                        <div className="h-7 bg-obsidian-panel-hover border-b border-obsidian-border flex items-center px-3 justify-between">
                            <span className="text-[11px] font-bold text-foreground">Execution Logs</span>
                            <Trash2 className="w-3.5 h-3.5 text-obsidian-muted hover:text-white cursor-pointer" />
                        </div>
                        <div className="flex-1 overflow-auto p-2 font-mono text-[11px] text-foreground space-y-1">
                            <div><span className="text-obsidian-muted">10:45:01</span> <span className="text-obsidian-success">[DEBUG]</span> Connecting to Airflow API...</div>
                            <div><span className="text-obsidian-muted">10:45:02</span> <span className="text-obsidian-success">[DEBUG]</span> Fetching DAG runs for 'bronze'...</div>
                            <div><span className="text-obsidian-muted">10:45:03</span> <span className="text-obsidian-info">[INFO]</span> Found 1 active run.</div>
                        </div>
                    </div>
                </div>
            </main>
        </div>
    );
}
