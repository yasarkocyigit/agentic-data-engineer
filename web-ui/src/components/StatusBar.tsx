import React from 'react';
import { GitBranch, CheckCircle2, AlertTriangle, Bell, XCircle } from 'lucide-react';
import clsx from 'clsx';

export default function StatusBar() {
    return (
        <footer className="h-6 bg-[#18181c] text-white flex items-center justify-between px-3 text-[11px] select-none shrink-0 z-50 relative border-t border-[#1f1f23]">
            {/* Left Section: Source Control & Errors */}
            <div className="flex items-center gap-4">
                <div className="flex items-center gap-1 hover:bg-white/20 px-1.5 py-0.5 rounded cursor-pointer transition-colors">
                    <GitBranch className="w-3 h-3" />
                    <span className="font-medium">main*</span>
                </div>

                <div className="flex items-center gap-1 hover:bg-white/20 px-1.5 py-0.5 rounded cursor-pointer transition-colors">
                    <XCircle className="w-3 h-3" />
                    <span>0</span>
                    <AlertTriangle className="w-3 h-3 ml-1" />
                    <span>0</span>
                </div>
            </div>

            {/* Right Section: Language, encoding, status */}
            <div className="flex items-center gap-4">
                <div className="flex items-center gap-2 hover:bg-white/20 px-1.5 py-0.5 rounded cursor-pointer transition-colors">
                    <span>Ln 12, Col 45</span>
                </div>
                <div className="flex items-center gap-2 hover:bg-white/20 px-1.5 py-0.5 rounded cursor-pointer transition-colors">
                    <span>UTF-8</span>
                </div>
                <div className="flex items-center gap-2 hover:bg-white/20 px-1.5 py-0.5 rounded cursor-pointer transition-colors">
                    <span>TypeScript React</span>
                </div>
                <div className="flex items-center gap-1 hover:bg-white/20 px-1.5 py-0.5 rounded cursor-pointer transition-colors">
                    <CheckCircle2 className="w-3 h-3" />
                    <span>Prettier</span>
                </div>
                <div className="flex items-center gap-1 hover:bg-white/20 px-1.5 py-0.5 rounded cursor-pointer transition-colors">
                    <Bell className="w-3 h-3" />
                </div>
            </div>
        </footer>
    );
}
