import re

with open("web-ui/src/app/notebooks/page.tsx", "r") as f:
    content = f.read()

# Pattern to replace the dedicated cell header with the minimalist floating toolbar
pattern = r'\{/\* NEW: Dedicated Cell Header \*/\}.*?\{/\* Right Side: Actions \*/\}.*?</div>\s*</div>'

replacement = """{/* NEW: Minimalist Absolute Toolbar straddling the top border */}
                            <div className={clsx(
                                "absolute top-0 right-4 -translate-y-1/2 flex items-center gap-0.5 px-1 py-0.5 rounded border border-obsidian-border bg-[#0c0c0d] z-20 transition-all duration-200",
                                isActive ? "opacity-100" : "opacity-0 scale-95 pointer-events-none group-hover/cell:opacity-100 group-hover/cell:scale-100 group-hover/cell:pointer-events-auto"
                            )}>
                                {/* Language Selector (Code only) */}
                                {cell.cell_type === 'code' && (
                                    <div className="relative flex items-center gap-1 mr-1 border-r border-obsidian-border/50 pr-1 pl-1">
                                        {selectedLang === 'sql' ? <Database className="w-3 h-3 text-obsidian-info absolute left-1 pointer-events-none" /> : <Code2 className="w-3 h-3 text-obsidian-success absolute left-1 pointer-events-none" />}
                                        <select
                                            value={cell.language}
                                            onChange={(e) => onUpdate({ language: e.target.value as 'python' | 'sql' })}
                                            className={clsx(
                                                "pl-5 pr-2 py-0.5 text-[9px] uppercase font-bold tracking-wider appearance-none bg-transparent hover:bg-white/5 rounded cursor-pointer outline-none transition-colors",
                                                selectedLang === 'sql' ? "text-obsidian-info" : "text-obsidian-success"
                                            )}
                                        >
                                            <option value="python" className="bg-obsidian-panel text-foreground">PYTHON</option>
                                            <option value="sql" className="bg-obsidian-panel text-foreground">SQL</option>
                                        </select>
                                    </div>
                                )}
                                {cell.cell_type === 'markdown' && <div className="text-[9px] font-bold uppercase tracking-wider text-obsidian-muted px-2 mr-1 border-r border-obsidian-border/50">MD</div>}
                                
                                {/* Actions */}
                                <button onClick={(e) => { e.stopPropagation(); onMoveUp(); }} disabled={index === 0} className="w-5 h-5 flex items-center justify-center rounded hover:bg-white/10 text-obsidian-muted hover:text-foreground disabled:opacity-30 transition-colors" title="Move up"><ChevronUp className="w-3.5 h-3.5" /></button>
                                <button onClick={(e) => { e.stopPropagation(); onMoveDown(); }} disabled={index === total - 1} className="w-5 h-5 flex items-center justify-center rounded hover:bg-white/10 text-obsidian-muted hover:text-foreground disabled:opacity-30 transition-colors" title="Move down"><ChevronDown className="w-3.5 h-3.5" /></button>
                                <div className="w-px h-3 bg-obsidian-border/50 mx-0.5" />
                                <button onClick={(e) => { e.stopPropagation(); onDelete(); }} disabled={total <= 1} className="w-5 h-5 flex items-center justify-center rounded hover:bg-obsidian-danger/20 text-obsidian-muted hover:text-obsidian-danger transition-colors disabled:opacity-30" title="Delete cell"><Trash2 className="w-3 h-3" /></button>
                            </div>"""

new_content = re.sub(pattern, replacement, content, flags=re.DOTALL)

with open("web-ui/src/app/notebooks/page.tsx", "w") as f:
    f.write(new_content)
print("Patched successfully")
