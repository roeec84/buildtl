import { memo } from 'react';
import { Handle, Position } from '@xyflow/react';
import { Database, FileOutput, Wand2, Settings2, Trash2 } from 'lucide-react';

export const SourceNode = memo(({ data }: any) => {
    return (
        <div className="bg-slate-900 border border-indigo-500/50 rounded-xl p-4 min-w-[200px] shadow-lg shadow-indigo-500/10">
            <div className="flex items-center justify-between mb-2 border-b border-indigo-500/20 pb-2">
                <div className="flex items-center gap-2">
                    <Database className="w-4 h-4 text-indigo-400" />
                    <span className="text-sm font-semibold text-white">Source Table</span>
                </div>
                <button
                    onClick={(e) => { e.stopPropagation(); data.onDelete?.(); }}
                    className="text-slate-500 hover:text-red-400 p-1 rounded hover:bg-white/5 transition-colors"
                    title="Delete node"
                >
                    <Trash2 className="w-3 h-3" />
                </button>
            </div>
            <div className="text-xs text-slate-300 font-mono bg-slate-950/50 p-2 rounded mb-2">
                {data.label}
            </div>
            <div className="text-[10px] text-slate-500 uppercase tracking-wider font-semibold">
                {data.sourceType || 'SQL'}
            </div>
            <Handle type="source" position={Position.Right} className="!bg-indigo-500 !w-3 !h-3" />
        </div>
    );
});

export const TransformNode = memo(({ data }: any) => {
    return (
        <div className="bg-slate-900 border border-purple-500/50 rounded-xl p-4 min-w-[250px] shadow-lg shadow-purple-500/10 dashed-border">
            <div className="flex items-center justify-between mb-2 border-b border-purple-500/20 pb-2">
                <div className="flex items-center gap-2">
                    <Wand2 className="w-4 h-4 text-purple-400" />
                    <span className="text-sm font-semibold text-white">AI Transform</span>
                </div>
                <div className="flex gap-1">
                    <button
                        className="p-1 hover:bg-white/10 rounded-lg text-slate-400 hover:text-white transition-colors"
                        onClick={(e) => { e.stopPropagation(); data.onEdit?.(); }}
                        title="Edit configuration"
                    >
                        <Settings2 className="w-3 h-3" />
                    </button>
                    <button
                        className="p-1 hover:bg-white/10 rounded-lg text-slate-400 hover:text-red-400 transition-colors"
                        onClick={(e) => { e.stopPropagation(); data.onDelete?.(); }}
                        title="Delete node"
                    >
                        <Trash2 className="w-3 h-3" />
                    </button>
                </div>
            </div>
            <div className="text-xs text-slate-300 italic mb-2 line-clamp-2">
                {data.prompt || "Describe transformation..."}
            </div>
            <div className="flex justify-center">
                <span className="text-[10px] bg-purple-500/20 text-purple-300 px-2 py-0.5 rounded-full">
                    PySpark
                </span>
            </div>
            <Handle type="target" position={Position.Left} className="!bg-purple-500 !w-3 !h-3" />
            <Handle type="source" position={Position.Right} className="!bg-purple-500 !w-3 !h-3" />
        </div>
    );
});

export const SinkNode = memo(({ data }: any) => {
    return (
        <div className="bg-slate-900 border border-emerald-500/50 rounded-xl p-4 min-w-[200px] shadow-lg shadow-emerald-500/10">
            <div className="flex items-center justify-between mb-2 border-b border-emerald-500/20 pb-2">
                <div className="flex items-center gap-2">
                    <FileOutput className="w-4 h-4 text-emerald-400" />
                    <span className="text-sm font-semibold text-white">Sink Table</span>
                </div>
                <button
                    onClick={(e) => { e.stopPropagation(); data.onDelete?.(); }}
                    className="text-slate-500 hover:text-red-400 p-1 rounded hover:bg-white/5 transition-colors"
                    title="Delete node"
                >
                    <Trash2 className="w-3 h-3" />
                </button>
            </div>
            <div className="text-xs text-slate-300 font-mono bg-slate-950/50 p-2 rounded">
                {data.label || "Output Table"}
            </div>
            <Handle type="target" position={Position.Left} className="!bg-emerald-500 !w-3 !h-3" />
        </div>
    );
});
