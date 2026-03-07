import { useState } from "react";
import { ChevronDown, ChevronRight, Layers } from "lucide-react";

export interface MapLayer {
  id: string;
  label: string;
  enabled: boolean;
  color?: string;
  size?: "sm" | "md" | "lg";
  icon?: string;
}

interface MapLayerControlProps {
  layers: MapLayer[];
  onToggle: (layerId: string) => void;
}

const SIZE_CLASS: Record<string, string> = {
  sm: "w-1.5 h-1.5",
  md: "w-2 h-2",
  lg: "w-2.5 h-2.5",
};

export function MapLayerControl({ layers, onToggle }: MapLayerControlProps) {
  const [minimized, setMinimized] = useState(false);

  return (
    <div className="absolute top-4 left-4 z-[1000] bg-white/95 backdrop-blur border border-slate-200 rounded-lg shadow-lg">
      <button
        onClick={() => setMinimized(!minimized)}
        className="flex items-center gap-2 px-3 py-2 w-full text-sm font-medium text-slate-900 hover:bg-slate-50 rounded-lg"
      >
        <Layers size={14} />
        <span>Map Layers</span>
        {minimized ? <ChevronRight size={14} className="ml-auto" /> : <ChevronDown size={14} className="ml-auto" />}
      </button>
      {!minimized && (
        <div className="px-3 pb-3 space-y-1.5">
          {layers.map((layer) => (
            <label
              key={layer.id}
              className="flex items-center gap-2 text-[11px] text-slate-600 cursor-pointer hover:text-slate-900"
            >
              <input
                type="checkbox"
                checked={layer.enabled}
                onChange={() => onToggle(layer.id)}
                className="rounded border-slate-300 bg-white text-blue-500 focus:ring-blue-500 focus:ring-offset-0 w-3.5 h-3.5"
              />
              {layer.icon === "!" ? (
                <span
                  className="w-3.5 h-3.5 rounded-full text-white text-[8px] font-bold flex items-center justify-center leading-none shrink-0"
                  style={{ backgroundColor: layer.color }}
                >!</span>
              ) : layer.color ? (
                <span
                  className={`${SIZE_CLASS[layer.size || "md"] || "w-2 h-2"} rounded-full inline-block shrink-0`}
                  style={{ backgroundColor: layer.color }}
                />
              ) : null}
              {layer.label}
            </label>
          ))}
        </div>
      )}
    </div>
  );
}
