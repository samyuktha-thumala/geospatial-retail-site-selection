interface LegendItem {
  label: string;
  color: string;
  shape?: "circle" | "square" | "diamond";
}

interface MapLegendProps {
  items: LegendItem[];
}

export function MapLegend({ items }: MapLegendProps) {
  return (
    <div className="absolute bottom-4 left-4 z-[1000] bg-white/95 backdrop-blur border border-slate-200 rounded-lg p-3 shadow-lg">
      <p className="text-xs font-medium text-slate-600 mb-2">Legend</p>
      <div className="space-y-1.5">
        {items.map((item) => (
          <div key={item.label} className="flex items-center gap-2 text-xs text-slate-500">
            <span
              className={`w-3 h-3 inline-block ${
                item.shape === "diamond" ? "rotate-45" : item.shape === "square" ? "" : "rounded-full"
              }`}
              style={{ backgroundColor: item.color }}
            />
            <span>{item.label}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
