import { createFileRoute } from "@tanstack/react-router";
import { useState, useEffect } from "react";
import { Database, MapPin, Users, Building2, RefreshCw, Clock } from "lucide-react";
import { api, type DataSource } from "@/lib/api";
import { MiniLineChart } from "@/components/charts/mini-line-chart";
import { MiniBarChart } from "@/components/charts/mini-bar-chart";

export const Route = createFileRoute("/_sidebar/data-exploration")({
  component: DataExplorationPage,
});

const ICONS: Record<string, React.ComponentType<{ size?: number; className?: string }>> = {
  database: Database,
  "map-pin": MapPin,
  users: Users,
  building: Building2,
};

function DataExplorationPage() {
  const [sources, setSources] = useState<DataSource[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.listDataSources().then((data) => {
      setSources(data);
      setLoading(false);
    });
  }, []);

  return (
    <div className="p-6 space-y-6 min-h-full flex flex-col">

      {loading ? (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="bg-white border border-slate-200 rounded-lg h-80 animate-pulse" />
          ))}
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-5 flex-1">
          {sources.map((source) => (
            <DataSourceCard key={source.id} source={source} />
          ))}
        </div>
      )}
    </div>
  );
}

function DataSourceCard({ source }: { source: DataSource }) {
  const Icon = ICONS[source.icon] || Database;
  const typeColors: Record<string, string> = {
    "Delta Table": "bg-blue-50 text-blue-700 border-blue-200",
    "REST API": "bg-green-50 text-green-700 border-green-200",
    "External API": "bg-purple-50 text-purple-700 border-purple-200",
    "Third Party Vendor": "bg-orange-50 text-orange-700 border-orange-200",
  };

  return (
    <div className="bg-white border border-slate-200 rounded-lg overflow-hidden flex flex-col h-full shadow-sm">
      {/* Header */}
      <div className="px-4 py-3 border-b border-slate-100">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2.5">
            <div className="p-1.5 bg-slate-100 rounded-md">
              <Icon size={16} className="text-blue-600" />
            </div>
            <div>
              <h3 className="text-sm font-medium text-slate-900">{source.name}</h3>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <span
              className={`text-[10px] px-2 py-0.5 rounded-full border ${
                typeColors[source.type] || "bg-slate-50 text-slate-600 border-slate-200"
              }`}
            >
              {source.type}
            </span>
            <span className="text-[10px] px-2 py-0.5 rounded-full bg-slate-50 text-slate-600 border border-slate-200">
              {source.records} records
            </span>
          </div>
        </div>
      </div>

      {/* Connection Info */}
      <div className="px-4 py-2 flex items-center gap-4 text-[11px] text-slate-500 border-b border-slate-100">
        <span className="flex items-center gap-1">
          <Clock size={10} /> Last Sync: {source.last_sync}
        </span>
        <span className="flex items-center gap-1">
          <RefreshCw size={10} /> Refresh: {source.refresh_rate}
        </span>
      </div>

      {/* Stats Grid */}
      <div className="px-4 py-3 flex-1">
        <p className="text-[10px] font-medium text-slate-400 uppercase tracking-wider mb-2">
          Exploratory Statistics
        </p>
        <div className="grid grid-cols-2 gap-x-4 gap-y-2.5">
          {source.stats.map((stat) => (
            <div key={stat.key} className="flex justify-between text-xs">
              <span className="text-slate-500">{stat.key}</span>
              <span className="text-slate-800 font-medium">{stat.value}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Chart */}
      <div className="px-4 pb-4 mt-auto">
        {source.chart_type === "line" ? (
          <MiniLineChart data={source.chart_data} />
        ) : (
          <MiniBarChart data={source.chart_data} />
        )}
      </div>
    </div>
  );
}
