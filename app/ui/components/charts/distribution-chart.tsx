"use client";

import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  Cell,
} from "recharts";

interface DistributionDataPoint {
  label: string;
  value: number;
  color?: string;
}

interface DistributionChartProps {
  data: DistributionDataPoint[];
  title: string;
  unit?: string;
  highlightMax?: boolean;
}

const DEFAULT_BAR_COLOR = "#3b82f6";
const HIGHLIGHT_COLOR = "#f59e0b";
const DIM_OPACITY = 0.45;

export function DistributionChart({
  data,
  title,
  unit,
  highlightMax = false,
}: DistributionChartProps) {
  const maxValue = highlightMax ? Math.max(...data.map((d) => d.value)) : -1;
  const maxEntry = highlightMax ? data.find((d) => d.value === maxValue) : null;

  return (
    <div className="w-full bg-slate-50 rounded-lg p-4">
      <h3 className="text-sm font-medium text-slate-700 mb-1">{title}</h3>
      {highlightMax && maxEntry && (
        <p className="text-[10px] text-amber-600 mb-2">
          Largest segment: <span className="font-semibold">{maxEntry.label}</span> ({maxEntry.value}{unit || ""})
        </p>
      )}
      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={data} margin={{ top: 4, right: 4, bottom: 24, left: 4 }}>
          <CartesianGrid
            strokeDasharray="3 3"
            stroke="#e2e8f0"
            vertical={false}
          />
          <XAxis
            dataKey="label"
            tick={{ fill: "#64748b", fontSize: 10 }}
            axisLine={{ stroke: "#e2e8f0" }}
            tickLine={{ stroke: "#e2e8f0" }}
            angle={-35}
            textAnchor="end"
            interval={0}
          />
          <YAxis
            tick={{ fill: "#64748b", fontSize: 12 }}
            axisLine={{ stroke: "#e2e8f0" }}
            tickLine={{ stroke: "#e2e8f0" }}
            tickFormatter={(value: number) =>
              unit ? `${value}${unit}` : String(value)
            }
          />
          <Tooltip
            contentStyle={{
              backgroundColor: "#ffffff",
              border: "1px solid #e2e8f0",
              borderRadius: "0.5rem",
              color: "#1e293b",
              fontSize: 12,
              boxShadow: "0 4px 6px -1px rgb(0 0 0 / 0.1)",
            }}
            formatter={(value: number) => [
              unit ? `${value}${unit}` : value,
              "Value",
            ]}
            labelStyle={{ color: "#64748b" }}
            cursor={{ fill: "rgba(148, 163, 184, 0.1)" }}
          />
          <Bar dataKey="value" radius={[4, 4, 0, 0]} isAnimationActive={false}>
            {data.map((entry, index) => {
              const isMax = highlightMax && entry.value === maxValue;
              return (
                <Cell
                  key={`cell-${index}`}
                  fill={isMax ? HIGHLIGHT_COLOR : (entry.color || DEFAULT_BAR_COLOR)}
                  fillOpacity={highlightMax && !isMax ? DIM_OPACITY : 1}
                />
              );
            })}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
