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

interface Feature {
  name: string;
  importance: number;
  direction: string;
  category: string;
}

interface FeatureImportanceProps {
  features: Feature[];
}

function getBarColor(direction: string): string {
  return direction === "positive" ? "#22c55e" : "#ef4444";
}

export function FeatureImportanceChart({ features }: FeatureImportanceProps) {
  const sorted = [...features].sort((a, b) => b.importance - a.importance);

  return (
    <div className="w-full bg-slate-50 rounded-lg p-4">
      <ResponsiveContainer
        width="100%"
        height={Math.max(200, sorted.length * 32 + 40)}
      >
        <BarChart
          data={sorted}
          layout="vertical"
          margin={{ top: 4, right: 8, bottom: 4, left: 0 }}
        >
          <CartesianGrid
            strokeDasharray="3 3"
            stroke="#e2e8f0"
            horizontal={false}
          />
          <XAxis
            type="number"
            domain={[0, 1]}
            tick={{ fill: "#64748b", fontSize: 12 }}
            axisLine={{ stroke: "#e2e8f0" }}
            tickLine={{ stroke: "#e2e8f0" }}
            tickFormatter={(value: number) => value.toFixed(1)}
          />
          <YAxis
            type="category"
            dataKey="name"
            tick={{ fill: "#64748b", fontSize: 12 }}
            axisLine={{ stroke: "#e2e8f0" }}
            tickLine={false}
            width={116}
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
              `${value.toFixed(3)}`,
              "Importance",
            ]}
            labelStyle={{ color: "#64748b" }}
            cursor={{ fill: "rgba(148, 163, 184, 0.1)" }}
          />
          <Bar
            dataKey="importance"
            radius={[0, 4, 4, 0]}
            isAnimationActive={false}
          >
            {sorted.map((feature, index) => (
              <Cell
                key={`cell-${index}`}
                fill={getBarColor(feature.direction)}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
