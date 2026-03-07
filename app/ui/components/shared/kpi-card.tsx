"use client";

import {
  TrendingUp,
  TrendingDown,
  Store,
  DollarSign,
  Calendar,
  Activity,
  MapPin,
  Target,
  type LucideIcon,
} from "lucide-react";
import { cn } from "@/lib/utils";

const iconMap: Record<string, LucideIcon> = {
  store: Store,
  Store: Store,
  dollar: DollarSign,
  DollarSign: DollarSign,
  calendar: Calendar,
  Calendar: Calendar,
  activity: Activity,
  Activity: Activity,
  trending_up: TrendingUp,
  TrendingUp: TrendingUp,
  trending_down: TrendingDown,
  TrendingDown: TrendingDown,
  MapPin: MapPin,
  Target: Target,
};

interface KpiCardProps {
  label: string;
  value: string;
  trend?: number | null;
  trendLabel?: string | null;
  icon: string;
}

export function KpiCard({ label, value, trend, trendLabel, icon }: KpiCardProps) {
  const IconComponent = iconMap[icon] ?? Activity;
  const isPositive = trend != null && trend >= 0;
  const isNegative = trend != null && trend < 0;

  return (
    <div
      className={cn(
        "rounded-lg border border-slate-200 bg-white px-4 py-3 shadow-sm",
        "transition-colors hover:border-slate-300"
      )}
    >
      <div className="flex items-center gap-2 mb-1">
        <div className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md bg-blue-50 text-blue-600">
          <IconComponent className="h-3.5 w-3.5" />
        </div>
        <p className="text-xs text-slate-500 truncate">{label}</p>
      </div>
      <p className="text-lg font-bold tracking-tight text-slate-900">{value}</p>
      {trend != null && (
        <div className="mt-1 flex items-center gap-1">
          {isPositive ? (
            <TrendingUp className="h-3 w-3 text-emerald-500" />
          ) : (
            <TrendingDown className="h-3 w-3 text-red-500" />
          )}
          <span
            className={cn(
              "text-[10px] font-medium",
              isPositive && "text-emerald-600",
              isNegative && "text-red-600"
            )}
          >
            {isPositive ? "+" : ""}
            {trend}%
          </span>
          {trendLabel && (
            <span className="text-[10px] text-slate-400">{trendLabel}</span>
          )}
        </div>
      )}
    </div>
  );
}
