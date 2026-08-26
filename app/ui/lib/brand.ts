// Runtime branding. The same frontend bundle serves multiple Databricks Apps;
// the backend /api/config endpoint reports which brand to render (driven by the
// APP_BRAND env var on each app). This keeps one codebase for all deployments.
import { useEffect, useState } from "react";

export type BrandId = "default" | "dollar_general";

export interface Brand {
  id: BrandId;
  name: string;
  tagline: string;
  /** Primary accent (nav-active, buttons) */
  accent: string;
  accentText: string;
  /** Short badge text shown in the logo mark */
  badge: string;
  badgeBg: string;
  badgeText: string;
}

export const BRANDS: Record<BrandId, Brand> = {
  default: {
    id: "default",
    name: "Retail SiteLab",
    tagline: "Site Selection for Strategic Expansion",
    accent: "#2563eb", // blue-600
    accentText: "#ffffff",
    badge: "",
    badgeBg: "",
    badgeText: "",
  },
  dollar_general: {
    id: "dollar_general",
    name: "Dollar General",
    tagline: "Site Selection for Strategic Expansion",
    accent: "#FFD400", // DG yellow
    accentText: "#231F20", // DG near-black
    badge: "DG",
    badgeBg: "#FFD400",
    badgeText: "#231F20",
  },
};

let cached: BrandId | null = null;

export function useBrand(): Brand {
  const [id, setId] = useState<BrandId>(cached ?? "default");
  useEffect(() => {
    if (cached) return;
    let alive = true;
    fetch("/api/config")
      .then((r) => (r.ok ? r.json() : { brand: "default" }))
      .then((c) => {
        const b: BrandId = c?.brand === "dollar_general" ? "dollar_general" : "default";
        cached = b;
        if (typeof document !== "undefined") {
          document.title = BRANDS[b].name;
          // Scope brand-specific CSS overrides (see app.css [data-brand=...]).
          document.documentElement.setAttribute("data-brand", b);
        }
        if (alive) setId(b);
      })
      .catch(() => {});
    return () => {
      alive = false;
    };
  }, []);
  return BRANDS[id];
}
