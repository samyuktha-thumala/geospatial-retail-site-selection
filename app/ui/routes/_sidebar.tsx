import { createFileRoute, Outlet, Link, useMatchRoute } from "@tanstack/react-router";
import { Database, Map, Brain, Globe, FlaskConical, HelpCircle, Presentation } from "lucide-react";

const navItems = [
  { to: "/network-diagnostics" as const, label: "Network Diagnostics", icon: Map },
  { to: "/site-playground" as const, label: "Site Playground", icon: Brain },
  // { to: "/data-exploration" as const, label: "Sources", icon: Database },
  // { to: "/data-validation" as const, label: "Validation", icon: FlaskConical },
];

export const Route = createFileRoute("/_sidebar")({
  component: TopNavLayout,
});

function TopNavLayout() {
  const matchRoute = useMatchRoute();

  return (
    <div className="flex flex-col h-screen bg-slate-50">
      {/* Top Navbar */}
      <header className="relative flex items-center justify-between bg-slate-900 px-8 h-14 shrink-0 shadow-md">
        {/* Brand */}
        <div className="flex items-center gap-3">
          <Globe size={22} className="text-red-500" strokeWidth={2.5} />
          <h1 className="text-base font-bold text-white tracking-tight">Retail SiteLab</h1>
          <div className="w-px h-5 bg-slate-700" />
          <span className="text-sm text-slate-400 hidden sm:inline">Site Selection for Strategic Expansion</span>
        </div>

        {/* Nav — centered */}
        <nav className="absolute left-1/2 -translate-x-1/2 flex items-center gap-1.5">
          {navItems.map((item) => {
            const isActive = matchRoute({ to: item.to });
            const Icon = item.icon;
            return (
              <Link
                key={item.to}
                to={item.to}
                className={`flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                  isActive
                    ? "bg-blue-600 text-white"
                    : "text-slate-300 hover:bg-slate-800 hover:text-white"
                }`}
              >
                <Icon size={16} />
                <span>{item.label}</span>
              </Link>
            );
          })}
        </nav>

        {/* Right side actions */}
        <div className="flex items-center gap-1">
          <button
            onClick={() => window.dispatchEvent(new CustomEvent("start-tour"))}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-slate-300 rounded-md hover:bg-slate-800 hover:text-white transition-colors"
          >
            <HelpCircle size={14} />
            Tour
          </button>
          <button
            onClick={() => window.open("/slides", "_blank", "noopener")}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-slate-300 rounded-md hover:bg-slate-800 hover:text-white transition-colors"
          >
            <Presentation size={14} />
          </button>
        </div>
      </header>

      {/* Main Content */}
      <main className="flex-1 overflow-hidden">
        <Outlet />
      </main>

      {/* Agent chat is now inline in each page's panel */}
    </div>
  );
}
