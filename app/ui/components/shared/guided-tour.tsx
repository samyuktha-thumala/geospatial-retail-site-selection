import { useState, useEffect, useCallback, useRef } from "react";

export interface TourStep {
  target: string; // data-tour attribute value
  title: string;
  description: string;
  action?: () => void; // optional action when step is shown (e.g. click a filter)
  position?: "top" | "bottom" | "left" | "right";
}

interface GuidedTourProps {
  steps: TourStep[];
  onComplete: () => void;
  isOpen: boolean;
  introTitle?: string;
  introDescription?: string;
}

export function GuidedTour({ steps, onComplete, isOpen, introTitle, introDescription }: GuidedTourProps) {
  // -1 = intro screen, 0..n = tour steps
  const [currentStep, setCurrentStep] = useState(-1);
  const [tooltipStyle, setTooltipStyle] = useState<React.CSSProperties>({});
  const [borderStyle, setBorderStyle] = useState<React.CSSProperties>({});
  const tooltipRef = useRef<HTMLDivElement>(null);

  // Reset to intro whenever tour opens
  useEffect(() => {
    if (isOpen) {
      setCurrentStep(introTitle ? -1 : 0);
    }
  }, [isOpen, introTitle]);

  const step = currentStep >= 0 ? steps[currentStep] : null;

  const positionTooltip = useCallback(() => {
    if (!step) return;
    const el = document.querySelector(`[data-tour="${step.target}"]`);
    if (!el) return;

    const rect = el.getBoundingClientRect();
    const pad = 6;

    // Border highlight around target
    setBorderStyle({
      top: rect.top - pad,
      left: rect.left - pad,
      width: rect.width + pad * 2,
      height: rect.height + pad * 2,
      borderRadius: 12,
    });

    // Tooltip position
    const pos = step.position || "bottom";
    const tooltipWidth = 320;
    let top = 0;
    let left = 0;

    if (pos === "bottom") {
      top = rect.bottom + pad + 12;
      left = rect.left + rect.width / 2 - tooltipWidth / 2;
    } else if (pos === "top") {
      top = rect.top - pad - 12 - 120;
      left = rect.left + rect.width / 2 - tooltipWidth / 2;
    } else if (pos === "right") {
      top = rect.top + rect.height / 2 - 60;
      left = rect.right + pad + 12;
    } else if (pos === "left") {
      top = rect.top + rect.height / 2 - 60;
      left = rect.left - pad - 12 - tooltipWidth;
    }

    // Keep within viewport
    left = Math.max(16, Math.min(left, window.innerWidth - tooltipWidth - 16));
    top = Math.max(16, top);

    setTooltipStyle({ top, left, width: tooltipWidth });
  }, [step]);

  useEffect(() => {
    if (!isOpen || !step) return;
    // Hide border/tooltip during transition
    setBorderStyle({ display: "none" });
    setTooltipStyle({ display: "none" });

    if (step.action) {
      step.action();
    }
    // Scroll target into view, wait for scroll to settle, then position
    const el = document.querySelector(`[data-tour="${step.target}"]`);
    if (el) {
      el.scrollIntoView({ behavior: "smooth", block: "center" });
    }
    // Wait for scroll + action to fully settle before showing
    setTimeout(positionTooltip, 500);
  }, [isOpen, currentStep, step, positionTooltip]);

  // Reposition on resize/scroll
  useEffect(() => {
    if (!isOpen) return;
    const handler = () => positionTooltip();
    window.addEventListener("resize", handler);
    window.addEventListener("scroll", handler, true);
    return () => {
      window.removeEventListener("resize", handler);
      window.removeEventListener("scroll", handler, true);
    };
  }, [isOpen, positionTooltip]);

  if (!isOpen) return null;

  // Intro screen — centered modal
  if (currentStep === -1 && introTitle) {
    return (
      <div className="fixed inset-0 z-[10001]">
        <div
          className="absolute inset-0 bg-black/15 transition-opacity duration-300"
          onClick={onComplete}
        />
        <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[420px] bg-white rounded-xl shadow-2xl border border-slate-200 p-6" style={{ zIndex: 10002 }}>
          <h3 className="text-base font-bold text-slate-900 mb-3">{introTitle}</h3>
          <p className="text-sm text-slate-500 leading-relaxed mb-6">{introDescription}</p>
          <div className="flex items-center justify-between">
            <button
              onClick={onComplete}
              className="text-[11px] text-slate-400 hover:text-slate-600 transition-colors"
            >
              Skip tour
            </button>
            <button
              onClick={() => setCurrentStep(0)}
              className="px-5 py-2 text-xs font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 transition-colors"
            >
              Start Tour
            </button>
          </div>
        </div>
      </div>
    );
  }

  if (!step) return null;

  const isLast = currentStep === steps.length - 1;

  return (
    <div className="fixed inset-0 z-[10001] pointer-events-none">
      {/* Click catcher — invisible but captures clicks to dismiss */}
      <div
        className="absolute inset-0 pointer-events-auto"
        onClick={onComplete}
      />

      {/* Border highlight around target */}
      <div
        className="absolute transition-all duration-300 ease-out pointer-events-none"
        style={{
          ...borderStyle,
          border: "2.5px solid #db2777",
          boxShadow: "0 0 12px 3px rgba(219,39,119,0.35), 0 0 0 4px rgba(219,39,119,0.12)",
        }}
      />

      {/* Tooltip */}
      <div
        ref={tooltipRef}
        className="absolute bg-white rounded-xl shadow-2xl border border-slate-200 p-5 transition-all duration-300 ease-out pointer-events-auto"
        style={{ ...tooltipStyle, zIndex: 10002 }}
      >
        {/* Step counter */}
        <div className="flex items-center gap-1.5 mb-3">
          {steps.map((_, i) => (
            <div
              key={i}
              className={`h-1.5 rounded-full transition-all ${
                i === currentStep
                  ? "w-6 bg-blue-500"
                  : i < currentStep
                  ? "w-1.5 bg-blue-300"
                  : "w-1.5 bg-slate-200"
              }`}
            />
          ))}
        </div>

        <h4 className="text-sm font-bold text-slate-900 mb-1">{step.title}</h4>
        <p className="text-xs text-slate-500 leading-relaxed mb-4">{step.description}</p>

        <div className="flex items-center justify-between">
          <button
            onClick={onComplete}
            className="text-[11px] text-slate-400 hover:text-slate-600 transition-colors"
          >
            Skip tour
          </button>
          <div className="flex gap-2">
            {currentStep > 0 && (
              <button
                onClick={() => setCurrentStep((s) => s - 1)}
                className="px-3 py-1.5 text-[11px] font-medium text-slate-600 border border-slate-200 rounded-lg hover:bg-slate-50 transition-colors"
              >
                Back
              </button>
            )}
            <button
              onClick={() => {
                if (isLast) {
                  onComplete();
                } else {
                  setCurrentStep((s) => s + 1);
                }
              }}
              className="px-4 py-1.5 text-[11px] font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 transition-colors"
            >
              {isLast ? "Done" : "Next"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
