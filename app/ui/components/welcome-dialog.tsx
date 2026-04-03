import { useState, useEffect } from "react"

const STORAGE_KEY = "site-selection-welcome-dismissed"

export function WelcomeDialog() {
  const [open, setOpen] = useState(false)

  useEffect(() => {
    if (!sessionStorage.getItem(STORAGE_KEY)) {
      setOpen(true)
    }
  }, [])

  if (!open) return null

  const handleDismiss = () => {
    sessionStorage.setItem(STORAGE_KEY, "true")
    setOpen(false)
  }

  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        zIndex: 99999,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        backgroundColor: "rgba(0,0,0,0.5)",
        backdropFilter: "blur(4px)",
      }}
    >
      <div
        style={{
          background: "white",
          borderRadius: "12px",
          padding: "32px",
          maxWidth: "420px",
          width: "90%",
          boxShadow: "0 25px 50px -12px rgba(0,0,0,0.25)",
          textAlign: "center",
        }}
      >
        <h2 style={{ fontSize: "20px", fontWeight: 600, color: "#0f172a", marginBottom: "8px" }}>
          Welcome to the Site Selection Accelerator
        </h2>
        <p style={{ fontSize: "14px", color: "#64748b", marginBottom: "24px" }}>
          Analyze your retail network, explore expansion opportunities, and run scenario simulations.
        </p>
        <button
          onClick={handleDismiss}
          style={{
            padding: "10px 32px",
            fontSize: "14px",
            fontWeight: 500,
            color: "white",
            backgroundColor: "#2563eb",
            border: "none",
            borderRadius: "8px",
            cursor: "pointer",
          }}
        >
          Get Started
        </button>
      </div>
    </div>
  )
}
