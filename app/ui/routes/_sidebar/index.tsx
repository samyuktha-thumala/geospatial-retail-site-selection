import { createFileRoute, Navigate } from "@tanstack/react-router";

export const Route = createFileRoute("/_sidebar/")({
  component: () => <Navigate to="/network-diagnostics" />,
});
