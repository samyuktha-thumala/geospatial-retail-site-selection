import { createRootRoute, Outlet } from "@tanstack/react-router";
import { WelcomeDialog } from "@/components/welcome-dialog";

export const Route = createRootRoute({
  component: () => (
    <>
      <WelcomeDialog />
      <Outlet />
    </>
  ),
});
