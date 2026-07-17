import { CandlestickChart, LogOut } from "lucide-react";
import { useState } from "react";
import { type AuthSession } from "./auth/api";
import { getApiToken } from "./config";
import {
  clearSavedSessionMeta,
  loadSavedSession,
  loadSavedToken,
  persistSession,
} from "./app/sessionPersistence";
import { ChartWorkspace } from "./components/ChartWorkspace";
import { AdminConsole } from "./components/AdminConsole";
import { LoginPage } from "./components/LoginPage";

type AppView = "chart" | "admin";

export default function App() {
  const [session, setSession] = useState<AuthSession | null>(loadSavedSession);
  const [view, setView] = useState<AppView>("chart");

  function handleAuthenticated(next: AuthSession) {
    persistSession(next);
    setSession(next);
    setView("chart");
  }

  function handleLogout() {
    clearSavedSessionMeta();
    setSession(null);
    setView("chart");
  }

  if (!session) {
    return (
      <LoginPage
        initialToken={loadSavedToken() || getApiToken()}
        onAuthenticated={handleAuthenticated}
      />
    );
  }

  return (
    <>
      <main className="terminal-shell terminal-shell--chart" hidden={view !== "chart"}>
        <ChartWorkspace
          session={session}
          onOpenAdmin={() => setView("admin")}
          onLogout={handleLogout}
        />
      </main>
      {session.role === "admin" ? (
        <main className="terminal-shell" hidden={view !== "admin"}>
          <header className="shell-header">
            <div className="brand-mark" aria-label="A-share terminal">
              <CandlestickChart size={19} />
              <span>A股终端</span>
            </div>
            <div className="shell-actions">
              <nav className="view-tabs" aria-label="工作区">
                <button type="button" onClick={() => setView("chart")}>
                  图表
                </button>
                <button type="button" data-active="true">
                  管理后台
                </button>
              </nav>
              <button className="ghost-button" type="button" onClick={handleLogout}>
                <LogOut size={16} />
                <span>退出登录</span>
              </button>
            </div>
          </header>
          <AdminConsole
            adminToken={session.token}
            onAuthenticationFailure={handleLogout}
          />
        </main>
      ) : null}
    </>
  );
}
