import { CandlestickChart, LogOut } from "lucide-react";
import { useEffect, useState } from "react";
import { type AuthSession, isCredentialRejection, loginWithToken } from "./auth/api";
import { getApiToken } from "./config";
import { chartDataManager } from "./api/chartDataManager";
import {
  clearSavedSessionMeta,
  loadSavedToken,
  persistSession,
} from "./app/sessionPersistence";
import { ChartWorkspace } from "./components/ChartWorkspace";
import { AdminConsole } from "./components/AdminConsole";
import { LoginPage } from "./components/LoginPage";
import { SessionAuthorityFence } from "./auth/sessionAuthorityFence";

type AppView = "chart" | "admin";

export default function App() {
  const [initialLoginToken] = useState(loadSavedToken);
  const [loginHint, setLoginHint] = useState(
    () => initialLoginToken || getApiToken(),
  );
  const [session, setSession] = useState<AuthSession | null>(null);
  const [restoringSession, setRestoringSession] = useState(Boolean(initialLoginToken));
  const [view, setView] = useState<AppView>("chart");
  const [sessionAuthority] = useState(() => new SessionAuthorityFence());

  useEffect(() => {
    if (!initialLoginToken) return;
    chartDataManager.resetSession();
    let active = true;
    void loginWithToken(initialLoginToken)
      .then((next) => {
        if (!active) return;
        handleAuthenticated(next);
      })
      .catch((error: unknown) => {
        if (!active) return;
        if (isCredentialRejection(error)) {
          clearSavedSessionMeta();
          setLoginHint("");
        }
      })
      .finally(() => {
        if (active) setRestoringSession(false);
      });
    return () => {
      active = false;
    };
  }, [initialLoginToken]);

  function handleAuthenticated(next: AuthSession) {
    sessionAuthority.activate();
    chartDataManager.resetSession();
    persistSession(next);
    setLoginHint(next.token);
    setSession(next);
    setView("chart");
  }

  function handleLogout() {
    sessionAuthority.invalidate();
    chartDataManager.resetSession();
    clearSavedSessionMeta();
    setLoginHint("");
    setSession(null);
    setView("chart");
  }

  function handleSessionAuthenticationFailure(generation: number) {
    sessionAuthority.runIfCurrent(generation, handleLogout);
  }

  if (restoringSession) {
    return (
      <main className="login-shell">
        <section className="login-panel" aria-label="Session verification">
          <div className="login-status" role="status">
            <span />
            <strong>Verifying saved session</strong>
          </div>
        </section>
      </main>
    );
  }

  if (!session) {
    return (
      <LoginPage
        initialToken={loginHint}
        onAuthenticated={handleAuthenticated}
        onAuthenticationFailure={handleLogout}
      />
    );
  }

  const sessionGeneration = sessionAuthority.capture();

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
            onAuthenticationFailure={() => handleSessionAuthenticationFailure(sessionGeneration)}
          />
        </main>
      ) : null}
    </>
  );
}
