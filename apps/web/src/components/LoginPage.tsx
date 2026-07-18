import { AlertCircle, KeyRound } from "lucide-react";
import { type FormEvent, useEffect, useRef, useState } from "react";
import { AuthenticationError, type AuthSession, loginWithToken } from "../auth/api";
import { LoginAttemptFence } from "../auth/loginAttemptFence";

type Props = {
  initialToken: string;
  onAuthenticated(session: AuthSession): void;
  onAuthenticationFailure?(): void;
};

export function LoginPage({
  initialToken,
  onAuthenticated,
  onAuthenticationFailure,
}: Props) {
  const [token, setToken] = useState(initialToken);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const attemptFence = useRef<LoginAttemptFence | null>(null);
  if (!attemptFence.current) {
    attemptFence.current = new LoginAttemptFence();
  }

  useEffect(() => {
    const fence = new LoginAttemptFence();
    attemptFence.current = fence;
    return () => fence.dispose();
  }, []);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const fence = attemptFence.current!;
    const attempt = fence.begin();
    setSubmitting(true);
    setError(null);
    try {
      const session = await loginWithToken(token);
      if (!fence.isCurrent(attempt)) return;
      onAuthenticated(session);
    } catch (nextError) {
      if (!fence.isCurrent(attempt)) return;
      if (
        nextError instanceof AuthenticationError &&
        (nextError.status === 401 || nextError.status === 403)
      ) {
        setToken("");
        onAuthenticationFailure?.();
      }
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      if (fence.isCurrent(attempt)) setSubmitting(false);
    }
  }

  return (
    <main className="login-shell">
      <section className="login-panel" aria-label="Token login">
        <div className="login-brand">
          <div className="brand-emblem">
            <KeyRound size={22} />
          </div>
          <div>
            <p>TradingView Access</p>
            <h1>A-Share Chan Terminal</h1>
          </div>
        </div>

        <form className="login-form" onSubmit={handleSubmit}>
          <label htmlFor="login-token">Access Token</label>
          <input
            id="login-token"
            type="password"
            autoComplete="current-password"
            value={token}
            onChange={(event) => setToken(event.target.value)}
            placeholder="Enter your token"
          />
          {error ? (
            <p className="form-error" role="alert">
              <AlertCircle size={15} />
              <span>{error}</span>
            </p>
          ) : null}
          <button className="primary-button" type="submit" disabled={submitting}>
            {submitting ? "Signing in" : "Sign in"}
          </button>
        </form>

        <div className="login-status">
          <span />
          <strong>Secure session</strong>
          <small>Token is stored locally on this browser.</small>
        </div>
      </section>
    </main>
  );
}
