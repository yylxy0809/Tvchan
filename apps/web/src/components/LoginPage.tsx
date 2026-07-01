import { AlertCircle, KeyRound } from "lucide-react";
import { type FormEvent, useState } from "react";
import { type AuthSession, loginWithToken } from "../auth/api";

type Props = {
  initialToken: string;
  onAuthenticated(session: AuthSession): void;
};

export function LoginPage({ initialToken, onAuthenticated }: Props) {
  const [token, setToken] = useState(initialToken);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting(true);
    setError(null);
    try {
      const session = await loginWithToken(token);
      onAuthenticated(session);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setSubmitting(false);
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
