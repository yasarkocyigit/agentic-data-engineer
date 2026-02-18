// ─── Airflow JWT Token Manager ───
// Authenticates as a service account and caches the JWT token
// Auto-refreshes 60s before expiry

const AIRFLOW_BASE = process.env.AIRFLOW_API_URL || 'http://localhost:8081';
const SA_USERNAME = process.env.AIRFLOW_SA_USERNAME || 'admin';
const SA_PASSWORD = process.env.AIRFLOW_SA_PASSWORD || 'admin';

let cachedToken: string | null = null;
let tokenExpiresAt = 0;

export async function getAirflowToken(): Promise<string> {
    // Return cached token if still valid (with 60s buffer)
    if (cachedToken && Date.now() < tokenExpiresAt - 60_000) {
        return cachedToken;
    }

    const res = await fetch(`${AIRFLOW_BASE}/auth/token`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: SA_USERNAME, password: SA_PASSWORD }),
    });

    if (!res.ok) {
        const errText = await res.text().catch(() => 'Unknown error');
        throw new Error(`Airflow auth failed (${res.status}): ${errText}`);
    }

    const { access_token } = await res.json();
    cachedToken = access_token;

    // Parse JWT to get expiry
    try {
        const payload = JSON.parse(
            Buffer.from(access_token.split('.')[1], 'base64').toString()
        );
        tokenExpiresAt = payload.exp * 1000;
    } catch {
        // Fallback: assume 24h expiry
        tokenExpiresAt = Date.now() + 24 * 60 * 60 * 1000;
    }

    return cachedToken!;
}
