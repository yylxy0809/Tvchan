# Cloudflare Domain and Tunnel Setup for 515178.xyz

Current DNS check:

```text
515178.xyz NS launch1.spaceship.net
515178.xyz NS launch2.spaceship.net
```

This means the domain is currently using Spaceship nameservers. Cloudflare will
not become authoritative until the nameservers are changed at Spaceship.

## Target architecture

```text
https://515178.xyz
  -> Cloudflare DNS / proxy / tunnel
  -> cloudflared container on NAS
  -> web-gateway container :80
  -> static TradingView frontend
  -> /api/* and /ws/* reverse proxied to api:8001
```

## 1. Add the domain to Cloudflare

Cloudflare dashboard:

1. Add a website or domain.
2. Enter `515178.xyz`.
3. Choose the Free plan unless you explicitly need paid features.
4. Let Cloudflare scan DNS records.
5. Save the two Cloudflare-assigned nameservers.

Cloudflare official flow: add domain, review DNS records, update nameservers,
then complete SSL/TLS setup.

## 2. Change nameservers at Spaceship

Log in to the registrar account where `515178.xyz` was purchased.

1. Open the domain `515178.xyz`.
2. Find Nameservers.
3. Replace:
   - `launch1.spaceship.net`
   - `launch2.spaceship.net`
4. With the two nameservers assigned by Cloudflare.
5. Save.

After this, Cloudflare may take minutes to hours to show the zone as active.

## 3. Create the Cloudflare Tunnel

Cloudflare dashboard path:

```text
Zero Trust -> Networks -> Connectors -> Cloudflare Tunnels
```

Create a tunnel:

1. Connector type: `cloudflared`.
2. Tunnel name: `tv-a-share-nas`.
3. Environment: Docker.
4. Copy the tunnel token from the Docker command.

Put only the token value into:

```env
CLOUDFLARED_TOKEN=<paste-token-here>
PUBLIC_BASE_URL=https://515178.xyz
CORS_ORIGINS=https://515178.xyz,http://192.168.1.5:8080,http://127.0.0.1:5173
```

Use `deploy/backend.env` on the NAS package.

## 4. Publish the public hostname

In the same tunnel, add a public hostname:

```text
Subdomain: empty
Domain: 515178.xyz
Path: empty
Service type: HTTP
URL: http://web-gateway:80
```

Optional extra hostname:

```text
Subdomain: tv
Domain: 515178.xyz
Service type: HTTP
URL: http://web-gateway:80
```

This would expose both:

```text
https://515178.xyz
https://tv.515178.xyz
```

## 5. Start NAS services

Core backend plus same-origin web gateway:

```bash
docker compose --env-file deploy/backend.env -f deploy/docker-compose.backend.yml up -d --build
```

Cloudflare tunnel:

```bash
docker compose --env-file deploy/backend.env -f deploy/docker-compose.backend.yml --profile tunnel up -d
```

## 6. Verify

Local NAS gateway:

```text
http://192.168.1.5:8080
```

Cloudflare public site:

```text
https://515178.xyz
```

Expected:

- The website loads.
- Token field is visible if no token is saved.
- After entering the backend `API_TOKEN`, status shows API online.
- K-line and Chan overlay requests use `/api/v3/chart/bundle` or `/ws/v2/chart`
  `get_chart_bundle` on the same origin.

## Notes

- Do not put the backend API token in `app-config.production.js` for public use.
- Let users enter their assigned token in the browser. The frontend stores it in
  `localStorage` on that device.
- After sharing account credentials with any assistant or operator, rotate the
  password and keep two-factor authentication enabled.
