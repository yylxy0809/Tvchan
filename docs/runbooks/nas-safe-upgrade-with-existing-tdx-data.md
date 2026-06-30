# NAS Safe Upgrade With Existing TDX Data

Use this when the old NAS package already contains copied TDX history data under
`work/tdx-csv`, and you want to deploy the newer Cloudflare/web-gateway package
without overwriting those files.

## Do not do this

Do not extract the new package directly over the old package folder while file
copy or CSV import is still running.

Do not run:

```bash
docker compose down -v
```

The `-v` flag removes database volumes.

## Recommended upgrade

Assume the old package is:

```text
/vol1/1000/docker/tv-a-share-backend/tv-backend-nas-package
```

And the copied TDX data is:

```text
/vol1/1000/docker/tv-a-share-backend/tv-backend-nas-package/work/tdx-csv
```

After the file transfer/import is idle:

1. Upload the new `tv-backend-nas-package.zip` beside the old folder.
2. Extract it into a new folder, for example:

```text
/vol1/1000/docker/tv-a-share-backend/tv-backend-nas-package-next
```

3. Edit:

```text
tv-backend-nas-package-next/deploy/backend.env
```

4. Set `TDX_CSV_HOST_ROOT` to the old data folder's absolute path:

```env
TDX_CSV_HOST_ROOT=/vol1/1000/docker/tv-a-share-backend/tv-backend-nas-package/work/tdx-csv
```

5. Keep these public gateway settings:

```env
PUBLIC_BASE_URL=https://515178.xyz
WEB_GATEWAY_PORT=8080
COMPOSE_PROFILES=market-fill,history,chan-recompute,tdx-csv-import,tunnel
```

6. Start from the new folder:

```bash
docker compose --env-file deploy/backend.env -f deploy/docker-compose.backend.yml up -d --build
```

Because the compose project name is still `tv-a-share-backend`, Docker will reuse
the existing named database volumes unless you remove them explicitly.

## Verify

Local gateway:

```text
http://192.168.1.5:8080
```

Cloudflare:

```text
https://515178.xyz
```

Docker containers that should exist:

```text
tv_backend_web_gateway
tv_backend_cloudflared
tv_backend_api
tv_backend_timescaledb
tv_backend_chan_service
```

## Rollback

If the new package fails:

1. Do not delete database volumes.
2. Stop the new compose project.
3. Start the old folder's compose project again.
4. Keep the old `work/tdx-csv` folder intact.
