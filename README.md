# WOC

**WOC (Wake On Call)** for Docker apps behind [Traefik](https://traefik.io/traefik/): an app that
opts in is stopped after a quiet period and started again by the next request. A stopped
container uses no memory and no CPU; its image stays on disk.

```
browser ─▶ Traefik ─▶ woc :8080 ─▶ app container (started on demand)
              ▲
              └── routers served by woc :8081/traefik (Traefik HTTP provider)
```

The hard part is not stopping a container, it's what disappears with it: Traefik's Docker
provider only sees running containers, so stopping an app also deletes its route and the next
visitor gets an error. WOC keeps the route alive while the app sleeps, starts the app when a
request arrives, and stops it again after `woc.idle` with no requests and none in flight.

It is one Go binary; its only dependency is [pgx](https://github.com/jackc/pgx), used just for
the optional Postgres listener. The story behind it:
[Off Until Someone Asks](https://www.pablofr.com/blog/off-until-someone-asks).

## How a request is handled

- **App awake:** proxied straight through.
- **App asleep:** WOC runs `docker start` (once, however many requests arrive), waits until the
  app's port accepts TCP connections, and forwards the request. A browser that is still waiting
  after 2 s gets a small "waking up the server" page that reloads itself; API calls and asset
  requests just wait, up to `woc.start_timeout`.
- **Going back to sleep:** after `woc.idle` without requests, and with nothing in flight (a long
  download or a websocket counts as activity until it closes), WOC runs `docker stop`.
- **Started from outside** (a deploy, CI, by hand): the app gets a full idle window from the
  moment WOC notices, and sleeps even if nobody visits.

Measured on a 4 vCPU Hetzner box with a small Node app: `docker start` takes about 230 ms and the
app listens about 150 ms later, so a cold request costs roughly a quarter to half a second more
than a warm one.

## Opt an app in

Labels on the container, and **no `traefik.*` labels** (WOC publishes the router):

```yaml
services:
  web:
    networks: [traefik]               # WOC reaches the app on this network
    labels:
      woc.enable: "true"
      woc.host: app.example.com       # several, comma-separated
      woc.port: "3000"                # port inside the container
      woc.idle: 30m                   # optional, default WOC_DEFAULT_IDLE
      woc.group: myapp                # optional: containers that wake and sleep together
      woc.middlewares: auth@file      # optional: Traefik middlewares for the router
      woc.start_timeout: 60s          # optional: how long a request waits for the wake
      woc.ready: port                 # optional: port (default) | healthy | postgres
      woc.busy_url: http://:4100/busy # optional: 200 = busy, don't stop it yet
      woc.wake_every: 1h              # optional: wake it after this long asleep
      woc.wake_on_notify: jobs        # optional: Postgres NOTIFY channel(s) that wake it
      woc.after: api                  # optional: start after these containers (compose depends_on is read on its own)
```

Containers without `woc.enable=true` are never touched. A group member without `woc.host`
(an internal API, say) starts and stops with the group but receives no traffic from woc.

`woc.ready: port` treats the app as ready the moment its port accepts a connection (probed every
25 ms). Use `woc.ready: healthy` for apps that open their port before they can really serve
(migrations, cache warm-up): WOC then waits for the container's Docker healthcheck as well.

## Start order

When a group wakes, its containers start in tiers: a container starts only once everything it
depends on is ready. WOC reads compose's own `depends_on` (Compose stores it on each container),
so a `db` → `api` → `web` stack wakes in that order with no extra labels. For containers that
don't come from compose, `woc.after` lists the container names to wait for. Dependencies outside
the group are ignored, and so is a cycle (whatever is left starts together).

A request is only forwarded once the whole group is ready, so the web app answering first never
means requests failing because the API or the database aren't up yet.

## Databases and other TCP services

Postgres and Meilisearch don't get their traffic through Traefik: apps connect to them directly.
TCP mode covers them. `woc.tcp: "6432:5432"` makes WOC listen on its own port 6432 and forward
to the container's 5432:

```yaml
services:
  db:
    image: postgres:18
    networks: [data]                  # the network your apps and WOC share
    healthcheck:                      # WOC waits for it: Postgres opens its port before it accepts queries
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 30s
      start_interval: 500ms
    labels:
      woc.enable: "true"
      woc.tcp: "6432:5432"
      woc.ready: postgres             # ask Postgres itself, every 25 ms
      woc.idle: 30m
```

`woc.ready: postgres` checks readiness at the protocol level: WOC sends a startup message and
an authentication request back means Postgres is taking connections, while error `57P03` ("the
database system is starting up") means not yet. It hangs up before authenticating, so it needs no
credentials. Without it, TCP mode waits for the container's Docker healthcheck, which only re-runs
every `start_interval`: measured on a 4 vCPU box, Postgres accepted queries about 130 ms after
`docker start`, and Docker reported it healthy about 550 ms after.

Apps connect to `woc:6432` instead of `db:5432`. They can't keep using the container's own name:
a stopped container drops out of Docker's DNS, and giving WOC the same alias would make DNS split
connections between the two.

- **A connection to a sleeping service waits, it doesn't fail:** WOC starts the container, waits
  until it is running, healthy if it has a healthcheck, and accepting connections, and then
  forwards. The client's own connect timeout applies.
- **An open connection counts as activity**, like a websocket, and WOC never cuts one. The service
  sleeps once every connection has closed and `woc.idle` has passed, so **connection pools must
  close idle connections before `woc.idle`** (pgxpool `MaxConnIdleTime`, node-postgres
  `idleTimeoutMillis`, `database/sql` `SetConnMaxIdleTime`). A pool that holds connections
  forever keeps the database awake forever, which is safe, just not useful.
- **Meilisearch works the same way** (`woc.tcp: "7701:7700"`): it's plain HTTP, forwarded as bytes.
- **Not for everything:** a message broker that clients hold open permanently (NATS) or a cache
  every request touches (Redis) would never sleep, or would pay a start on every cold request.

## Backups and other jobs: holds

A backup that runs `docker exec db pgbackrest …` from cron fails if the database is asleep.
A **hold** wakes a group and keeps it awake for a while, whatever the traffic:

```sh
docker exec woc /woc hold db 3h      # wakes db, waits until ready, keeps it up for at most 3h
docker exec db pgbackrest --stanza=main backup
docker exec woc /woc release db      # sleeps again on its last real traffic
```

Every hold expires on its own (24 h at most), so a job that dies without releasing it costs at
most its duration. Releasing is not activity: a database woken only for a backup goes back to
sleep right after it. Holds are the one piece of state WOC keeps across a restart, in
`WOC_STATE_FILE`, so a long backup never loses its database to a WOC redeploy. The same thing is
available over HTTP: `POST /hold/<group>?for=3h` and `DELETE /hold/<group>`.

Everything else WOC knows is either in Docker (which containers exist, which run, their labels)
or cheap to lose: after a restart, running groups get a full idle window from that moment and
stopped ones wake on their next request.

## Apps that don't receive HTTP

A queue worker never gets a request through WOC, so on its own WOC would put it to sleep and
nothing would wake it, and it could stop it in the middle of a job. Four things cover that, and
they combine:

- **Wake from whoever enqueues:** `POST http://woc:8081/wake/<group>` starts the group (202 at
  once, or 200 once it's ready with `?wait=1`). Your API calls it after it enqueues a job. Works
  with any queue. Set `WOC_ADMIN_TOKEN` to require `Authorization: Bearer <token>`.
- **Don't stop it mid-job:** `woc.busy_url` is checked before every stop. A `200` means busy, and
  WOC asks again on the next reap without resetting the idle clock, so the worker sleeps within
  `WOC_REAP_EVERY` of finishing. Any other answer, or none, means not busy. Leave the host out
  (`http://:4100/busy`) and WOC fills in the container's name.
- **Wake on Postgres NOTIFY:** with `WOC_PG_URL` set, WOC LISTENs on every channel named in a
  `woc.wake_on_notify` label, so a queue that already runs `NOTIFY jobs` on enqueue wakes its
  worker with no change to the producer. It reconnects on its own if Postgres restarts.
- **A safety net:** `woc.wake_every` wakes the group after it has been asleep that long, to drain
  anything that arrived without anyone telling WOC. The worker then sleeps again on its own
  idle rules.

Put the worker in the same `woc.group` as the web app or API that feeds it and it also wakes with
their traffic.

## Run it

[`examples/`](examples) has a complete setup:

- [`examples/docker-compose.yml`](examples/docker-compose.yml): WOC and two
  [docker-socket-proxy](https://github.com/Tecnativa/docker-socket-proxy) instances.
- [`examples/traefik.yml`](examples/traefik.yml): the HTTP provider block for Traefik's static
  configuration.
- [`examples/app/docker-compose.yml`](examples/app/docker-compose.yml): an app that opts in.

### Why two socket proxies

WOC never mounts the Docker socket. It reads through one proxy (`CONTAINERS=1`, `POST=0`: list
and inspect, no writes) and starts and stops through another (`CONTAINERS=0`, `POST=1`,
`ALLOW_START=1`, `ALLOW_STOP=1`: only `POST /containers/<id>/start` and `/stop`). A single
proxy can't express that, because `POST=1` with `CONTAINERS=1` also allows create, delete and
exec.

The read side is still sensitive: inspecting a container returns its environment, secrets
included. Treat WOC like any other component that can see the Docker API.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `DOCKER_HOST` | `unix:///var/run/docker.sock` | Docker API for reads (`tcp://`, `http://` or `unix://`) |
| `DOCKER_WRITE_HOST` | same as `DOCKER_HOST` | Docker API for start and stop |
| `WOC_LISTEN` | `:8080` | proxy port (Traefik sends app traffic here) |
| `WOC_ADMIN_LISTEN` | `:8081` | `/traefik`, `/status`, `/healthz` (keep it off the internet) |
| `WOC_SELF_URL` | `http://woc:8080` | how Traefik reaches the proxy port |
| `WOC_ENTRYPOINT` | `websecure` | Traefik entrypoint for the routers |
| `WOC_CERTRESOLVER` | `le` | Traefik certificate resolver for the routers |
| `WOC_DEFAULT_IDLE` | `30m` | idle window when an app sets no `woc.idle` |
| `WOC_PAGE_AFTER` | `2s` | how long a browser waits before getting the waiting page |
| `WOC_SYNC_EVERY` | `5s` | how often WOC re-reads container labels and states |
| `WOC_REAP_EVERY` | `15s` | how often WOC looks for idle apps |
| `WOC_NODE` | hostname | node name in the waiting page footer |
| `WOC_BRAND` | `Wake On Call` | text before the node in the waiting page footer |
| `WOC_ADMIN_TOKEN` | none | if set, `POST /wake/<group>` needs `Authorization: Bearer <token>` |
| `WOC_PG_URL` | none | Postgres to LISTEN on for `woc.wake_on_notify` (off when empty) |
| `WOC_STATE_FILE` | none | where holds are kept across restarts (put it on a volume) |

## The waiting page

One line in the browser's language (32 languages from `Accept-Language`, right to left for
Arabic and Hebrew, English otherwise), light or dark from the system setting, and a footer with
the node and a [UUIDv7](https://www.rfc-editor.org/rfc/rfc9562#name-uuid-version-7) request id
that WOC also logs:

```
We're waking up the server ...

Wake On Call | my-server::01a1228f-92ed-76da-99c5-32287f59d24e
```

Every host WOC manages serves it at `/.well-known/woc/preview`, so you can see it without
putting an app to sleep.

## Operating it

```sh
curl -s http://woc:8081/status    # each group: running, ready, in flight, time until it sleeps
curl -s http://woc:8081/traefik   # the dynamic configuration Traefik polls
curl -s -X POST http://woc:8081/wake/myapp?wait=1   # wake a group by hand
docker logs woc                   # JSON lines: "woke" (with how long it took), "woken" (by whom), "slept"
```

## When not to use it

- **Small idle apps:** a sleeping Node or Go process frees a few hundred MB at most. If memory
  isn't short, leave them running.
- **Public sites:** crawlers, monitors and link previews wake them constantly, and the first
  visitor after a quiet spell pays the start.
- **Workers that can't say when they're needed:** a worker woken only by `woc.wake_every` picks
  up work late. Wake it from the producer or from NOTIFY when you can.
- **Latency floors:** `docker start` itself is a few hundred milliseconds that no proxy removes.

## Related

[Sablier](https://github.com/sablierapp/sablier) solves the same problem as a Traefik plugin and
supports more platforms (Swarm, Kubernetes, Podman). WOC is smaller and label-driven, built for
one Docker host.

## License

[MIT](LICENSE)
