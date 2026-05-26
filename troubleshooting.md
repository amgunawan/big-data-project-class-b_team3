## Troubleshooting

### 1) Container Name Already Exists

If `docker compose up --build` fails because a container name is already in use, it usually means there is an existing container from a previous project or Docker Compose setup.

Before starting the services:

1. Check all container names defined in `docker-compose.yml`
2. Open Docker Desktop and check whether containers with the same names already exist
3. If there are duplicate container names, stop and remove the previous containers first:

```bash
docker compose down
```

After the conflicting container has been stopped, run the project again:

```bash
docker compose up --build
```

---

### 2) Port Already in Use

If `docker compose up --build` fails with an error like:

```text
Bind for 0.0.0.0:XXXX failed: port is already allocated
```

It means another application or container is already using the same port.
To fix this issue:

1. Open Docker Desktop
2. Check whether another container is already using the same port
3. Stop or remove the conflicting container

You can also stop all existing containers using:

```bash
docker compose down
```

or check running containers with:

```bash
docker ps
```

After the conflicting container has been stopped, run the project again:

```bash
docker compose up --build
```
