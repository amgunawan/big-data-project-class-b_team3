## Troubleshooting

### 1) Container Name Already Exists

If `docker compose up --build` fails because a container name is already in use, it usually means there is an existing container from a previous project or Docker Compose setup.

Before starting the services:

1. Check all container names defined in `docker-compose.yml`
2. Open Docker Desktop and check whether containers with the same names already exist
3. If there are duplicate container names, stop and remove the previous containers first in the SAME directory as your `docker-compose.yml`:

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

---

### 3) Spark Warning: No Resources Available

If you see this warning during `spark-submit`:

```text
WARN TaskSchedulerImpl: Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources
```

It means Spark cannot find any available worker nodes to execute the job.

Possible Causes:

1. Spark Worker is not running or not registered to the cluster
2. Spark Worker failed to connect to Spark Master
3. Not enough allocated CPU/memory for the worker
4. Docker containers were not started properly or lost connection

#### How to Fix

Restart the Spark cluster services:

```bash
docker compose restart spark-worker spark-master
```

Run the command again:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
    /opt/zomato/jobs/batch_analysis.py
```
