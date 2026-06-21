# Real-Time Log Processing ETL Platform

A production-style observability and data processing pipeline that continuously generates application logs, streams them through Kafka, transforms and stores them using an ETL consumer, exposes operational metrics through Prometheus, and visualizes system health in Grafana.

The platform is fully containerized with Docker Compose and includes Kubernetes manifests for deployment to local or cloud-native environments.

## Key Features

* Real-time log generation using Go
* Event streaming with Apache Kafka
* ETL processing and transformation pipeline
* PostgreSQL storage for structured log data
* Prometheus metrics collection
* Grafana dashboards for monitoring
* Docker Compose local deployment
* Kubernetes deployment manifests
* Horizontal consumer scaling
* GitHub Actions CI/CD pipeline

## Architecture

```text
              Log Generator (Go)
                    |
                    v
                 Kafka
                    |
                    v
             ETL Consumer (Go)
                    |
        +-----------+-----------+
        |                       |
        v                       v
   PostgreSQL             Prometheus
                                |
                                v
                             Grafana
```

## Data Flow

1. The Log Generator continuously produces structured application logs.
2. Kafka acts as the event streaming backbone.
3. The ETL Consumer reads logs from Kafka.
4. Logs are validated and transformed.
5. Structured records are inserted into PostgreSQL.
6. Processing metrics are exposed through Prometheus.
7. Grafana visualizes operational and business metrics.

## Example Log Event

```json
{
  "service": "payment",
  "level": "ERROR",
  "message": "database timeout",
  "timestamp": "2026-06-21T10:00:00Z"
}
```

## Example ETL Transformation

Input:

```json
{
  "service": "payment",
  "level": "ERROR",
  "message": "database timeout",
  "timestamp": "2026-06-21T10:00:00Z"
}
```

Output:

```json
{
  "service": "payment",
  "severity": "critical",
  "date": "2026-06-21"
}
```

## Technology Stack

| Layer            | Technology     |
| ---------------- | -------------- |
| Language         | Go             |
| Event Streaming  | Apache Kafka   |
| Database         | PostgreSQL     |
| Monitoring       | Prometheus     |
| Visualization    | Grafana        |
| Containerization | Docker         |
| Orchestration    | Kubernetes     |
| CI/CD            | GitHub Actions |

## Quick Start (Docker Compose)

### Clone the Repository

```bash
git clone <your-repository-url>
cd ETL-pipeline
```

### Start All Services

```bash
docker compose up --build
```

### Verify Data Flow

```bash
docker exec -it etl-pipeline-postgres-1 \
psql -U postgres -d etl \
-c "SELECT COUNT(*) FROM logs;"
```

The record count should continuously increase as logs are processed.

## Access Services

### Grafana

URL:

```text
http://localhost:3000
```

Credentials:

```text
Username: admin
Password: admin
```

### Prometheus

```text
http://localhost:9090
```

Check targets under:

```text
Status → Targets
```

## Kubernetes Deployment

### Create Cluster

```bash
kind create cluster --name etl
```

### Build Images

```bash
docker build -t etl-producer:latest ./producer
docker build -t etl-consumer:latest ./consumer
```

### Load Images

```bash
kind load docker-image etl-producer:latest --name etl
kind load docker-image etl-consumer:latest --name etl
```

### Create Namespace

```bash
kubectl create namespace etl
```

### Apply Resources

```bash
kubectl apply -f k8s/
```

### Monitor Pods

```bash
kubectl get pods -n etl -w
```

## Database Verification

```bash
kubectl port-forward -n etl pod/postgres-0 5432:5432 &
```

```bash
psql -U postgres -d etl -h localhost -p 5432 \
-c "SELECT COUNT(*) FROM logs;"
```

## Access Grafana

```bash
kubectl port-forward -n etl service/grafana 3000:3000 &
```

Open:

```text
http://localhost:3000
```

## Access Prometheus

```bash
kubectl port-forward -n etl service/prometheus 9090:9090 &
```

Open:

```text
http://localhost:9090
```

## Scaling Consumers

Kafka supports horizontal scaling through consumer groups.

Scale consumers:

```bash
kubectl scale deployment consumer \
--replicas=5 \
-n etl
```

Consumers automatically rebalance partitions and distribute workload.

## Monitoring Metrics

The ETL Consumer exposes:

```text
logs_processed_total
logs_failed_total
```

Example PromQL:

```promql
rate(logs_processed_total[1m])
```

```promql
rate(logs_failed_total[1m])
```

## Grafana Dashboard

The ETL Log Processing dashboard includes:

* Total Logs Processed
* Total Failed Logs
* Processing Rate
* Consumer Health
* Database Throughput
* Error Trends

## CI/CD Pipeline

GitHub Actions automatically:

* Builds Go applications
* Runs validation checks
* Starts Docker Compose
* Verifies database ingestion
* Validates metrics endpoint
* Builds Docker images
* Pushes images on main branch

### Required Secrets

```text
DOCKER_USERNAME
DOCKER_PASSWORD
```

## Project Structure

```text
.
├── producer/
│   ├── Dockerfile
│   ├── go.mod
│   └── main.go
├── consumer/
│   ├── Dockerfile
│   ├── go.mod
│   └── main.go
├── postgres/
│   └── init.sql
├── k8s/
│   ├── namespace.yaml
│   ├── zookeeper.yaml
│   ├── kafka.yaml
│   ├── postgres.yaml
│   ├── producer.yaml
│   ├── consumer.yaml
│   ├── prometheus.yaml
│   └── grafana.yaml
├── grafana/
│   ├── datasources/
│   └── dashboards/
├── .github/workflows/
│   └── ci.yml
├── docker-compose.yml
├── prometheus.yml
└── README.md
```

## Troubleshooting

| Problem                      | Solution                                       |
| ---------------------------- | ---------------------------------------------- |
| Kafka fails to start         | Verify Zookeeper health and inspect Kafka logs |
| PostgreSQL connection issues | Validate connection string and readiness       |
| No records inserted          | Inspect consumer logs and Kafka topic activity |
| Empty Grafana dashboard      | Verify Prometheus datasource configuration     |
| Kind cluster issues          | Increase Docker resource allocation            |

## Future Improvements

* Multi-region Kafka deployment
* Dead-letter queue support
* OpenTelemetry integration
* Distributed tracing
* Alertmanager integration
* Kafka schema registry
* Persistent storage classes
* Cloud deployment (EKS, GKE, AKS)

## Author

**Krishna (Krishna8167)**

Computer Science Engineer specializing in Cloud, DevOps, Backend Systems, and Distributed Systems.

GitHub: https://github.com/Krishna8167

## Maintainer

This project is designed, developed, and maintained by **Krishna8167**.

For questions, suggestions, or contributions, please open an issue or submit a pull request.

---

© 2026 Krishna8167. All rights reserved.



---

Built with Go, Kafka, PostgreSQL, Prometheus, Grafana, Docker, Kubernetes, and GitHub Actions.
