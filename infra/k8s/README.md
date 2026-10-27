# Kubernetes deployment (starting point)

For the course cluster. A reasonable first draft, **not validated on a real cluster and not
production-grade**: single replicas for the data services, placeholder image names, no
network policies, no ingress.

| What | How |
|------|-----|
| Kafka (KRaft) and Kafka Connect | Strimzi operator ([helm-values/strimzi-operator.yaml](helm-values/strimzi-operator.yaml)), custom resources in `manifests/10-kafka.yaml` and `12-kafka-connect.yaml` |
| Cassandra | K8ssandra operator ([helm-values/k8ssandra-operator.yaml](helm-values/k8ssandra-operator.yaml)), `manifests/30-data-services.yaml` |
| Prometheus and Grafana | kube-prometheus-stack ([helm-values/kube-prometheus-stack.yaml](helm-values/kube-prometheus-stack.yaml)) |
| Schema Registry, ksqlDB, Redis, HDFS | plain manifests (no Strimzi equivalent; no Bitnami charts, see docs/decisions.md) |
| Our services | plain manifests in `manifests/40-apps.yaml` |
| Training, analytics, evaluation | CronJobs in `manifests/cronjobs/spark-jobs.yaml` (suspended until the jobs are implemented) |

Chart and operator versions are in `versions.env` (`STRIMZI_VERSION`, `K8SSANDRA_OPERATOR_VERSION`,
`KUBE_PROMETHEUS_STACK_VERSION`). Image tags inside the manifests repeat the versions of `versions.env`;
change both together.

## Order

```bash
source versions.env
kubectl apply -f infra/k8s/manifests/00-namespace.yaml

# operators
helm install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator --version $STRIMZI_VERSION \
  -n port-congestion -f infra/k8s/helm-values/strimzi-operator.yaml
helm repo add k8ssandra https://helm.k8ssandra.io/stable
helm install k8ssandra-operator k8ssandra/k8ssandra-operator --version $K8SSANDRA_OPERATOR_VERSION \
  -n port-congestion -f infra/k8s/helm-values/k8ssandra-operator.yaml

# config and secrets (nothing secret is stored in the repo)
kubectl -n port-congestion create configmap pc-config --from-file=config/
kubectl -n port-congestion create configmap ksql-statements --from-file=ksql/
kubectl -n port-congestion create configmap cassandra-schema --from-file=cassandra/schema.cql
kubectl -n port-congestion create secret generic pc-secrets \
  --from-literal=AISSTREAM_API_KEY=... --from-literal=DMI_API_KEY=... --from-literal=ALERT_WEBHOOK_URL=...

# data services, then Kafka Connect, Schema Registry, ksqlDB
kubectl apply -f infra/k8s/manifests/10-kafka.yaml -f infra/k8s/manifests/30-data-services.yaml
kubectl apply -f infra/k8s/manifests/20-schema-registry-ksqldb.yaml -f infra/k8s/manifests/12-kafka-connect.yaml

# schemas and connectors (port-forward Schema Registry and Connect first)
python3 scripts/register_schemas.py --url http://localhost:8081
CONNECT_URL=http://localhost:8083 scripts/deploy_connectors.sh

# our services and the (suspended) jobs
kubectl apply -f infra/k8s/manifests/40-apps.yaml -f infra/k8s/manifests/cronjobs/spark-jobs.yaml

# monitoring
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install monitoring prometheus-community/kube-prometheus-stack --version $KUBE_PROMETHEUS_STACK_VERSION \
  -n port-congestion -f infra/k8s/helm-values/kube-prometheus-stack.yaml
kubectl -n port-congestion create configmap feed-lag-dashboard \
  --from-file=infra/monitoring/grafana/dashboards/feed-lag-and-rate.json
kubectl -n port-congestion label configmap feed-lag-dashboard grafana_dashboard=1
```

## Images

Build with the repo root as context, push to a registry the cluster can pull from, and replace
`registry.example.org/port-congestion` in the manifests:

```bash
docker build -f services/ais-producer/Dockerfile -t REGISTRY/ais-producer:latest .
docker build -f services/api/Dockerfile          -t REGISTRY/api:latest .
docker build -t REGISTRY/dashboard:latest services/dashboard
docker build -f spark/Dockerfile --build-arg SPARK_IMAGE_TAG=$SPARK_IMAGE_TAG -t REGISTRY/spark:4.2.0 .
```

## Known gaps

- The connector configs use compose host names (`cassandra`, `redis`, `namenode`, `schema-registry`)
  which exist here as Services. `connect/filepulse-dma-source.json` also says `kafka:29092`
  (the compose listener); on Strimzi use `pc-kafka-kafka-bootstrap:9092`. Historical replay is
  mostly a laptop job, so this was left alone.
- The Spark CronJobs use `spark-submit --packages`, which needs `spark.kubernetes.file.upload.path`
  to point at storage the executors can read (HDFS here). Baking the jars into the image is the
  alternative.
- Strimzi builds the Connect image from the plugin list. The Avro converter and its dependencies
  are the part most likely to need adjusting.
- No Ingress, TLS, authentication or resource tuning.
