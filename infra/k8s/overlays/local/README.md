# Spark on the local OrbStack cluster

Runs Spark jobs as pods on OrbStack's Kubernetes on your Mac, with no registry and no school
cluster. It deploys only the Spark RBAC from `base/spark` (no Kafka, HDFS and so on) and two test
Jobs, in namespace `bd-gr-05`.

**Every command passes `--context orbstack`.** Your default context may point at the school
cluster. Never use `cluster/env.sh`, `deploy.sh` or `check.sh` with this overlay.

## What happens when a Job runs

1. The Job starts a small **submitter** pod that only runs `spark-submit` in cluster mode.
2. `spark-submit` asks Kubernetes for a **driver** pod (`<name>-…-driver`): your program.
3. The driver asks for 2 **executor** pods, which do the work.
4. When the program ends, the driver and executor pods stay behind as `Completed`, so you can
   read their logs. They are not owned by the Job; see [Clean up](#clean-up).

Shared settings (master, image, memory, 2 executors) are in [spark-defaults.conf](spark-defaults.conf).

## One-time setup

Turn on Kubernetes in OrbStack (Settings → Kubernetes). Then check that the `orbstack` context
exists:

```bash
kubectl config get-contexts
```

## Build the image

OrbStack's Kubernetes uses the same Docker engine as `docker`, so a local build is on the
cluster right away (the pods use `imagePullPolicy: Never`). From the repo root:

```bash
docker build -f spark/Dockerfile --build-arg SPARK_IMAGE_TAG=4.2.0-scala2.13-java21-python3-ubuntu -t portcongestion/spark:4.2.0 .
```

Rebuild after changing anything under `spark/`.

## Apply the namespace, RBAC and settings

This applies everything except the Jobs (objects without a `spark-job` label):

```bash
kubectl --context orbstack apply -k infra/k8s/overlays/local -l '!spark-job'
```

## Run a job

Run one at a time, so it fits on a laptop:

```bash
kubectl --context orbstack apply -k infra/k8s/overlays/local -l spark-job=spark-pi
```

```bash
kubectl --context orbstack apply -k infra/k8s/overlays/local -l spark-job=smoke-count
```

Watch the pods come and go:

```bash
kubectl --context orbstack -n bd-gr-05 get pods -w
```

## Check the result

The Job only shows that `spark-submit` finished. **The result is the driver pod's phase**:
`Succeeded` means the program worked, `Failed` means it crashed.

```bash
kubectl --context orbstack -n bd-gr-05 get pods -l spark-role=driver -L spark-job
```

Driver log (the program's output: `Pi is roughly …`, or `rows 20`):

```bash
kubectl --context orbstack -n bd-gr-05 logs -l spark-role=driver,spark-job=smoke-count --tail=-1
```

Submitter log (what `spark-submit` saw):

```bash
kubectl --context orbstack -n bd-gr-05 logs job/smoke-count
```

## Run a job again

A Job runs once and can't be changed. Delete it, then apply it again:

```bash
kubectl --context orbstack -n bd-gr-05 delete job smoke-count
```

## Clean up

Driver pods are created by `spark-submit`, not by the Job, so deleting a Job leaves its driver
pod behind. Executor pods are owned by their driver pod, so deleting a driver pod removes its
executors too. Remove the old driver pods of one job, or of all jobs:

```bash
kubectl --context orbstack -n bd-gr-05 delete pods -l spark-role=driver,spark-job=smoke-count
```

```bash
kubectl --context orbstack -n bd-gr-05 delete pods -l spark-role=driver
```

Remove everything this overlay created, namespace included:

```bash
kubectl --context orbstack delete -k infra/k8s/overlays/local
```
