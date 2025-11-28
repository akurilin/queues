# SQS Demo

Minimal-but-complete SQS workflow: Terraform builds VPC + SQS (with DLQ) + ECR + ECS Fargate consumer, and a local producer enqueues messages for experiments.

## Prerequisites
- AWS CLI configured with a profile that can create VPC/ECR/ECS/SQS
- Terraform >= 1.5
- Docker
- Python 3.11+ for producer script

## Quickstart
1) (Optional) load defaults and any existing `.env` outputs
```bash
source ./scripts/set_env.sh
```
2) Provision infrastructure (local state)
```bash
terraform -chdir=terraform init
terraform -chdir=terraform apply -auto-approve
```
3) Build and push consumer image (uses ECR URL from Terraform output if `ECR_REPO` unset)
```bash
IMAGE_TAG=latest ./scripts/build_and_push.sh
```
4) Point the service at the pushed tag (if you changed `IMAGE_TAG`)
```bash
terraform -chdir=terraform apply -auto-approve -var container_image_tag=${IMAGE_TAG:-latest}
```
5) Run the producer locally
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r producer/requirements.txt
QUEUE_URL=$(terraform -chdir=terraform output -raw queue_url)
python producer/produce.py --n 25 --rate 5 --queue-url "$QUEUE_URL"
```

Terraform emits `.env` at repo root with `QUEUE_URL`, `DLQ_ARN`, `ECR_REPO`, and `AWS_REGION` for convenience.

## Current deployed outputs (us-west-1)
- `queue_url`: https://sqs.us-west-1.amazonaws.com/618170664907/sqs-demo-queue
- `queue_arn`: arn:aws:sqs:us-west-1:618170664907:sqs-demo-queue
- `dlq_arn`: arn:aws:sqs:us-west-1:618170664907:sqs-demo-queue-dlq
- `ecr_repository_url`: 618170664907.dkr.ecr.us-west-1.amazonaws.com/sqs-demo/consumer
- `log_group_name`: /aws/ecs/sqs-demo-consumer
- `ecs_cluster_name`: sqs-demo-cluster
- `ecs_service_name`: sqs-demo-service

## Consumer knobs (env vars)
- `QUEUE_URL` (required) – queue to poll
- `AWS_REGION` – region override (defaults to AWS SDK resolution)
- `WAIT_TIME_SECONDS` (default 10) – long-poll wait
- `MAX_MESSAGES` (default 5, capped at 10)
- `SLEEP_MIN_SECONDS` / `SLEEP_MAX_SECONDS` (default 0.1–1.0) – work duration window
- `LONG_SLEEP_SECONDS` + `LONG_SLEEP_EVERY` – simulate visibility timeout overruns (e.g., sleep 10s every 3rd message)
- `CRASH_RATE` – probability 0–1 of intentional crash per message
- `IDEMPOTENCY_CACHE_SIZE` – track last N IDs to drop duplicates (0 disables)
- `LOG_LEVEL` – INFO by default

## Producer usage
```bash
python producer/produce.py --n 100 --rate 20 --queue-url "$QUEUE_URL" --profile "$AWS_PROFILE" --region "$AWS_REGION"
```
- `--rate` limits messages/sec (0 = as fast as possible)
- `--batch-size` (max 10) controls batch payload per API call

## Failure-mode experiments
- Set `LONG_SLEEP_SECONDS=10 LONG_SLEEP_EVERY=2` → expect visibility overruns and duplicates
- Set `CRASH_RATE=0.2` → container exits intermittently; ECS restarts, messages redeliver
- Scale service: `terraform -chdir=terraform apply -auto-approve -var desired_count=3` → watch backlog drain
- Burst load: `producer --n 1000 --rate 0` → observe SQS metrics and processing throughput
- Kill task from console while processing → message should reappear due to no delete

## Tear down
```bash
terraform -chdir=terraform destroy -auto-approve
```
