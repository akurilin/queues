# SQS Demo

Minimal-but-complete SQS workflow. Terraform creates:
- VPC (public subnets)
- SQS queue + DLQ
- ECR repo for the consumer image
- ECS Fargate cluster/service/task for the consumer
- CloudWatch log group

Repo contains:
- `consumer/`: Python SQS consumer (Dockerized) with failure-mode knobs.
- `producer/`: Local Python script to enqueue messages.
- `scripts/`: Build/push helper, env helper.
- `terraform/`: Infra definitions (local state).

## Prerequisites
- AWS CLI configured with a profile that can create VPC/ECR/ECS/SQS
- Terraform >= 1.5
- Docker (can build for amd64; script defaults to `--platform linux/amd64`)
- Python 3.11+ for the producer script

## How to use this repo end-to-end
1) **(Optional) load env helpers**  
   `source ./scripts/set_env.sh`

2) **Provision infrastructure** (local Terraform state in `terraform/`)  
   ```bash
   terraform -chdir=terraform init
   terraform -chdir=terraform apply -auto-approve
   ```
   This writes a `.env` at repo root with `QUEUE_URL`, `DLQ_ARN`, `ECR_REPO`, `AWS_REGION`.

3) **Build and push the consumer image**  
   ```bash
   IMAGE_TAG=latest ./scripts/build_and_push.sh
   ```
   - Uses `ECR_REPO` from `.env` or Terraform output.
   - Builds for amd64 by default (`PLATFORM=linux/amd64`), so Fargate can run it.

4) **Point ECS at the pushed tag (if not `latest`)**  
   ```bash
   terraform -chdir=terraform apply -auto-approve -var container_image_tag=${IMAGE_TAG:-latest}
   ```

5) **Run the producer locally to send messages**  
   ```bash
   python -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   QUEUE_URL=$(terraform -chdir=../terraform output -raw queue_url)
   python produce.py --n 25 --rate 5 --queue-url "$QUEUE_URL"
   ```

6) **Watch the consumer**  
   - Logs: `aws logs tail /aws/ecs/sqs-demo-consumer --follow`
   - ECS service: `aws ecs describe-services --cluster sqs-demo-cluster --services sqs-demo-service`
   - Scale: `terraform -chdir=terraform apply -auto-approve -var desired_count=3`

7) **Tear down**  
   ```bash
   terraform -chdir=terraform destroy -auto-approve
   ```

## What the consumer does
- Long-polls SQS, logs payloads, deletes after processing.
- Chaos knobs via env vars:
  - `WAIT_TIME_SECONDS` and `MAX_MESSAGES` (capped at 10)
  - `SLEEP_MIN_SECONDS` / `SLEEP_MAX_SECONDS`
  - `LONG_SLEEP_SECONDS` + `LONG_SLEEP_EVERY` to force visibility overruns
  - `CRASH_RATE` to randomly crash the task
  - `IDEMPOTENCY_CACHE_SIZE` to drop duplicates (in-memory)
  - `LOG_LEVEL`
  
  Default values for all environment variables are defined in `consumer/consume.py` (see the `env_int`, `env_float`, and `get_log_level` functions). Refer to the app code for the authoritative default values.

## Producer usage
```bash
python producer/produce.py --n 100 --rate 20 --queue-url "$QUEUE_URL" --profile "$AWS_PROFILE" --region "$AWS_REGION"
```
- `--rate` limits messages/sec (`0` = as fast as possible)
- `--batch-size` (max 10) controls batch size per API call

## Failure-mode experiments
- Visibility overrun: `LONG_SLEEP_SECONDS=10 LONG_SLEEP_EVERY=2`
- Crashy worker: `CRASH_RATE=0.2` (ECS restarts; messages redeliver)
- Scale out: `terraform -chdir=terraform apply -auto-approve -var desired_count=3`
- Burst load: `producer --n 1000 --rate 0`
- Kill a task mid-run: message should redeliver (not deleted)

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
- `WAIT_TIME_SECONDS` – long-poll wait
- `MAX_MESSAGES` (capped at 10)
- `SLEEP_MIN_SECONDS` / `SLEEP_MAX_SECONDS` – work duration window
- `LONG_SLEEP_SECONDS` + `LONG_SLEEP_EVERY` – simulate visibility timeout overruns (e.g., sleep 10s every 3rd message)
- `CRASH_RATE` – probability 0–1 of intentional crash per message
- `IDEMPOTENCY_CACHE_SIZE` – track last N IDs to drop duplicates (0 disables)
- `LOG_LEVEL`

Default values for all environment variables are defined in `consumer/consume.py` (see the `env_int`, `env_float`, and `get_log_level` functions). Refer to the app code for the authoritative default values.

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
