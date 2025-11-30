SQS Demo Practice Project Plan
==============================

Goals
-----
- Stand up a minimal-but-complete SQS workflow: producers (local), consumers (ECS Fargate), queue + optional DLQ.
- Use Terraform modules, Dockerized consumers, boto3-based Python scripts.
- Keep it simple, local Terraform state, easy to scale consumer count (1–5).

To-Do Tracker
-------------
- [x] Scaffold repo layout (terraform/, consumer/, producer/, scripts/, README.md stub).
- [x] Terraform skeleton: providers/versions, VPC (public-only), SQS + DLQ module, hand-rolled ECS cluster/service/task, ECR repo, IAM roles, CloudWatch log group, outputs (queue URL/ARN, DLQ ARN, image URI), local_file for `.env`.
- [x] Consumer: Dockerfile, consume.py (long-poll loop, failure knobs: crash rate, long sleep to exceed visibility, optional idempotency tracking), requirements.txt.
- [x] Producer: produce.py (N messages, optional rate, UUID payload, CLI args for queue URL/profile).
- [x] Scripts: build_and_push.sh (login, build, tag, push), set_env.sh (optional helpers).
- [x] README: usage (3-command flow), failure-mode experiments, scaling notes.
- [x] Run terraform apply locally (us-west-1) and capture outputs.
- [ ] Build/push consumer image to ECR.
- [ ] Run producer locally against the deployed queue.
- [ ] Execute failure-mode experiments (visibility overrun, crash rate, duplicates, burst load, task kill) and note observations.

Architecture
------------
- AWS Region: us-west-1.
- VPC: new minimal VPC with public subnets only (no NAT) via terraform-aws-modules/vpc to keep infra minimal.
- SQS: Standard queue + DLQ (terraform-aws-modules/sqs-queue). Default settings (visibility, retention, long polling default). Redrive to DLQ on default max receives.
- ECS Fargate: cluster/service/task hand-rolled (no ECS module) running consumer container in public subnets without public inbound (SG egress-only; tasks can get public IP for outbound).
- ECR: repo `sqs-demo/consumer` with lifecycle policy keeping last 5 images.
- IAM: execution role for pulling from ECR + logging; task role scoped to SQS queue (send/receive/delete) and CloudWatch logs.
- Local producer: Python script using boto3, authenticating via local AWS CLI profile.
- Logging/metrics: CloudWatch Logs for consumers; SQS queue metrics. (No CloudWatch alarms for now.)

Repo Layout
-----------
- `terraform/`
  - `main.tf`, `variables.tf`, `outputs.tf`, `providers.tf`, `versions.tf`
  - Modules used: VPC, SQS queue; hand-rolled ECS/Fargate resources; ECR; optional local_file to emit `.env` with outputs
- `consumer/`
  - `Dockerfile`
  - `consume.py` (single-threaded long-poll loop, supports failure simulation)
  - `requirements.txt` (boto3)
- `producer/`
  - `produce.py` (enqueue N messages, optional rate)
- `scripts/`
  - `build_and_push.sh` (build/push image to ECR)
  - `set_env.sh` (helper to export TF_VARs/profile if desired)
- `README.md` (how to run Terraform, build/push, run producer; experiments/failure modes)

Terraform Details
-----------------
- Providers: aws (region us-west-1), local state (no remote backend).
- VPC: small CIDR, 2 AZs, public subnets only (no NAT) to keep setup minimal.
- SQS queue: Standard, DLQ attached, default visibility/retention/long-poll, redrive policy default, outputs for queue URL and DLQ ARN.
- ECS Fargate (hand-rolled):
  - Cluster + service + task definition
  - Desired count variable (default 1, max 5 guidance)
  - Assign to public subnets; allow public IP for outbound; SG egress-only
  - CloudWatch Logs group creation
- ECR: `sqs-demo/consumer`, lifecycle keep last 5 images.
- IAM: execution role (ECR pull, logs), task role (SQS queue + DLQ permissions, logs).
- Outputs: queue URL, DLQ ARN, image URI, and optional local_file writing `.env` for local producer convenience.

Consumer Implementation (Python)
--------------------------------
- Use boto3 SQS client.
- Long-poll receive_message (wait time 10s), max 1–5 messages per poll.
- For each message: parse JSON, simulate work (sleep random short duration), log processed, delete message.
- Handle errors: catch exceptions, avoid deleting failed messages (let SQS retry). Expose failure knobs: random crash rate, optional over-long sleep to exceed visibility timeout, optional idempotency key check (e.g., track last N IDs in memory or via hash to simulate dedupe).
- Environment-driven config: queue URL, wait time, max messages, sleep range, log level, failure rates, long-sleep toggle.

Producer Implementation (Python)
--------------------------------
- CLI options: number of messages N, optional rate (messages/sec), queue URL (or fetch via Terraform output), profile selection.
- Payload: `{ "id": UUID, "work": random number }`.
- Send in batches where possible; log success/failure counts.

Build/Deploy Workflow
---------------------
- Pre-req: install awscli, terraform, docker; configure AWS profile (default).
- Terraform: `terraform init && terraform apply -auto-approve` in `terraform/` (goal: repo runnable with 3 commands).
- Build/push consumer: `./scripts/build_and_push.sh` (docker build, aws ecr get-login-password, docker push).
- Update service: `terraform apply` picks new image tag (passed as variable).
- Run producer locally: `python producer/produce.py --n 100 --rate 5 --queue-url $(terraform output -raw queue_url)`.

Scaling & Tuning
----------------
- Desired count variable controls consumers (default 1, tested up to 5).
- Adjust visibility timeout if work time increases; set to 2–6x max expected processing time.
- Adjust retention or redrive max receives if needed for experiments.

Security & Cost Notes
---------------------
- Principle of least privilege on task role (SQS + logs only).
- Egress-only SG for tasks; no inbound.
- Keep queue Standard (higher throughput, at-least-once). FIFO optional later.
- Costs minimal (Fargate tasks, SQS, CloudWatch). Stop service/destroy stack when done.

Next Steps to Implement
-----------------------
1) Scaffold repo layout and Terraform skeleton with selected modules.
2) Author consumer Dockerfile + consume.py and producer script.
3) Write build/push helper script.
4) Wire Terraform variables/outputs (queue URL/ARN, log group, image URI).
5) Apply infra, push image, update service.
6) Run local producer, observe logs/metrics, test failure modes (visibility timeout overrun, crashes, duplicates) and DLQ behavior.

Failure Mode Experiments (to document in README)
-----------------------------------------------
- Scale consumer count 1→3→5 and observe backlog drain.
- Set visibility timeout low (e.g., 5s), configure consumer to sleep 10s → expect redelivery/duplicates.
- Inject error rate (e.g., 20%) → observe retries and DLQ growth.
- Burst 1000 messages quickly → watch backlog drain and scaling behavior.
- Kill consumer task mid-run → observe message reprocessing.