.PHONY: help preflight infra-up infra-down infra-validate validate \
       scenario-happy scenario-crash scenario-dup scenario-business scenario-poison scenario-poison-partial-batch scenario-dup-side-effects scenario-dup-graceful-shutdown scenario-fifo-order scenario-version-order scenario-happy-purge-timing scenario-backpressure \
       scenarios scenarios-fast scenarios-slow venv

ARGS ?=

VENV := .venv
VENV_STAMP := $(VENV)/.installed

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-22s %s\n", $$1, $$2}'

preflight: $(VENV_STAMP) ## Check local environment for required tools and services
	@printf '%-40s' 'terraform is installed…' ; command -v terraform > /dev/null 2>&1 \
		&& echo '[ok]' \
		|| { echo '[FAIL]  Install terraform: https://developer.hashicorp.com/terraform/install'; exit 1; }
	@printf '%-40s' 'aws CLI is installed…' ; command -v aws > /dev/null 2>&1 \
		&& echo '[ok]' \
		|| { echo '[FAIL]  Install the AWS CLI: https://aws.amazon.com/cli/'; exit 1; }
	@printf '%-40s' 'git is installed…' ; command -v git > /dev/null 2>&1 \
		&& echo '[ok]' \
		|| { echo '[FAIL]  Install git'; exit 1; }
	@printf '%-40s' 'python3 is installed…' ; command -v python3 > /dev/null 2>&1 \
		&& echo '[ok]' \
		|| { echo '[FAIL]  Install Python 3'; exit 1; }
	@printf '%-40s' 'AWS credentials are valid…' ; aws sts get-caller-identity > /dev/null 2>&1 \
		&& echo '[ok]' \
		|| { echo '[FAIL]  AWS session expired or not configured — run: aws sso login'; exit 1; }
	@echo '[ok]  Python venv is up to date (handled by dependency)'

# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------

infra-up: ## Provision infrastructure with Terraform
	terraform -chdir=terraform init
	terraform -chdir=terraform apply -auto-approve $(ARGS)

infra-down: ## Destroy infrastructure with Terraform
	terraform -chdir=terraform destroy -auto-approve $(ARGS)

infra-validate: ## Validate Terraform config
	terraform -chdir=terraform init -backend=false
	terraform -chdir=terraform validate

# ---------------------------------------------------------------------------
# Validation & scenarios
# ---------------------------------------------------------------------------

validate: $(VENV_STAMP) ## Validate all scenario infra is healthy (live AWS checks)
	$(VENV)/bin/python scenarios/validate_infra.py $(ARGS)

# The root .venv is shared across scenarios and validation scripts — all of
# them only need boto3.  The stamp file means pip install only re-runs when
# requirements.txt changes.  Every scenario target depends on the stamp so
# you can just `make scenario-happy` without thinking about setup.
venv: $(VENV_STAMP) ## Create/update the project venv

$(VENV_STAMP): scenarios/requirements.txt
	python -m venv $(VENV)
	$(VENV)/bin/pip install -r scenarios/requirements.txt
	@touch $@

# --- Fast scenarios (seconds) ---

scenario-happy: $(VENV_STAMP) ## Run the happy-path scenario
	$(VENV)/bin/python scenarios/run.py happy $(ARGS)

scenario-crash: $(VENV_STAMP) ## Run the crash scenario
	$(VENV)/bin/python scenarios/run.py crash $(ARGS)

scenario-dup: $(VENV_STAMP) ## Run the duplicate delivery scenario
	$(VENV)/bin/python scenarios/run.py dup $(ARGS)

scenario-business: $(VENV_STAMP) ## Run the business idempotency scenario
	$(VENV)/bin/python scenarios/run.py business $(ARGS)

scenario-poison: $(VENV_STAMP) ## Run the poison message scenario
	$(VENV)/bin/python scenarios/run.py poison $(ARGS)

scenario-poison-partial-batch: $(VENV_STAMP) ## Run the partial batch failure scenario
	$(VENV)/bin/python scenarios/run.py poison-partial-batch $(ARGS)

scenario-dup-side-effects: $(VENV_STAMP) ## Run the transactional side effects scenario
	$(VENV)/bin/python scenarios/run.py dup-side-effects $(ARGS)

scenario-dup-graceful-shutdown: $(VENV_STAMP) ## Run the graceful shutdown scenario
	$(VENV)/bin/python scenarios/run.py dup-graceful-shutdown $(ARGS)

scenario-fifo-order: $(VENV_STAMP) ## Run the FIFO ordering scenario
	$(VENV)/bin/python scenarios/run.py fifo-order $(ARGS)

scenario-version-order: $(VENV_STAMP) ## Run the versioning order scenario
	$(VENV)/bin/python scenarios/run.py version-order $(ARGS)

scenario-happy-purge-timing: $(VENV_STAMP) ## Run the SQS purge timing scenario (~70s, tests 60s danger window)
	$(VENV)/bin/python scenarios/run.py happy-purge-timing $(ARGS)

scenarios-fast: scenario-happy scenario-crash scenario-dup scenario-business scenario-poison scenario-poison-partial-batch scenario-dup-side-effects scenario-dup-graceful-shutdown scenario-fifo-order scenario-version-order ## Run fast scenarios only

# --- Slow scenarios (minutes) ---

scenario-backpressure: $(VENV_STAMP) ## Run the backpressure / scaling scenario (slow)
	$(VENV)/bin/python scenarios/run.py backpressure $(ARGS)

scenarios-slow: scenario-backpressure scenario-happy-purge-timing ## Run slow scenarios only

# --- All ---

scenarios: scenarios-fast scenarios-slow ## Run all scenarios (fast then slow)
