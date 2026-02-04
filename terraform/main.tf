locals {
  name = var.project_name

  scenarios = {
    happy  = "${local.name}-happy"
    crash  = "${local.name}-crash"
    dup    = "${local.name}-dup"
    business = "${local.name}-business-idempotency"
    poison       = "${local.name}-poison"
    backpressure = "${local.name}-backpressure"
    fifo_order  = "${local.name}-fifo-order"
  }

  env_file_path = "${path.module}/../.env"
}

data "aws_region" "current" {}

# --- SQS queues (one per scenario) ---

module "sqs" {
  source   = "terraform-aws-modules/sqs/aws"
  version  = "~> 4.2"
  for_each = local.scenarios

  name = each.key == "fifo_order" ? "${each.value}-queue.fifo" : "${each.value}-queue"

  create_dlq = true
  fifo_queue = each.key == "fifo_order"
  content_based_deduplication = each.key == "fifo_order"
  dlq_name = each.key == "fifo_order" ? "${each.value}-dlq.fifo" : null

  visibility_timeout_seconds = var.queue_visibility_timeout
  receive_wait_time_seconds  = var.queue_receive_wait
  redrive_policy = {
    maxReceiveCount = 2
  }
}

# --- DynamoDB tables (two per scenario) ---

resource "aws_dynamodb_table" "message_status" {
  for_each = local.scenarios

  name         = "${each.value}-message-status"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "message_id"

  attribute {
    name = "message_id"
    type = "S"
  }
}

resource "aws_dynamodb_table" "message_side_effects" {
  for_each = local.scenarios

  name         = "${each.value}-message-side-effects"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "message_id"

  attribute {
    name = "message_id"
    type = "S"
  }
}

# --- .env for ad-hoc local testing (uses the happy-path resources) ---

resource "local_file" "env" {
  filename = local.env_file_path
  content  = <<EOT
QUEUE_URL=${module.sqs["happy"].queue_url}
DLQ_ARN=${module.sqs["happy"].dead_letter_queue_arn}
AWS_REGION=${var.aws_region}
MESSAGE_STATUS_TABLE=${aws_dynamodb_table.message_status["happy"].name}
MESSAGE_SIDE_EFFECTS_TABLE=${aws_dynamodb_table.message_side_effects["happy"].name}
EOT
}
