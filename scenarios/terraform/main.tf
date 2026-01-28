locals {
  container_name  = "${var.scenario_name}-consumer"
  status_table    = "${var.scenario_name}-message-status"
  completed_table = "${var.scenario_name}-message-completed"
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# --- SQS ---

module "sqs" {
  source  = "terraform-aws-modules/sqs/aws"
  version = "~> 4.2"

  name = "${var.scenario_name}-queue"

  create_dlq = true

  visibility_timeout_seconds = var.queue_visibility_timeout
  receive_wait_time_seconds  = var.queue_receive_wait
  redrive_policy = {
    maxReceiveCount = 5
  }
}

# --- DynamoDB ---

resource "aws_dynamodb_table" "message_status" {
  name         = local.status_table
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "message_id"

  attribute {
    name = "message_id"
    type = "S"
  }
}

resource "aws_dynamodb_table" "message_completed" {
  name         = local.completed_table
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "message_id"

  attribute {
    name = "message_id"
    type = "S"
  }
}

# --- Task IAM role (scoped to per-scenario resources) ---

resource "aws_iam_role" "task" {
  name = "${var.scenario_name}-task"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

data "aws_iam_policy_document" "task" {
  statement {
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:ChangeMessageVisibility",
      "sqs:GetQueueAttributes",
      "sqs:GetQueueUrl",
      "sqs:SendMessage",
      "sqs:ListDeadLetterSourceQueues"
    ]
    resources = [
      module.sqs.queue_arn,
      module.sqs.dead_letter_queue_arn
    ]
  }

  statement {
    actions = [
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:TransactWriteItems",
      "dynamodb:GetItem"
    ]
    resources = [
      aws_dynamodb_table.message_status.arn,
      aws_dynamodb_table.message_completed.arn
    ]
  }
}

resource "aws_iam_policy" "task" {
  name   = "${var.scenario_name}-task-policy"
  policy = data.aws_iam_policy_document.task.json
}

resource "aws_iam_role_policy_attachment" "task" {
  role       = aws_iam_role.task.name
  policy_arn = aws_iam_policy.task.arn
}

# --- ECS ---

resource "aws_ecs_cluster" "this" {
  name = "${var.scenario_name}-cluster"
}

resource "aws_ecs_task_definition" "consumer" {
  family                   = local.container_name
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = tostring(var.container_cpu)
  memory                   = tostring(var.container_memory)
  execution_role_arn       = var.execution_role_arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([
    {
      name      = local.container_name
      image     = var.container_image
      essential = true
      environment = [
        { name = "QUEUE_URL", value = module.sqs.queue_url },
        { name = "AWS_REGION", value = var.aws_region },
        { name = "MESSAGE_STATUS_TABLE", value = local.status_table },
        { name = "MESSAGE_COMPLETED_TABLE", value = local.completed_table }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = var.log_group_name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])
}
