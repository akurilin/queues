locals {
  name            = var.project_name
  container_name  = "${var.project_name}-consumer"
  env_file_path   = "${path.module}/../.env"
  ecr_repo_name   = "${var.project_name}/consumer"
  container_image = "${aws_ecr_repository.consumer.repository_url}:${var.container_image_tag}"
  status_table    = "${var.project_name}-message-status"
  completed_table = "${var.project_name}-message-completed"
}

data "aws_caller_identity" "current" {}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_region" "current" {}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.1"

  name = "${local.name}-vpc"
  cidr = var.vpc_cidr

  azs                = slice(data.aws_availability_zones.available.names, 0, 2)
  public_subnets     = [cidrsubnet(var.vpc_cidr, 4, 0), cidrsubnet(var.vpc_cidr, 4, 1)]
  enable_nat_gateway = false
  single_nat_gateway = false

  enable_dns_support   = true
  enable_dns_hostnames = true
}

module "sqs" {
  source  = "terraform-aws-modules/sqs/aws"
  version = "~> 4.2"

  name = "${local.name}-queue"

  create_dlq = true

  visibility_timeout_seconds = var.queue_visibility_timeout
  receive_wait_time_seconds  = var.queue_receive_wait
  redrive_policy = {
    maxReceiveCount = 5
  }
}

resource "aws_cloudwatch_log_group" "consumer" {
  name              = "/aws/ecs/${local.container_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_ecr_repository" "consumer" {
  name                 = local.ecr_repo_name
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "consumer" {
  repository = aws_ecr_repository.consumer.name
  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 5 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 5
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

resource "aws_iam_role" "execution" {
  name = "${local.name}-execution"

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

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "task" {
  name = "${local.name}-task"

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
  name   = "${local.name}-task-policy"
  policy = data.aws_iam_policy_document.task.json
}

resource "aws_iam_role_policy_attachment" "task" {
  role       = aws_iam_role.task.name
  policy_arn = aws_iam_policy.task.arn
}

resource "aws_ecs_cluster" "this" {
  name = "${local.name}-cluster"
}

resource "aws_security_group" "tasks" {
  name        = "${local.name}-tasks"
  description = "Egress-only security group for consumer tasks"
  vpc_id      = module.vpc.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_ecs_task_definition" "consumer" {
  family                   = local.container_name
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = tostring(var.container_cpu)
  memory                   = tostring(var.container_memory)
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([
    {
      name      = local.container_name
      image     = local.container_image
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
          awslogs-group         = aws_cloudwatch_log_group.consumer.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])
}

resource "aws_ecs_service" "consumer" {
  name            = "${local.name}-service"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.consumer.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = module.vpc.public_subnets
    security_groups  = [aws_security_group.tasks.id]
    assign_public_ip = true
  }

  depends_on = [aws_security_group.tasks]
}

resource "local_file" "env" {
  filename = local.env_file_path
  content  = <<EOT
QUEUE_URL=${module.sqs.queue_url}
DLQ_ARN=${module.sqs.dead_letter_queue_arn}
ECR_REPO=${aws_ecr_repository.consumer.repository_url}
AWS_REGION=${var.aws_region}
MESSAGE_STATUS_TABLE=${aws_dynamodb_table.message_status.name}
MESSAGE_COMPLETED_TABLE=${aws_dynamodb_table.message_completed.name}
EOT
}

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
