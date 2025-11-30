# Copy to `imports.tf` and fill in real IDs before running `terraform plan`.
# This helps rebuild state for existing resources without re-creating them.

# VPC module (terraform-aws-modules/vpc/aws)
import {
  to = module.vpc.aws_vpc.this[0]
  id = "vpc-0fa0db173c90dd92d"
}
import {
  to = module.vpc.aws_internet_gateway.this[0]
  id = "igw-0331b8832a8d748ea"
}
import {
  to = module.vpc.aws_subnet.public[0]
  id = "subnet-060b55098011d244d"
}
import {
  to = module.vpc.aws_subnet.public[1]
  id = "subnet-0fdf32d2f29ca47fe"
}
import {
  to = module.vpc.aws_route_table.public[0]
  id = "rtb-06ddcac93ce4fbaeb"
}
import {
  to = module.vpc.aws_route.public_internet_gateway[0]
  id = "rtb-06ddcac93ce4fbaeb_0.0.0.0/0"
}
import {
  to = module.vpc.aws_route_table_association.public[0]
  id = "subnet-060b55098011d244d/rtb-06ddcac93ce4fbaeb"
}
import {
  to = module.vpc.aws_route_table_association.public[1]
  id = "subnet-0fdf32d2f29ca47fe/rtb-06ddcac93ce4fbaeb"
}

# SQS module (terraform-aws-modules/sqs/aws)
import {
  to = module.sqs.aws_sqs_queue.this[0]
  id = "https://sqs.us-west-1.amazonaws.com/618170664907/sqs-demo-queue"
}
import {
  to = module.sqs.aws_sqs_queue.dlq[0]
  id = "https://sqs.us-west-1.amazonaws.com/618170664907/sqs-demo-queue-dlq"
}

# ECR
import {
  to = aws_ecr_repository.consumer
  id = "sqs-demo/consumer"
}
import {
  to = aws_ecr_lifecycle_policy.consumer
  id = "sqs-demo/consumer"
}

# CloudWatch Logs
import {
  to = aws_cloudwatch_log_group.consumer
  id = "/aws/ecs/sqs-demo-consumer"
}

# IAM
import {
  to = aws_iam_role.execution
  id = "sqs-demo-execution"
}
import {
  to = aws_iam_role_policy_attachment.execution
  id = "sqs-demo-execution/arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}
import {
  to = aws_iam_role.task
  id = "sqs-demo-task"
}
import {
  to = aws_iam_policy.task
  id = "arn:aws:iam::618170664907:policy/sqs-demo-task-policy"
}
import {
  to = aws_iam_role_policy_attachment.task
  id = "sqs-demo-task/arn:aws:iam::618170664907:policy/sqs-demo-task-policy"
}

# ECS
import {
  to = aws_ecs_cluster.this
  id = "sqs-demo-cluster"
}
import {
  to = aws_ecs_task_definition.consumer
  id = "arn:aws:ecs:us-west-1:618170664907:task-definition/sqs-demo-consumer:6" # family:revision
}
import {
  to = aws_security_group.tasks
  id = "sg-07f1e7c156bbf9e6e"
}
import {
  to = aws_ecs_service.consumer
  id = "sqs-demo-cluster/sqs-demo-service"
}

# DynamoDB
import {
  to = aws_dynamodb_table.message_status
  id = "sqs-demo-message-status"
}
import {
  to = aws_dynamodb_table.message_completed
  id = "sqs-demo-message-completed"
}
