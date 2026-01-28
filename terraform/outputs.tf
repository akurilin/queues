output "queue_url" {
  value       = module.sqs.queue_url
  description = "Primary SQS queue URL"
}

output "queue_arn" {
  value       = module.sqs.queue_arn
  description = "Primary SQS queue ARN"
}

output "dlq_arn" {
  value       = module.sqs.dead_letter_queue_arn
  description = "Dead letter queue ARN"
}

output "ecr_repository_url" {
  value       = aws_ecr_repository.consumer.repository_url
  description = "ECR repository URL for the consumer image"
}

output "ecs_cluster_name" {
  value       = aws_ecs_cluster.this.name
  description = "ECS cluster name"
}

output "ecs_service_name" {
  value       = aws_ecs_service.consumer.name
  description = "ECS service name"
}

output "log_group_name" {
  value       = aws_cloudwatch_log_group.consumer.name
  description = "CloudWatch Logs group for the consumer"
}

output "message_status_table" {
  value       = aws_dynamodb_table.message_status.name
  description = "DynamoDB table for message status tracking"
}

output "message_completed_table" {
  value       = aws_dynamodb_table.message_completed.name
  description = "DynamoDB table for completion idempotency"
}

output "subnet_ids" {
  value       = module.vpc.public_subnets
  description = "Public subnet IDs from the VPC"
}

output "security_group_ids" {
  value       = [aws_security_group.tasks.id]
  description = "Security group IDs for ECS tasks"
}

output "execution_role_arn" {
  value       = aws_iam_role.execution.arn
  description = "ECS task execution role ARN"
}
