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
