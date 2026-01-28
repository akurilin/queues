output "queue_url" {
  value       = module.sqs.queue_url
  description = "Per-scenario SQS queue URL"
}

output "dlq_arn" {
  value       = module.sqs.dead_letter_queue_arn
  description = "Per-scenario dead-letter queue ARN"
}

output "cluster_name" {
  value       = aws_ecs_cluster.this.name
  description = "Per-scenario ECS cluster name"
}

output "task_definition_arn" {
  value       = aws_ecs_task_definition.consumer.arn
  description = "Per-scenario ECS task definition ARN"
}

output "container_name" {
  value       = local.container_name
  description = "Container name in the task definition"
}

output "message_status_table" {
  value       = aws_dynamodb_table.message_status.name
  description = "Per-scenario DynamoDB status table name"
}

output "message_completed_table" {
  value       = aws_dynamodb_table.message_completed.name
  description = "Per-scenario DynamoDB completed table name"
}

output "aws_region" {
  value       = var.aws_region
  description = "AWS region used for this scenario"
}
