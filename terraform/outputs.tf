output "scenarios" {
  description = "Per-scenario resource references"
  value = {
    for key, prefix in local.scenarios : key => {
      queue_url                 = module.sqs[key].queue_url
      dlq_arn                   = module.sqs[key].dead_letter_queue_arn
      message_status_table      = aws_dynamodb_table.message_status[key].name
      message_side_effects_table = aws_dynamodb_table.message_side_effects[key].name
    }
  }
}

output "aws_region" {
  value       = var.aws_region
  description = "AWS region used for all resources"
}
