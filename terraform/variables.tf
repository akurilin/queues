variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-west-1"
}

variable "project_name" {
  description = "Base name for created resources"
  type        = string
  default     = "sqs-demo"
}

variable "queue_visibility_timeout" {
  description = "Visibility timeout for the SQS queue (seconds)"
  type        = number
  default     = 10
}

variable "queue_receive_wait" {
  description = "Long-poll wait time for the SQS queue (seconds)"
  type        = number
  default     = 10
}
