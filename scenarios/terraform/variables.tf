variable "scenario_name" {
  description = "Unique prefix for per-scenario resources"
  type        = string
}

variable "container_image" {
  description = "Full ECR image URI (repo:tag)"
  type        = string
}

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-west-1"
}

variable "subnets" {
  description = "Subnet IDs from the main VPC"
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security group IDs from the main infrastructure"
  type        = list(string)
}

variable "execution_role_arn" {
  description = "ECS execution role ARN from the main infrastructure"
  type        = string
}

variable "log_group_name" {
  description = "CloudWatch log group name from the main infrastructure"
  type        = string
}

variable "container_cpu" {
  description = "CPU units for the consumer task"
  type        = number
  default     = 256
}

variable "container_memory" {
  description = "Memory (MB) for the consumer task"
  type        = number
  default     = 512
}

variable "queue_visibility_timeout" {
  description = "SQS visibility timeout (seconds)"
  type        = number
  default     = 10
}

variable "queue_receive_wait" {
  description = "SQS long-poll wait time (seconds)"
  type        = number
  default     = 10
}
