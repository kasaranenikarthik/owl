variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "project_name" {
  type    = string
  default = "video-pipeline"
}

variable "key_pair_name" {
  type        = string
  description = "Name of your existing EC2 key pair (created in AWS Console)"
}

variable "instance_type" {
  type        = string
  default     = "g4dn.xlarge"
  description = "EC2 instance type. Use g4dn.xlarge for GPU, or t3.xlarge for CPU-only fallback"
}

# ── AWS Academy / LabRole ──
# In Learner Lab the pre-created instance profile is usually "LabInstanceProfile".
# If terraform errors on this, go to IAM → Roles → LabRole → Trust relationships
# and look for the instance profile name, or check IAM → Instance Profiles.
variable "iam_instance_profile_name" {
  type        = string
  default     = "LabInstanceProfile"
  description = "IAM instance profile for EC2. AWS Academy typically has 'LabInstanceProfile'."
}

variable "use_gpu" {
  type        = bool
  default     = true
  description = "Set false for CPU-only fallback (if GPU quota is 0)"
}
