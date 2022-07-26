variable "bucket_prefix" {
  type = string
}

variable "acl" {
  type = string
}

variable "versioning" {
  type = bool
}

variable "subnet_s3" {
  #type = string
  description = "Private subnet where the S3 instance is going to be placed"
}

variable "vpc_id_s3" {
  description = "VPC id"
}
