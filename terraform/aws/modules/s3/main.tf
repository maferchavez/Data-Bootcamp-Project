resource "aws_s3_bucket" "bucket" {
  bucket = var.bucket

  versioning {
    enabled = var.versioning
  }

  tags = {
    Name = "s3-data-bootcamp"
  }
}