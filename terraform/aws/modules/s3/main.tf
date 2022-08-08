resource "aws_s3_bucket" "bucket" {
  bucket = var.bucket

  versioning {
    enabled = var.versioning
  }

  tags = {
    Name = "s3-data-bootcamp"
  }
}

resource "aws_s3_bucket_object" "object1" {
for_each = fileset("C:/Users/maryf/Documents/Data-bootcamp/Data-Bootcamp-Project/dags/spark_scripts/", "*")
bucket = aws_s3_bucket.bucket.id
key = each.value
source = "C:/Users/maryf/Documents/Data-bootcamp/Data-Bootcamp-Project/dags/spark_scripts/${each.value}"
etag = filemd5("C:/Users/maryf/Documents/Data-bootcamp/Data-Bootcamp-Project/dags/spark_scripts/${each.value}")
}