resource "aws_s3_bucket" "bucket" {
  bucket = var.bucket

  versioning {
    enabled = var.versioning
  }

  tags = {
    Name = "s3-data-bootcamp"
  }
}

resource "aws_s3_bucket_policy" "allow_full_access" {
  bucket = var.bucket
  policy = data.aws_iam_policy_document.allow_full_access.json
}

data "aws_iam_policy_document" "allow_full_access" {
  Version= "2012-10-17"
  Statement=[
      {
          Action= [
              "s3:PutObject",
              "s3:PutObjectAcl",
              "s3:GetObject",
              "s3:GetObjectAcl",
              "s3:DeleteObject"
            ],
            Resource=[
                "arn:aws:s3:::mafer-bucket-deb-220296",
                "arn:aws:s3:::mafer-bucket-deb-220296/*"
            ],
            Effect= "Allow",
            Principal= "*"
        }
    ]
}