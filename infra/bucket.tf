resource "aws_s3_bucket" "bucket" {
  bucket = "etienne-bucket-deploy"

  tags = local.tags
}