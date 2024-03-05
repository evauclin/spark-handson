resource "aws_s3_bucket" "bucket" {
  bucket = "dream-bucket-eti"

  tags = local.tags
}