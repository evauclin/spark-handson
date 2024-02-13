resource "aws_glue_job" "etienne_example" {
  name            = "etienne_example"
  role_arn        = aws_iam_policy.glue_role_policy.arn

  command {
    script_location = "s3://etienne-bucket-deploy/spark-jobs/exo2_glue_job.py"
  }

  glue_version = "3.0"
  number_of_workers = 2
  worker_type = "Standard"

  default_arguments = {
    "--additional-python-modules"       = "s3://etienne-bucket-deploy/wheel//spark-handson.whl"
    "--python-modules-installer-option" = "--upgrade"
    "--job-language"                    = "python"
    "--PARAM_1"                         = "src/resources/exo1/data.csv"
    "--PARAM_2"                         = "text"
  }
  tags = local.tags
}