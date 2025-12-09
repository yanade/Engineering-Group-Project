resource "aws_lambda_function" "etl_ingestion" {
  function_name = "gamboge-etl-ingestion-lambda"
  role          = aws_iam_role.lambda_exec.arn
  runtime       = "python3.12"
  handler       = "etl_handler.handler"
  filename         = "lambda/etl_handler.zip"
  source_code_hash = filebase64sha256("lambda/etl_handler.zip")
  timeout     = 60
  memory_size = 256

  environment {
    variables = {
      STAGE               = "dev"
      LANDING_BUCKET_NAME = aws_s3_bucket.landing_zone.bucket
    }
  }

  tags = {
    Name = "gamboge-etl-ingestion-lambda"
  }
}


