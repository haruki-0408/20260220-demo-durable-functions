#!/bin/bash
FUNCTION_NAME="demo-durable-function"

# Durable Functionsのアップデート
zip -qj function.zip src/lambda_function.py
aws lambda update-function-code \
  --function-name "$FUNCTION_NAME" \
  --zip-file fileb://function.zip
rm -f function.zip

# バージョンの発行
aws lambda publish-version \
  --function-name "$FUNCTION_NAME"

