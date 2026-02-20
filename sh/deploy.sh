#!/bin/bash
FUNCTION_NAME="demo-durable-function"

# Durable Functionsのアップデート
zip -q function.zip lambda_function.py
aws lambda update-function-code \
  --function-name "$FUNCTION_NAME" \
  --zip-file fileb://function.zip
rm -rf function.zip

# バージョンの発行
aws lambda publish-version \
  --function-name "$FUNCTION_NAME"

