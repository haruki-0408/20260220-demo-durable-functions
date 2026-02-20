# Lambda関数の実行
aws lambda invoke \
  --function-name demo-durable-function:20 \
  --invocation-type Event \
  --cli-binary-format raw-in-base64-out \
  --payload '{"date": "2025-01-15"}'

# 人間による承認処理 コールバックID返却
CALLBACK_ID="XXXXXXXXXX"
aws lambda send-durable-execution-callback-success \
  --callback-id "$CALLBACK_ID" \
  --result $(echo -n '{"approved_ids": ["00003", "00005"]}' | base64) \
  --region ap-northeast-1