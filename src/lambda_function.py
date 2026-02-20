import json
import csv
import os
import time
from io import StringIO

import boto3
from aws_durable_execution_sdk_python import DurableContext, durable_execution, durable_step
from aws_durable_execution_sdk_python.config import Duration, MapConfig, CallbackConfig, CompletionConfig, StepConfig

# =============================================================================
# 定数
# =============================================================================
BUCKET_NAME = os.environ.get('BUCKET_NAME', '')  # S3バケット名（売上データ/レポート保存先）
HIGH_VALUE_THRESHOLD = 1000000                    # 高額取引の閾値（100万円以上は承認必要）
BATCH_SIZE = 100                                  # context.map での並列処理単位
API_BATCH_SIZE = 1000                             # 外部API連携時のバッチサイズ
API_RATE_LIMIT_WAIT_SECONDS = 10                  # 外部API連携時のレート制限待機秒数

s3 = boto3.client('s3')


# =============================================================================
# S3から売上データを取得
# CSVファイルを読み込み、各行をdict形式に変換して返却
# =============================================================================
def fetch_sales_data(context: DurableContext, bucket: str, key: str) -> list:
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    reader = csv.DictReader(StringIO(content))
    records = [
        {
            'id': row['id'],
            'customer_name': row['customer_name'],
            'product': row['product'],
            'amount': int(row['amount']),
            'quantity': int(row['quantity']),
            'region': row['region'],
            'category': row['category'],
            'timestamp': row['timestamp']
        }
        for row in reader
    ]
    context.logger.info({
        "action": "売上データ取得",
        "details": {
            "bucket": bucket,
            "key": key,
            "record_count": len(records)
        }
    })
    return records


# =============================================================================
# 1件のレコードを検証・加工
# 税額(10%)と合計金額を計算し、処理済みフラグを付与
# =============================================================================
def process_record(record: dict) -> dict:
    time.sleep(0.05)
    return {
        **record,
        'tax': int(record['amount'] * 0.1),
        'total': int(record['amount'] * 1.1),
        'processed': True
    }


# =============================================================================
# バッチ内の全レコードを処理
# context.mapから呼び出され、バッチ単位で並列実行される
# =============================================================================
def process_batch(ctx: DurableContext, batch: list, index: int, all_batches: list) -> list:
    ctx.logger.info({
        "action": "バッチ処理",
        "details": {
            "batch_index": index,
            "batch_total": len(all_batches),
            "record_count": len(batch)
        }
    })
    return [process_record(record) for record in batch]


# =============================================================================
# 高額取引のIDリストを抽出
# =============================================================================
@durable_step
def extract_high_value_ids(step_context, records: list, threshold: int) -> list:
    high_value_ids = [r['id'] for r in records if r['amount'] >= threshold]
    step_context.logger.info({
        "action": "高額取引抽出",
        "details": {
            "total_records": len(records),
            "high_value_count": len(high_value_ids),
            "threshold": threshold
        }
    })
    return high_value_ids


# =============================================================================
# 承認依頼を送信
# 実運用ではSlack/メール等の承認システムに通知を送信
# =============================================================================
@durable_step
def send_approval_request(step_context, callback_id: str, high_value_count: int, date: str) -> dict:
    # 実運用では以下のように承認システムへ通知:
    # slack_client.chat_postMessage(
    #     channel='#approval',
    #     text=f'{date} の高額取引 {high_value_count} 件の承認をお願いします。\n'
    #          f'承認後、以下のコマンドを実行してください:\n'
    #          f'aws lambda send-durable-execution-callback-success --callback-id {callback_id} ...'
    # )
    step_context.logger.info({
        "action": "承認依頼送信",
        "details": {
            "callback_id": callback_id,
            "high_value_count": high_value_count,
            "date": date
        }
    })
    return {'sent': True, 'count': high_value_count}



# =============================================================================
# 外部会計APIに連携
# 承認済みレコードを外部の会計システムに同期
# =============================================================================
@durable_step
def sync_to_external_api(step_context, records: list) -> dict:
    # 実運用では外部APIを呼び出す:
    # response = requests.post('https://accounting-api.example.com/sync', json=records)
    # return {'synced': len(records), 'transaction_id': response.json()['id']}
    step_context.logger.info({
        "action": "外部API連携",
        "details": {
            "synced_count": len(records)
        }
    })
    return {'synced': len(records)}


# =============================================================================
# 最終レポート生成
# 却下詳細（rejected_records がある場合）と集計サマリーの両方をS3に保存
# =============================================================================
@durable_step
def generate_report(step_context, bucket: str, date: str, approved_records: list, rejected_records: list) -> dict:
    # 却下詳細レポート（却下がある場合のみ）
    rejected_url = None
    if rejected_records:
        rejected_report = {
            'date': date,
            'rejected_count': len(rejected_records),
            'total_amount': sum(r['amount'] for r in rejected_records),
            'records': rejected_records
        }
        rejected_key = f'rejected/{date}-rejected.json'
        s3.put_object(Bucket=bucket, Key=rejected_key, Body=json.dumps(rejected_report, ensure_ascii=False))
        rejected_url = f's3://{bucket}/{rejected_key}'

    # 集計サマリーレポート
    approved_sales = sum(r['amount'] for r in approved_records)
    rejected_sales = sum(r['amount'] for r in rejected_records)
    summary_report = {
        'date': date,
        'summary': {
            'total_approved': len(approved_records),
            'total_rejected': len(rejected_records),
            'approved_sales': approved_sales,
            'rejected_sales': rejected_sales
        }
    }
    summary_key = f'reports/{date}-report.json'
    s3.put_object(Bucket=bucket, Key=summary_key, Body=json.dumps(summary_report, ensure_ascii=False))
    step_context.logger.info({
        "action": "最終レポート生成",
        "details": {
            "approved_count": len(approved_records),
            "rejected_count": len(rejected_records),
            "approved_sales": approved_sales,
            "rejected_sales": rejected_sales,
            "summary_key": summary_key,
            "rejected_key": rejected_key if rejected_records else None
        }
    })
    return {
        'summary_url': f's3://{bucket}/{summary_key}',
        'rejected_url': rejected_url
    }


# =============================================================================
# メインハンドラ
# =============================================================================
@durable_execution
def lambda_handler(event: dict, context: DurableContext) -> dict:
    bucket = BUCKET_NAME
    date = event['date']

    # === Step 1: S3から売上データ取得 ===
    records = fetch_sales_data(context, bucket, f'sales/{date}.csv')

    # === Step 2: 全レコードを検証・加工（バッチ並列処理） ===
    batches = [records[i:i + BATCH_SIZE] for i in range(0, len(records), BATCH_SIZE)]
    map_result = context.map(
        batches,
        process_batch,
        name='process-records',
        config=MapConfig(
            max_concurrency=10,
            completion_config=CompletionConfig.all_successful()
        )
    )
    processed = []
    for batch_result in map_result.get_results():
        processed.extend(batch_result)

    # === Step 3: 高額取引のIDを抽出 ===
    high_value_ids = context.step(extract_high_value_ids(processed, HIGH_VALUE_THRESHOLD))

    # === Step 4: 高額取引の承認フロー ===
    approved_ids = []
    rejected_ids = []

    if high_value_ids:
        callback = context.create_callback(
            name='high-value-transaction-approval',
            config=CallbackConfig(timeout=Duration.from_days(3))
        )
        context.step(send_approval_request(callback.callback_id, len(high_value_ids), date))

        approval_result = json.loads(callback.result() or '{}')
        approved_ids = approval_result.get('approved_ids', [])
        rejected_ids = [id for id in high_value_ids if id not in approved_ids]

        if rejected_ids:
            rejected_records = [r for r in processed if r['id'] in rejected_ids]

    # === Step 5: 承認済みデータを外部会計APIに連携 ===
    approved_records = [
        r for r in processed
        if r['amount'] < HIGH_VALUE_THRESHOLD or r['id'] in approved_ids
    ]

    for i in range(0, len(approved_records), API_BATCH_SIZE):
        batch = approved_records[i:i + API_BATCH_SIZE]
        context.step(
            sync_to_external_api(batch),
            config=StepConfig(retry_strategy=lambda e, n: {
                'should_retry': n < 5,           # 最大5回リトライ
                'delay': min(5 * 2**(n-1), 60)  # 指数バックオフ: 5s→10s→20s→40s、上限60s
            })
        )
        if i + API_BATCH_SIZE < len(approved_records):
            context.wait(Duration.from_seconds(API_RATE_LIMIT_WAIT_SECONDS))

    # === Step 6: 最終レポート生成 ===
    rejected_records = [r for r in processed if r['id'] in rejected_ids]

    report = context.step(generate_report(
        bucket, date,
        approved_records,
        rejected_records
    ))

    return {
        'status': 'completed',
        'date': date,
        'total_records': len(records),
        'approved_records': len(approved_records),
        'rejected_records': len(rejected_records),
        'report_url': report['summary_url']
    }
