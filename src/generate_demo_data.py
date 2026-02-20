#!/usr/bin/env python3
"""
Lambda Durable Functions デモ用データ生成スクリプト

使用方法:
    python generate_demo_data.py
"""

import csv
import random
import os
from datetime import datetime, timedelta

# 設定
NUM_RECORDS = 10000
HIGH_VALUE_COUNT = 50
DATE = "2025-01-15"
OUTPUT_DIR = "sales"
OUTPUT_FILE = f"{OUTPUT_DIR}/{DATE}.csv"

# 商品リスト（商品名, 最小価格, 最大価格, カテゴリ）
PRODUCTS = [
    ("高級ドライヤー", 30000, 80000, "beauty"),
    ("美容液セット", 15000, 50000, "beauty"),
    ("シャンプー詰め合わせ", 5000, 20000, "haircare"),
    ("電動歯ブラシ", 10000, 40000, "health"),
    ("空気清浄機", 30000, 80000, "appliance"),
    ("ヘアアイロン", 15000, 45000, "beauty"),
    ("美顔器", 20000, 60000, "beauty"),
    ("電気シェーバー", 10000, 35000, "health"),
    ("加湿器", 8000, 30000, "appliance"),
    ("マッサージガン", 15000, 50000, "health"),
]

# 高額商品（100万円以上）
HIGH_VALUE_PRODUCTS = [
    ("業務用エステ機器", "equipment"),
    ("高級マッサージチェア", "furniture"),
    ("業務用美容機器セット", "equipment"),
    ("サロン向け大型什器", "furniture"),
]

REGIONS = ["東京", "大阪", "名古屋", "福岡", "札幌", "仙台", "広島", "横浜"]


def generate_data():
    data = []
    base_date = datetime.fromisoformat(f"{DATE}T00:00:00")
    high_value_indices = set(random.sample(range(NUM_RECORDS), HIGH_VALUE_COUNT))

    for i in range(NUM_RECORDS):
        # 会社名はシンプルに
        company = f"会社{chr(65 + (i % 26))}{i // 26 + 1}"  # 会社A1, 会社B1, ...

        if i in high_value_indices:
            # 高額取引（100万円以上）
            amount = random.randint(1000000, 5000000)
            product_name, category = random.choice(HIGH_VALUE_PRODUCTS)
        else:
            # 通常取引
            product_name, min_price, max_price, category = random.choice(PRODUCTS)
            amount = random.randint(min_price, max_price)

        data.append({
            "id": f"{i+1:05d}",
            "customer_name": company,
            "product": product_name,
            "amount": amount,
            "quantity": random.randint(1, 10),
            "region": random.choice(REGIONS),
            "category": category,
            "timestamp": (base_date + timedelta(seconds=i * 8)).isoformat()
        })

    return data


def save_to_csv(data):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    fieldnames = ["id", "customer_name", "product", "amount", "quantity", "region", "category", "timestamp"]
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def main():
    print(f"Generating {NUM_RECORDS:,} records...")
    data = generate_data()
    save_to_csv(data)

    # サマリー
    total = sum(d['amount'] for d in data)
    high_value = [d for d in data if d['amount'] >= 1000000]

    print(f"\n=== Summary ===")
    print(f"Total records: {len(data):,}")
    print(f"Total sales: ¥{total:,}")
    print(f"High-value (>=¥1,000,000): {len(high_value)} records")
    print(f"File: {OUTPUT_FILE}")
    print(f"\n=== Next Steps ===")
    print(f"aws s3 mb s3://YOUR-BUCKET --region ap-northeast-1")
    print(f"aws s3 cp {OUTPUT_FILE} s3://YOUR-BUCKET/sales/{DATE}.csv")


if __name__ == "__main__":
    main()