import os
import json
import pandas as pd

# ==========================================
# CẤU HÌNH ĐƯỜNG DẪN
# ==========================================
# Trỏ tới thư mục airflow/dags/data/
BASE_DIR = os.path.dirname(os.path.dirname(__file__)) 
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")

# Tạo thư mục silver nếu chưa có
os.makedirs(SILVER_DIR, exist_ok=True)

def process_bronze_to_silver():
    print("🔄 Đang đọc các file JSON từ Bronze Layer...")
    all_data = []

    # Quét toàn bộ file json trong thư mục bronze
    for filename in os.listdir(BRONZE_DIR):
        if filename.endswith(".json"):
            filepath = os.path.join(BRONZE_DIR, filename)
            
            # Lấy tên feature (ví dụ: gdp_growth từ gdp_growth_2026...json)
            feature_name = filename.split('_202')[0] 
            
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # Lấy data thô
            records = data.get("raw_payload", [])
            for row in records:
                # Bỏ qua nếu giá trị null
                if row['value'] is None:
                    continue
                    
                # Chỉ lấy quốc gia thật, bỏ qua các khu vực (các khu vực thường không có ISO3 hoặc là mã lạ, nhưng tạm thời lấy hết)
                # World bank có mã iso3code rỗng cho một số nhóm
                if not row['countryiso3code']:
                    continue

                all_data.append({
                    'country_code': row['countryiso3code'],
                    'country_name': row['country']['value'],
                    'year': int(row['date']),
                    'indicator': feature_name,
                    'value': row['value']
                })

    # Biến thành DataFrame (Excel)
    df = pd.DataFrame(all_data)
    
    if df.empty:
        print("⚠️ Không có dữ liệu để xử lý!")
        return

    print("🔄 Đang xoay dọc thành ngang (Pivot) để tạo Features...")
    # Xoay bảng: Gộp các indicator thành từng cột riêng biệt
    df_pivot = df.pivot_table(
        index=['country_code', 'country_name', 'year'],
        columns='indicator',
        values='value'
    ).reset_index()

    # Sắp xếp lại cho đẹp: theo Quốc gia và Năm tăng dần
    df_pivot = df_pivot.sort_values(by=['country_name', 'year']).reset_index(drop=True)

    # Lưu ra CSV
    csv_path = os.path.join(SILVER_DIR, "wb_macro_data.csv")
    df_pivot.to_csv(csv_path, index=False)
    
    print(f"✅ Đã biến đổi thành công! Lưu tại: {csv_path}")
    print(f"📊 Kích thước bảng (Shape): {df_pivot.shape[0]} dòng x {df_pivot.shape[1]} cột")
    print("\n👁️  Xem thử 5 dòng đầu tiên (Data Preview):")
    print(df_pivot.head())

if __name__ == "__main__":
    process_bronze_to_silver()
