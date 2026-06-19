import requests
import json
import os
import time
from datetime import datetime

# cấu hình các feature 
INDICATORS = {
    "gdp_growth": "NY.GDP.MKTP.KD.ZG",                 # Tăng trưởng GDP (%)
    "inflation": "FP.CPI.TOTL.ZG",                     # Lạm phát (%)
    "fdi_inflow": "BX.KLT.DINV.WD.GD.ZS",              # Dòng vốn FDI (% GDP)
    "unemployment": "SL.UEM.TOTL.ZS",                  # Tỷ lệ thất nghiệp (%)
    "trade_balance": "NE.TRD.GNFS.ZS",                 # Tổng xuất nhập khẩu (% GDP)
    "real_interest_rate": "FR.INR.RINR",               # Lãi suất thực (%)
    "current_account": "BN.CAB.XOKA.GD.ZS",            # Cán cân vãng lai (% GDP)
    "external_debt": "DT.DOD.DECT.GN.ZS",              # Nợ nước ngoài (% GNI)
    "total_reserves": "FI.RES.TOTL.CD",                # Tổng dự trữ ngoại hối (USD)
    "exchange_rate": "PA.NUS.FCRF",                    # Tỷ giá hối đoái so với USD
}

# lưu vào ổ cứng => sau khi hoàn thành xong pipeline chuyển sang MinIO (Local Storage)
BRONZE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "bronze")
os.makedirs(BRONZE_DIR, exist_ok=True)

def fetch_indicator_data(indicator_code: str, name: str):
    """
    Kéo toàn bộ lịch sử của 1 chỉ số từ World Bank API (Tất cả quốc gia).
    Có xử lý phân trang (pagination) vì API WB giới hạn số dòng mỗi lần gọi.
    """
    print(f"Bắt đầu lấy dữ liệu: {name} ({indicator_code})...")
    
    base_url = f"http://api.worldbank.org/v2/country/all/indicator/{indicator_code}"
    page = 1
    total_pages = 1
    all_data = []

    while page <= total_pages:
        params = {
            "format": "json",
            "per_page": 1000, # Lấy 1000 dòng mỗi lần gọi để nhanh hơn
            "page": page
        }
        
        try:
            response = requests.get(base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # WB API trả về mảng 2 phần tử: [metadata, dữ_liệu_thật]
            if len(data) < 2 or not data[1]:
                print(f"Không có dữ liệu ở trang {page}")
                break
                
            metadata = data[0]
            records = data[1]
            
            total_pages = metadata['pages']
            all_data.extend(records)
            
            print(f"   ✓ Đã lấy trang {page}/{total_pages} ({len(records)} dòng)")
            
            page += 1
            time.sleep(0.5) # Tránh bị WB khóa IP do gọi quá nhanh
            
        except requests.exceptions.RequestException as e:
            print(f"Lỗi khi gọi API trang {page}: {e}")
            break

    # Lưu toàn bộ vào file JSON
    if all_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{timestamp}.json"
        filepath = os.path.join(BRONZE_DIR, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            # Lưu raw payload nguyên bản để giữ tính Idempotency
            json.dump({
                "metadata": {
                    "indicator": indicator_code,
                    "extracted_at": timestamp,
                    "total_records": len(all_data)
                },
                "raw_payload": all_data
            }, f, ensure_ascii=False, indent=2)
            
        print(f"Đã lưu thành công {len(all_data)} dòng vào: {filepath}\n")

if __name__ == "__main__":
    print("=== WORLD BANK INGESTION PIPELINE (LOCAL MODE) ===")
    for name, code in INDICATORS.items():
        fetch_indicator_data(code, name)
    print("Hoàn tất quá trình kéo data!")
