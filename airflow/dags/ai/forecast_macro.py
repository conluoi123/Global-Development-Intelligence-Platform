import os
import pandas as pd
import numpy as np
import warnings
from statsmodels.tsa.holtwinters import ExponentialSmoothing

# Tắt các cảnh báo không cần thiết của statsmodels
warnings.filterwarnings('ignore')

# Cấu hình các đường dẫn dựa trên cấu trúc thư mục Airflow
BASE_DIR = os.path.dirname(os.path.dirname(__file__)) # Thư mục dags/
INPUT_CSV = os.path.join(BASE_DIR, "data", "silver", "wb_macro_clean.csv")
OUTPUT_CSV = os.path.join(BASE_DIR, "data", "gold", "global_forecast_2025_2029.csv")

def forecast_macro_indicators():
    print("=== BẮT ĐẦU DỰ BÁO CHUỖI THỜI GIAN VĨ MÔ (5 NĂM TỚI) ===")
    
    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(f"Không tìm thấy dữ liệu Silver tại: {INPUT_CSV}")
        
    df_clean = pd.read_csv(INPUT_CSV)
    
    # 1. Xác định mốc thời gian lớn nhất hiện tại và 5 năm tiếp theo để dự báo
    max_year = df_clean['year'].max()
    forecast_years = list(range(int(max_year) + 1, int(max_year) + 6))
    print(f"-> Dữ liệu lịch sử kết thúc tại năm {max_year}. Tiến hành dự báo 5 năm tiếp theo: {forecast_years}")
    
    # Các chỉ số cốt lõi cần chạy Holt-Winters
    indicators = ['gdp_growth', 'inflation', 'fdi_inflow']
    
    # 2. Lấy danh sách các quốc gia thực tế
    countries = df_clean[['country_code', 'country_name']].drop_duplicates().values
    print(f"-> Tìm thấy {len(countries)} quốc gia thực tế cần chạy mô hình dự báo.")
    
    forecast_records = []
    success_count = 0
    error_count = 0
    
    # 3. Chạy mô hình Holt-Winters cho từng quốc gia và từng chỉ số
    for code, name in countries:
        df_country = df_clean[df_clean['country_code'] == code].sort_values(by='year')
        
        # Nếu quốc gia có ít hơn 5 năm dữ liệu lịch sử, ta không thể fit model chuỗi thời gian được
        if len(df_country) < 5:
            # Dùng giá trị trung bình đơn giản hoặc bfill nếu thiếu
            for ind in indicators:
                mean_val = df_country[ind].mean() if not df_country[ind].empty else 0.0
                for year in forecast_years:
                    forecast_records.append({
                        'country_code': code,
                        'country_name': name,
                        'indicator': ind,
                        'year': year,
                        'forecast_value': mean_val
                    })
            continue
            
        for ind in indicators:
            series = df_country.set_index('year')[ind]
            
            try:
                # Cấu hình Holt-Winters: Sử dụng mô hình xu hướng Additive (tuyến tính)
                # Dữ liệu năm (annual) nên không có tính mùa vụ (seasonal=None)
                model = ExponentialSmoothing(
                    series,
                    trend='add',
                    seasonal=None,
                    initialization_method="estimated"
                )
                model_fit = model.fit()
                
                # Dự báo 5 bước (5 năm tới)
                predictions = model_fit.forecast(steps=5)
                
                for year, val in zip(forecast_years, predictions):
                    forecast_records.append({
                        'country_code': code,
                        'country_name': name,
                        'indicator': ind,
                        'year': year,
                        'forecast_value': val if not np.isnan(val) else series.mean()
                    })
                success_count += 1
            except Exception as e:
                # Nếu fit model bị lỗi (ví dụ dữ liệu bị phẳng hoặc hỏng), ta điền giá trị trung bình lịch sử làm baseline fallback
                error_count += 1
                mean_val = series.mean() if not np.isnan(series.mean()) else 0.0
                for year in forecast_years:
                    forecast_records.append({
                        'country_code': code,
                        'country_name': name,
                        'indicator': ind,
                        'year': year,
                        'forecast_value': mean_val
                    })
                    
    print(f"-> Chạy Holt-Winters hoàn tất. Thành công: {success_count} models | Lỗi/Dùng fallback: {error_count} models")
    
    # 4. Gộp kết quả và lưu file
    df_forecast = pd.DataFrame(forecast_records)
    
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    df_forecast.to_csv(OUTPUT_CSV, index=False)
    
    print(f"✅ Đã lưu kết quả dự báo vĩ mô 5 năm toàn cầu tại: {OUTPUT_CSV}")
    print(f"=== HOÀN THÀNH FORECAST LAYER (Shape: {df_forecast.shape}) ===")

if __name__ == "__main__":
    forecast_macro_indicators()
