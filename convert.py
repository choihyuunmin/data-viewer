import pandas as pd
import os
from datetime import datetime

def convert_to_parquet(input_file):
    """CSV 또는 Excel 파일을 Parquet 형식으로 변환"""
    # 파일 확장자 확인
    ext = input_file.split('.')[-1].lower()
    if ext not in ['csv', 'xlsx', 'xls']:
        raise ValueError("지원하지 않는 파일 형식입니다. CSV 또는 Excel 파일만 변환 가능합니다.")
    
    # 파일 읽기
    if ext == 'csv':
        df = pd.read_csv(input_file, low_memory=False)
    else:  # Excel 파일
        df = pd.read_excel(input_file)
    
    # 데이터 타입 변환
    df = df.convert_dtypes()
    df_obj = df.select_dtypes(include=['object'])
    df[df_obj.columns] = df_obj.astype('string')
    
    # 출력 파일명 생성
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    output_file = f"dataset/{timestamp}_{base_name}.parquet"
    
    # Parquet 파일로 저장
    df.to_parquet(output_file, compression='zstd', index=False)
    print(f"변환 완료: {output_file}")
    return df

if __name__ == "__main__":
    # dataset 디렉토리 생성
    os.makedirs("dataset", exist_ok=True)
    
    # 기존 파일 변환
    df1 = pd.read_csv('dataset/test1.csv', low_memory=False)
    df2 = pd.read_csv('dataset/test2.csv', low_memory=False)

    for df in (df1, df2):
        df.convert_dtypes()
        df_obj = df.select_dtypes(include=['object'])
        df[df_obj.columns] = df_obj.astype('string')
    
    df1.to_parquet('dataset/test1.parquet', compression='zstd', index=False)
    df2.to_parquet('dataset/test2.parquet', compression='zstd', index=False)
    
    print("기존 파일 변환 완료")