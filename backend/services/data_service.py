import os
import io
from datetime import timedelta

import requests
import polars as pl
import duckdb
import numpy as np
from minio import Minio
from minio.error import S3Error

from config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

class DataService:
    def __init__(self):
        self.minio_client = self._init_minio()
        self.url = ""
        self.df = None
        self.con = duckdb.connect()

    def _init_minio(self):
        try:
            client = Minio(
                MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=False
            )
            return client
        except Exception as e:
            print(f"MinIO에 연결할 수 없습니다. 오류: {e}")
            return None

    def _create_presigned_url(self, bucket_name: str, file_name: str, expires_in_hours: int = 1) -> str:
        """pre-signed URL을 생성합니다."""
        if not self.minio_client:
            raise ConnectionError("MinIO 클라이언트가 설정되지 않아 URL을 생성할 수 없습니다.")
        
        if not self.minio_client.bucket_exists(bucket_name):
            raise FileNotFoundError(f"버킷 '{bucket_name}'을 찾을 수 없습니다.")

        try:
            self.url = self.minio_client.presigned_get_object(
                bucket_name,
                file_name,
                expires=timedelta(hours=expires_in_hours)
            )

            print(f"Pre-signed URL: {self.url}")
        except Exception as e:
            raise RuntimeError(f"Pre-signed URL 생성 중 오류 발생: {e}") from e

    def _get_or_load_dataframe(self, bucket_name: str, file_name: str, folder: str = "UPLOAD") -> pl.DataFrame:
        if not self.minio_client:
            raise ConnectionError("MinIO 클라이언트가 초기화되지 않았습니다.")
        if not self.minio_client.bucket_exists(bucket_name):
            raise FileNotFoundError(f"MinIO 버킷 '{bucket_name}'을 찾을 수 없습니다.")
        if not self.minio_client.stat_object(bucket_name, f"{folder}/{file_name}"):
            raise FileNotFoundError(f"파일 '{file_name}'을 찾을 수 없습니다.")
        
        base_name, ext = os.path.splitext(file_name)
        ext = ext.lower().lstrip(".")
        parquet_name = f"{base_name}.parquet"

        original_path = f"{folder}/{file_name}"
        parquet_path = f"{folder}/{parquet_name}"

        try:
            objects = self.minio_client.list_objects(bucket_name, prefix=parquet_path, recursive=True)
            parquet_exists = any(obj.object_name == parquet_path for obj in objects)

            if not parquet_exists:
                self._create_presigned_url(bucket_name, original_path)
                response = requests.get(self.url)
                buffer = io.BytesIO(response.content)

                if ext == "csv":
                    df = pl.read_csv(buffer, infer_schema_length=10000, ignore_errors=True)
                elif ext in ("xlsx", "xls"):
                    df = pl.read_excel(buffer, infer_schema_length=10000, ignore_errors=True)
                else:
                    raise ValueError(f"지원하지 않는 파일 형식: {ext}")

                parquet_buffer = io.BytesIO()
                df.write_parquet(parquet_buffer)
                parquet_buffer.seek(0)

                self.minio_client.put_object(
                    bucket_name,
                    parquet_path,
                    data=parquet_buffer,
                    length=len(parquet_buffer.getvalue()),
                    content_type="application/octet-stream"
                )

            self._create_presigned_url(bucket_name, parquet_path)

            df = self.con.execute(f"SELECT * FROM read_parquet('{self.url}')").pl()
            self.con.register("df", df)
            return df

        except S3Error as exc:
            if exc.code == 'NoSuchKey':
                raise FileNotFoundError(f"파일 '{file_name}' 또는 Parquet 버전을 찾을 수 없습니다.")
            else:
                raise

    def _calculate_distributions(self, df: pl.DataFrame):
        """Polars DataFrame으로 데이터 분포 계산"""
        distributions = {}
        for col in df.columns:
            dtype_str = str(df[col].dtype).lower()
            if 'string' in dtype_str or 'categorical' in dtype_str or 'bool' in dtype_str:
                value_counts = df[col].value_counts().sort("count", descending=True)
                distributions[col] = {
                    'type': 'categorical',
                    'counts': value_counts["count"].to_list(),
                    'labels': value_counts[col].to_list()
                }
            elif 'int' in dtype_str or 'float' in dtype_str:
                series = df[col].drop_nulls().to_numpy()
                if len(series) == 0: continue
                min_val, max_val = int(np.floor(series.min())), int(np.ceil(series.max()))
                if min_val == max_val:
                    counts, bin_edges = np.array([len(series)]), np.array([min_val, max_val + 1])
                else:
                    bin_edges = np.unique(np.round(np.linspace(min_val, max_val, num=20)).astype(int))
                    counts, _ = np.histogram(series, bins=bin_edges)
                
                distributions[col] = {
                    'type': 'numeric',
                    'counts': counts.tolist(),
                    'labels': [str(b) for b in bin_edges[:-1]]
                }
        return distributions

    def get_dataset_details(self, bucket_name: str, file_name: str):
        """데이터셋의 초기 정보 반환"""
        df = self._get_or_load_dataframe(bucket_name, file_name)
        preview = self.con.execute(f"SELECT * FROM df LIMIT 10").pl()
        
        distributions = self._calculate_distributions(df)
        return {
            "bucket_name": bucket_name,
            "file_name": file_name,
            "columns": df.columns,
            "tableData": preview.to_dicts(),
            "distributions": distributions,
            "total": len(df)
        }

    def get_paged_data(self, bucket_name: str, file_name: str, query: str, page: int, page_size: int):
        base_query = query.replace('from data', f'from df')
        offset = (page - 1) * page_size
        paged_query = f"{base_query} LIMIT {page_size} OFFSET {offset}"
        
        result_df = self.con.execute(paged_query).pl()
        return {"tableData": result_df.to_dicts()}

    def execute_query(self, bucket_name: str, file_name: str, query: str):
        """사용자 쿼리를 실행하고 결과 반환"""        
        base_query = query.replace('from data', 'from df')
        total_count = self.con.execute(f"SELECT COUNT(*) FROM ({base_query}) AS sub").fetchone()[0]
        
        paged_query = f"{base_query} LIMIT 10"

        full_df = self.con.execute(base_query).pl()
        result_df = self.con.execute(paged_query).pl()
        distributions = self._calculate_distributions(full_df)
        
        return {
            "columns": result_df.columns,
            "tableData": result_df.to_dicts(),
            "distributions": distributions,
            "total": total_count
        } 