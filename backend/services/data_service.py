import os
import io
import re
from datetime import timedelta
import logging

import requests
import polars as pl
import duckdb
import numpy as np
from minio import Minio
from minio.error import S3Error

from config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    NAS_ROOT_PATH
)

logger = logging.getLogger(__name__)

SUPPORTED_FILE_TYPES = {"csv", "tsv", "psv", "txt", "xlsx", "xls", "parquet"}
DELIMITED_FILE_TYPES = {"csv", "tsv", "psv", "txt"}
EXCEL_FILE_TYPES = {"xlsx", "xls"}


class DataViewerError(Exception):
    status_code = 400

    def __init__(self, detail: str):
        super().__init__(detail)
        self.detail = detail


class UnsupportedFileTypeError(DataViewerError):
    status_code = 415


class FileTooLargeError(DataViewerError):
    status_code = 413


class DataService:
    def __init__(self):
        self.minio_client = self._init_minio()
        self.url = ""
        self.con = duckdb.connect()
        self.use_duckdb_view = False
        self.total_rows_cache = None
        self.large_file_threshold_bytes = int(os.environ.get("LARGE_FILE_THRESHOLD_BYTES", str(256 * 1024 * 1024)))
        self.large_sample_rows = int(os.environ.get("LARGE_SAMPLE_ROWS", "100000"))
        self.max_excel_preview_bytes = int(os.environ.get("MAX_EXCEL_PREVIEW_BYTES", str(100 * 1024 * 1024)))
        self.max_distribution_categories = int(os.environ.get("MAX_DISTRIBUTION_CATEGORIES", "20"))
        self.max_page_size = int(os.environ.get("MAX_PAGE_SIZE", "100"))
        self.duckdb_csv_sample_size = int(os.environ.get("DUCKDB_CSV_SAMPLE_SIZE", "200000"))

    def _is_integer_dtype(self, dtype: pl.DataType) -> bool:
        integer_dtypes = {pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64}
        return dtype in integer_dtypes

    def _is_float_dtype(self, dtype: pl.DataType) -> bool:
        float_dtypes = {pl.Float32, pl.Float64}
        return dtype in float_dtypes

    def _is_boolean_dtype(self, dtype: pl.DataType) -> bool:
        return dtype == pl.Boolean

    def _is_temporal_dtype(self, dtype: pl.DataType) -> bool:
        temporal_dtypes = {pl.Date, pl.Datetime, pl.Time, pl.Duration}
        return dtype in temporal_dtypes

    def _sql_string_literal(self, value: str) -> str:
        return value.replace("'", "''")

    def _reset_dataset_state(self):
        self.url = ""
        self.use_duckdb_view = False
        self.total_rows_cache = None
        try:
            self.con.unregister("df")
        except Exception:
            pass
        try:
            self.con.execute("DROP VIEW IF EXISTS df")
        except Exception:
            pass
        try:
            self.con.execute("DROP TABLE IF EXISTS df")
        except Exception:
            pass

    def _get_extension(self, file_name: str) -> str:
        return os.path.splitext(file_name or "")[1].lower().lstrip(".")

    def _validate_supported_extension(self, ext: str):
        if not ext:
            raise UnsupportedFileTypeError("확장자가 없는 파일은 미리보기를 지원하지 않습니다.")
        if ext not in SUPPORTED_FILE_TYPES:
            supported = ", ".join(sorted(SUPPORTED_FILE_TYPES))
            raise UnsupportedFileTypeError(f"지원하지 않는 파일 형식입니다: .{ext}. 지원 형식: {supported}")

    def _format_bytes(self, size: int | None) -> str:
        if size is None:
            return "알 수 없음"
        units = ("B", "KB", "MB", "GB", "TB")
        value = float(size)
        for unit in units:
            if value < 1024 or unit == units[-1]:
                return f"{value:.1f}{unit}" if unit != "B" else f"{int(value)}B"
            value /= 1024
        return f"{size}B"

    def _ensure_excel_preview_allowed(self, size: int | None, has_parquet_cache: bool = False):
        if has_parquet_cache:
            return
        if size is not None and size > self.max_excel_preview_bytes:
            raise FileTooLargeError(
                "대용량 Excel 파일은 서버에서 직접 미리보기할 수 없습니다. "
                f"현재 파일 크기: {self._format_bytes(size)}, 제한: {self._format_bytes(self.max_excel_preview_bytes)}. "
                "CSV 또는 Parquet으로 변환한 뒤 다시 시도해주세요."
            )

    def _is_local_cache_current(self, source_path: str, cache_path: str) -> bool:
        if not os.path.exists(cache_path):
            return False
        try:
            return os.path.getmtime(cache_path) >= os.path.getmtime(source_path)
        except OSError:
            return False

    def _is_minio_cache_current(self, bucket_name: str, source_name: str, cache_name: str, source_stat) -> bool:
        try:
            cache_stat = self.minio_client.stat_object(bucket_name, cache_name)
        except S3Error as exc:
            if exc.code in ("NoSuchKey", "NoSuchObject", "NoSuchBucket"):
                return False
            raise

        source_modified = getattr(source_stat, "last_modified", None)
        cache_modified = getattr(cache_stat, "last_modified", None)
        if source_name == cache_name:
            return True
        if source_modified and cache_modified:
            return cache_modified >= source_modified
        return True

    def _delimiter_candidates(self, ext: str):
        if ext == "tsv":
            return ["\t"]
        if ext == "psv":
            return ["|"]
        if ext == "txt":
            return [None, "\t", "|"]
        return [None, "|", "\t"]

    def _csv_reader_sql(self, source: str, ext: str, delimiter=None, all_varchar: bool = False) -> str:
        options = [
            f"sample_size={self.duckdb_csv_sample_size}",
            "ignore_errors=true",
        ]
        if delimiter is not None:
            options.append(f"delim='{self._sql_string_literal(delimiter)}'")
        if all_varchar:
            options.append("all_varchar=true")
        option_sql = ", ".join(options)
        return f"read_csv_auto('{self._sql_string_literal(source)}', {option_sql})"

    def _create_csv_view_with_fallback(self, source: str, ext: str, all_varchar: bool = False) -> pl.DataFrame:
        last_error = None
        best_cols_df = None

        for delimiter in self._delimiter_candidates(ext):
            try:
                reader_sql = self._csv_reader_sql(source, ext, delimiter, all_varchar)
                self.con.execute(f"CREATE OR REPLACE VIEW df AS SELECT * FROM {reader_sql}")
                cols_df = self.con.execute("SELECT * FROM df LIMIT 0").pl()
                if best_cols_df is None:
                    best_cols_df = cols_df
                if len(cols_df.columns) > 1:
                    return cols_df
            except Exception as exc:
                last_error = exc

        if best_cols_df is not None:
            return best_cols_df

        if not all_varchar:
            return self._create_csv_view_with_fallback(source, ext, all_varchar=True)

        raise RuntimeError(f"구분자 기반 파일을 읽을 수 없습니다: {last_error}")

    def _read_csv_dataframe_with_fallback(self, source: str, ext: str) -> pl.DataFrame:
        self._create_csv_view_with_fallback(source, ext)
        df = self.con.execute("SELECT * FROM df").pl()
        self.con.execute("DROP VIEW IF EXISTS df")
        return df

    def _read_parquet_as_view(self, source: str) -> pl.DataFrame:
        self.con.execute(f"CREATE OR REPLACE VIEW df AS SELECT * FROM read_parquet('{self._sql_string_literal(source)}')")
        return self.con.execute("SELECT * FROM df LIMIT 0").pl()

    def _rewrite_user_query(self, query: str) -> str:
        safe_query = query.replace(';', '').strip()
        rewritten = re.sub(r"\bfrom\s+data\b", "from df", safe_query, flags=re.IGNORECASE)
        if rewritten == safe_query and re.search(r"\bdata\b", safe_query, flags=re.IGNORECASE):
            rewritten = re.sub(r"\bdata\b", "df", safe_query, count=1, flags=re.IGNORECASE)
        return rewritten

    def _sample_for_distributions(self, query: str = "SELECT * FROM df") -> pl.DataFrame:
        try:
            return self.con.execute(f"SELECT * FROM ({query}) AS sample_src LIMIT {self.large_sample_rows}").pl()
        except Exception:
            return self.con.execute("SELECT * FROM df LIMIT 10").pl()

    def _resolve_nas_path(self, bucket_name: str, file_name: str) -> str:
        if file_name.startswith('/'):
            file_name = file_name[1:]
        safe_bucket = bucket_name.lstrip('/') if bucket_name else ''
        abs_path = os.path.abspath(os.path.join(NAS_ROOT_PATH, safe_bucket, file_name))
        nas_root_abs = os.path.abspath(NAS_ROOT_PATH)
        if not abs_path.startswith(nas_root_abs + os.sep) and abs_path != nas_root_abs:
            raise PermissionError("NAS 경로 이탈이 감지되었습니다.")
        return abs_path

    def _read_csv_with_fallback(self, source, ext: str = "csv", **common_args) -> pl.DataFrame:
        """CSV/TSV/TXT 계열 파일을 구분자와 인코딩 후보로 재시도합니다."""
        best_df = None
        last_error = None

        for encoding in (None, "utf-8-sig", "euc-kr"):
            for separator in self._delimiter_candidates(ext):
                if hasattr(source, 'seek'):
                    source.seek(0)
                args = dict(common_args)
                if encoding:
                    args["encoding"] = encoding
                if separator is not None:
                    args["separator"] = separator
                try:
                    df = pl.read_csv(source, **args)
                    if best_df is None:
                        best_df = df
                    if len(df.columns) > 1:
                        return df
                except Exception as exc:
                    last_error = exc

        if best_df is not None:
            return best_df

        if hasattr(source, 'seek'):
            source.seek(0)
        args = {k: v for k, v in common_args.items() if k != "ignore_errors"}
        try:
            return pl.read_csv(source, dtypes=pl.Utf8, **args)
        except Exception as exc:
            raise RuntimeError(f"구분자 기반 파일을 읽을 수 없습니다: {last_error or exc}") from exc

    def _clean_dataframe_nulls(self, df: pl.DataFrame) -> pl.DataFrame:
        expressions = []
        for column_name, dtype in zip(df.columns, df.dtypes):
            col = pl.col(column_name)
            if self._is_integer_dtype(dtype):
                expressions.append(col.fill_null(0).alias(column_name))
            elif self._is_float_dtype(dtype):
                expressions.append(col.fill_nan(0.0).fill_null(0.0).alias(column_name))
            elif self._is_boolean_dtype(dtype):
                expressions.append(col.fill_null(False).alias(column_name))
            elif self._is_temporal_dtype(dtype):
                # 날짜/시간 계열은 문자열로 변환해 빈 문자열로 채움(표시 일관성 유지)
                expressions.append(col.cast(pl.Utf8).fill_null("").alias(column_name))
            else:
                # 문자열/범주형 등은 빈 문자열로 채움
                expressions.append(col.fill_null("").alias(column_name))
        return df.select(expressions)

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
            logger.error("MinIO 초기화 실패", exc_info=e)
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
            logger.info("Pre-signed URL 생성 완료")
        except Exception as e:
            raise RuntimeError(f"Pre-signed URL 생성 중 오류 발생: {e}") from e

    def _get_or_load_dataframe(self, bucket_name: str, file_name: str, storage_type: str | None = None) -> pl.DataFrame:
        if storage_type == 'nas':
            # ---- NAS 경로 처리 ----
            if not os.path.isdir(NAS_ROOT_PATH):
                raise FileNotFoundError(f"NAS 루트 경로를 찾을 수 없습니다: {NAS_ROOT_PATH}")

            if file_name and file_name[0] == "/":
                file_name = file_name[1:]
            abs_path = self._resolve_nas_path(bucket_name, file_name)
            if not os.path.exists(abs_path):
                raise FileNotFoundError(f"파일을 찾을 수 없습니다: {abs_path}")

            base_name, ext = os.path.splitext(abs_path)
            ext = ext.lower().lstrip(".")
            self._validate_supported_extension(ext)
            parquet_path = f"{base_name}.parquet"

            object_size = None
            try:
                object_size = os.path.getsize(abs_path)
            except Exception:
                pass
            is_large_initial = object_size is not None and object_size >= self.large_file_threshold_bytes
            parquet_exists = ext == "parquet" or self._is_local_cache_current(abs_path, parquet_path)

            # 확장자별 Parquet 캐시 생성 정책
            if ext in DELIMITED_FILE_TYPES:
                if not is_large_initial and not parquet_exists:
                    common_args = dict(
                        infer_schema_length=100000,
                        ignore_errors=True,
                        null_values=["", "NA", "N/A", "null", "NULL", "NaN", "nan", "-", "—"],
                        try_parse_dates=True,
                    )
                    try:
                        df = self._read_csv_with_fallback(abs_path, ext=ext, **common_args)
                        df = self._clean_dataframe_nulls(df)
                        df.write_parquet(parquet_path)
                        parquet_exists = True
                    except Exception as e:
                        logger.warning("NAS Parquet 캐시 저장 실패", exc_info=e)
            elif ext in EXCEL_FILE_TYPES:
                self._ensure_excel_preview_allowed(object_size, parquet_exists)
                if not parquet_exists:
                    df = pl.read_excel(abs_path, infer_schema_length=10000)
                    df = self._clean_dataframe_nulls(df)
                    try:
                        df.write_parquet(parquet_path)
                        parquet_exists = True
                    except Exception as e:
                        logger.warning("NAS Parquet 캐시 저장 실패", exc_info=e)
                        self.con.register("df", df)
                        return df
            elif ext == "parquet":
                pass

            # read_target 결정 및 구분자 기반 대용량 파일은 원본 사용 강제
            if ext in EXCEL_FILE_TYPES:
                if not parquet_exists:
                    raise RuntimeError("Excel 파일의 Parquet 캐시를 생성하지 못했습니다.")
                read_target = parquet_path
            elif ext in DELIMITED_FILE_TYPES:
                read_target = parquet_path if parquet_exists else abs_path
            else:
                read_target = abs_path
            read_target_ext = self._get_extension(read_target)

            is_large = False
            if ext in DELIMITED_FILE_TYPES:
                is_large = object_size is not None and object_size >= self.large_file_threshold_bytes
            else:
                read_target_size = None
                try:
                    read_target_size = os.path.getsize(read_target)
                    is_large = read_target_size is not None and read_target_size >= self.large_file_threshold_bytes
                except Exception:
                    pass

            if is_large:
                self.use_duckdb_view = True
                if read_target_ext in DELIMITED_FILE_TYPES:
                    cols_df = self._create_csv_view_with_fallback(read_target, read_target_ext)
                else:
                    cols_df = self._read_parquet_as_view(read_target)
                self.total_rows_cache = self.con.execute("SELECT COUNT(*) FROM df").fetchone()[0]
                logger.info("대용량 모드(DuckDB 뷰, NAS) 활성화", extra={"rows": self.total_rows_cache, "cols": len(cols_df.columns)})
                return cols_df
            else:
                if read_target_ext in DELIMITED_FILE_TYPES:
                    df = self._read_csv_dataframe_with_fallback(read_target, read_target_ext)
                else:
                    df = self.con.execute(f"SELECT * FROM read_parquet('{self._sql_string_literal(read_target)}')").pl()
                self.con.register("df", df)
                return df

        # ---- MinIO 처리 ----
        if not self.minio_client:
            raise ConnectionError("MinIO 클라이언트가 초기화되지 않았습니다.")
        if not self.minio_client.bucket_exists(bucket_name):
            raise FileNotFoundError(f"MinIO 버킷 '{bucket_name}'을 찾을 수 없습니다.")
        if not self.minio_client.stat_object(bucket_name, file_name):
            raise FileNotFoundError(f"파일 '{file_name}'을 찾을 수 없습니다.")

        if file_name and file_name[0] == "/":
            file_name = file_name[1:]

        base_name, ext = os.path.splitext(file_name)
        ext = ext.lower().lstrip(".")
        self._validate_supported_extension(ext)
        parquet_name = f"{base_name}.parquet"

        try:
            stat = self.minio_client.stat_object(bucket_name, file_name)
            object_size = getattr(stat, 'size', None)

            if object_size is not None and object_size >= self.large_file_threshold_bytes and ext in (DELIMITED_FILE_TYPES | {"parquet"}):
                self._create_presigned_url(bucket_name, file_name)
                self.use_duckdb_view = True
                if ext in DELIMITED_FILE_TYPES:
                    cols_df = self._create_csv_view_with_fallback(self.url, ext)
                else:  # parquet
                    cols_df = self._read_parquet_as_view(self.url)
                self.total_rows_cache = self.con.execute("SELECT COUNT(*) FROM df").fetchone()[0]
                logger.info("대용량 모드(DuckDB 뷰, MinIO) 활성화", extra={"rows": self.total_rows_cache, "cols": len(cols_df.columns)})
                return cols_df

            parquet_exists = self._is_minio_cache_current(bucket_name, file_name, parquet_name, stat)

            if ext in EXCEL_FILE_TYPES:
                self._ensure_excel_preview_allowed(object_size, parquet_exists)

            if not parquet_exists:
                self._create_presigned_url(bucket_name, file_name)
                response = requests.get(self.url, timeout=120)
                response.raise_for_status()
                buffer = io.BytesIO(response.content)

                if ext in DELIMITED_FILE_TYPES:
                    common_args = dict(
                        infer_schema_length=100000,
                        ignore_errors=True,
                        null_values=["", "NA", "N/A", "null", "NULL", "NaN", "nan", "-", "—"],
                        try_parse_dates=True,
                    )
                    df = self._read_csv_with_fallback(buffer, ext=ext, **common_args)
                    df = self._clean_dataframe_nulls(df)
                elif ext in EXCEL_FILE_TYPES:
                    df = pl.read_excel(buffer, infer_schema_length=10000)
                    df = self._clean_dataframe_nulls(df)
                elif ext == "parquet":
                    parquet_name = file_name
                    parquet_exists = True
                    df = None
                else:
                    raise UnsupportedFileTypeError(f"지원하지 않는 파일 형식입니다: .{ext}")

                if df is not None:
                    parquet_buffer = io.BytesIO()
                    df.write_parquet(parquet_buffer)
                    parquet_buffer.seek(0)

                    self.minio_client.put_object(
                        bucket_name,
                        parquet_name,
                        data=parquet_buffer,
                        length=len(parquet_buffer.getvalue()),
                        content_type="application/octet-stream"
                    )

            self._create_presigned_url(bucket_name, parquet_name)
            logger.info("Parquet 읽기 시작", extra={"url": True, "bucket": bucket_name, "file": parquet_name})

            if object_size is not None and object_size >= self.large_file_threshold_bytes:
                self.use_duckdb_view = True
                cols_df = self._read_parquet_as_view(self.url)
                self.total_rows_cache = self.con.execute("SELECT COUNT(*) FROM df").fetchone()[0]
                return cols_df
            else:
                df = self.con.execute(f"SELECT * FROM read_parquet('{self._sql_string_literal(self.url)}')").pl()
                self.con.register("df", df)
                return df

        except S3Error as exc:
            if exc.code == 'NoSuchKey':
                raise FileNotFoundError(f"파일 '{file_name}' 또는 Parquet 버전을 찾을 수 없습니다.")
            else:
                raise


    def _calculate_distributions(self, df: pl.DataFrame):
        distributions = {}
        for col in df.columns:
            dtype_str = str(df[col].dtype).lower()
            
            if 'string' in dtype_str or 'categorical' in dtype_str or 'bool' in dtype_str:
                value_counts = (
                    df[col]
                    .drop_nulls()
                    .value_counts()
                    .sort("count", descending=True)
                    .head(self.max_distribution_categories)
                )
                distributions[col] = {
                    'type': 'categorical',
                    'counts': value_counts["count"].to_list(),
                    'labels': [str(value) for value in value_counts[col].to_list()]
                }
            
            elif 'int' in dtype_str or 'float' in dtype_str:
                series = df[col].drop_nulls()
                if 'float' in dtype_str:
                    series = series.filter(series.is_finite())
                if series.is_empty():
                    continue
                
                unique_values = series.unique().sort()
                unique_count = len(unique_values)
                series_np = series.to_numpy()
                series_np = series_np[np.isfinite(series_np)]
                if len(series_np) == 0:
                    continue
                counts, labels = [], []

                if 1 < unique_count <= 20:
                    diff_mean = unique_values.diff().mean()
                    
                    if diff_mean is None:
                        diff = unique_values[1] - unique_values[0]
                        bin_edges = [unique_values[0] - diff / 2, unique_values[0] + diff / 2, unique_values[1] + diff / 2]
                    else:
                        bin_edges = (unique_values - (diff_mean / 2)).to_list()
                        bin_edges.append(unique_values[-1] + (diff_mean / 2))

                    hist_counts, _ = np.histogram(series_np, bins=bin_edges)
                    counts = hist_counts
                    labels = [f"{float(val):.6g}" for val in unique_values]

                elif unique_count == 1:
                    counts = [len(series_np)]
                    labels = [f"{float(unique_values[0]):.6g}"]

                else:
                    n = len(series_np)
                    if n < 2: continue

                    min_val, max_val = series_np.min(), series_np.max()

                    q1 = np.quantile(series_np, 0.25)
                    q3 = np.quantile(series_np, 0.75)
                    iqr = q3 - q1
                    
                    if iqr > 0:
                        bin_width = 2 * iqr * (n ** (-1/3))
                        num_bins = min(20, int(np.ceil((max_val - min_val) / bin_width))) if bin_width > 0 else 20
                    else:
                        num_bins = 20

                    num_bins = max(2, num_bins)
                    hist_counts, hist_edges = np.histogram(series_np, bins=num_bins)
                    counts = hist_counts
                    labels = [f"{edge:.6g}" for edge in hist_edges[:-1]]
                
                distributions[col] = {
                    'type': 'numeric',
                    'counts': counts if isinstance(counts, list) else counts.tolist(),
                    'labels': labels
                }
        return distributions

    def get_dataset_details(self, bucket_name: str, file_name: str, storage_type: str | None = None):
        """데이터셋의 초기 정보 반환"""
        self._reset_dataset_state()
        df = self._get_or_load_dataframe(bucket_name, file_name, storage_type)
        preview = self.con.execute(f"SELECT * FROM df LIMIT 10").pl()

        if self.use_duckdb_view:
            # 대용량: 전체 적재 대신 샘플로 분포 계산, 총건수는 캐시/COUNT(*) 사용
            sample_df = self._sample_for_distributions()
            distributions = self._calculate_distributions(sample_df)
            total_count = self.total_rows_cache
            if total_count is None:
                try:
                    total_count = self.con.execute("SELECT COUNT(*) FROM df").fetchone()[0]
                except Exception:
                    total_count = 0
            return {
                "bucket_name": bucket_name,
                "columns": preview.columns,
                "tableData": preview.to_dicts(),
                "distributions": distributions,
                "total": int(total_count),
            }
        else:
            distributions = self._calculate_distributions(df.head(self.large_sample_rows))
            return {
                "bucket_name": bucket_name,
                "columns": df.columns,
                "tableData": preview.to_dicts(),
                "distributions": distributions,
                "total": len(df)
            }

    def get_paged_data(self, query: str, page: int, page_size: int):
        base_query = self._rewrite_user_query(query)
        safe_page = max(1, int(page or 1))
        safe_page_size = min(self.max_page_size, max(1, int(page_size or 10)))
        offset = (safe_page - 1) * safe_page_size
        paged_query = f"SELECT * FROM ({base_query}) AS page_src LIMIT {safe_page_size} OFFSET {offset}"
        
        result_df = self.con.execute(paged_query).pl()
        return {"tableData": result_df.to_dicts()}

    def execute_query(self, query: str):  
        """사용자 쿼리를 실행하고 결과 반환""" 
        base_query = self._rewrite_user_query(query)
        total_count = self.con.execute(f"SELECT COUNT(*) FROM ({base_query}) AS sub").fetchone()[0]

        paged_query = f"SELECT * FROM ({base_query}) AS query_src LIMIT 10"
        result_df = self.con.execute(paged_query).pl()

        # 대용량: 분포는 샘플로 계산하여 OOM 방지
        try:
            sample_limit = min(self.large_sample_rows, max(10, int(total_count)))
        except Exception:
            sample_limit = self.large_sample_rows
        try:
            sample_df = self.con.execute(f"SELECT * FROM ({base_query}) AS sub LIMIT {sample_limit}").pl()
        except Exception:
            sample_df = result_df
        distributions = self._calculate_distributions(sample_df)

        return {
            "columns": result_df.columns,
            "tableData": result_df.to_dicts(),
            "distributions": distributions,
            "total": int(total_count)
        } 

    def download_query(self, query: str) -> bytes:
        """사용자 쿼리 전체 결과를 CSV로 반환"""
        base_query = self._rewrite_user_query(query)
        try:
            df = self.con.execute(base_query).pl()
        except Exception as e:
            raise RuntimeError(f"쿼리 실행 실패: {e}") from e

        try:
            csv_bytes_io = io.BytesIO()
            df.write_csv(csv_bytes_io)
            csv_bytes_io.seek(0)
            return csv_bytes_io.getvalue()
        except Exception as e:
            raise RuntimeError(f"CSV 변환 실패: {e}") from e
