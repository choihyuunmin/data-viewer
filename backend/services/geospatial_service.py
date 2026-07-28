import codecs
import hashlib
import logging
import math
import os
import shutil
import tempfile
import threading
import zipfile
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from itertools import islice
from pathlib import Path, PurePosixPath
from typing import Any

import shapefile
from minio.error import S3Error
from pyproj import CRS, Transformer
from shapely import get_num_coordinates
from shapely.geometry import mapping, shape as to_shapely
from shapely.ops import transform as transform_geometry

from errors import GeospatialDataError

logger = logging.getLogger(__name__)

SHAPEFILE_EXTENSIONS = {".shp", ".shx", ".dbf", ".prj", ".cpg"}
GEOSPATIAL_EXTENSIONS = {"shp", "zip"}
CACHE_FORMAT_VERSION = "2"


@dataclass(frozen=True)
class PreparedDataset:
    root: Path
    layers: tuple[Path, ...]


class GeospatialService:
    def __init__(self, minio_client, nas_root_path: str):
        self.minio_client = minio_client
        self.nas_root_path = nas_root_path
        self.cache_root = Path(
            os.environ.get(
                "GEO_CACHE_DIR",
                str(Path(tempfile.gettempdir()) / "dataviewer-geo-cache"),
            )
        )
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self._cache_locks: dict[str, threading.Lock] = {}
        self._cache_locks_guard = threading.Lock()
        self.default_preview_limit = int(os.environ.get("GEO_PREVIEW_LIMIT", "1000"))
        self.max_preview_limit = int(os.environ.get("GEO_MAX_PREVIEW_LIMIT", "5000"))
        self.max_property_fields = int(os.environ.get("GEO_MAX_PROPERTY_FIELDS", "30"))
        self.max_coordinates_per_feature = int(
            os.environ.get("GEO_MAX_COORDINATES_PER_FEATURE", "20000")
        )
        self.max_archive_entries = int(os.environ.get("GEO_MAX_ARCHIVE_ENTRIES", "10000"))
        self.max_archive_uncompressed_bytes = int(
            os.environ.get("GEO_MAX_ARCHIVE_UNCOMPRESSED_BYTES", str(2 * 1024 * 1024 * 1024))
        )

    def supports(self, file_name: str) -> bool:
        return Path(file_name or "").suffix.lower().lstrip(".") in GEOSPATIAL_EXTENSIONS

    def get_dataset_details(
        self,
        bucket_name: str,
        file_name: str,
        storage_type: str | None = None,
    ) -> dict[str, Any]:
        prepared = self._prepare_dataset(bucket_name, file_name, storage_type)
        layer_summaries = [self._inspect_layer(prepared, path) for path in prepared.layers]
        selected_layer = layer_summaries[0]
        selected_path = self._select_layer(prepared, selected_layer["id"])

        with self._open_reader(selected_path) as reader:
            columns = self._field_names(reader)
            preview_records = []
            for record in islice(reader.iterRecords(), 10):
                if record is not None:
                    preview_records.append(self._record_to_dict(record))

        return {
            "dataset_type": "geospatial",
            "bucket_name": bucket_name,
            "file_name": file_name,
            "columns": columns,
            "tableData": preview_records,
            "distributions": {},
            "total": selected_layer["feature_count"],
            "map": {
                "layers": layer_summaries,
                "selected_layer": selected_layer["id"],
                "bounds": selected_layer["bounds"],
                "preview_limit": min(self.default_preview_limit, self.max_preview_limit),
                "max_preview_limit": self.max_preview_limit,
            },
        }

    def get_preview(
        self,
        bucket_name: str,
        file_name: str,
        storage_type: str | None = None,
        layer: str | None = None,
        bbox: tuple[float, float, float, float] | None = None,
        limit: int | None = None,
        simplify_tolerance: float = 0.0,
        fields: list[str] | None = None,
    ) -> dict[str, Any]:
        prepared = self._prepare_dataset(bucket_name, file_name, storage_type)
        selected_path = self._select_layer(prepared, layer)
        layer_summary = self._inspect_layer(prepared, selected_path)
        safe_limit = min(
            self.max_preview_limit,
            max(1, int(limit or self.default_preview_limit)),
        )

        with self._open_reader(selected_path) as reader:
            all_fields = self._field_names(reader)
            selected_fields = self._select_fields(all_fields, fields)
            source_crs, crs_warning = self._read_crs(selected_path, reader.bbox)
            to_wgs84 = Transformer.from_crs(source_crs, CRS.from_epsg(4326), always_xy=True)
            source_bbox = self._source_bbox(bbox, source_crs) if bbox else None

            if source_bbox:
                candidates = reader.iterShapeRecords(
                    fields=selected_fields or None,
                    bbox=source_bbox,
                )
                records = list(islice(candidates, safe_limit + 1))
                has_more = len(records) > safe_limit
                records = records[:safe_limit]
                sampled = has_more
                feature_ids = range(len(records))
            else:
                total = len(reader)
                indices = self._sample_indices(total, safe_limit)
                indexed_records = [
                    (index, reader.shapeRecord(index, fields=selected_fields))
                    for index in indices
                ]
                indexed_records = [
                    (index, record)
                    for index, record in indexed_records
                    if record is not None
                ]
                feature_ids = [index for index, _record in indexed_records]
                records = [record for _index, record in indexed_records]
                has_more = total > len(records)
                sampled = total > safe_limit

            features = []
            omitted = 0
            for feature_id, shape_record in zip(feature_ids, records):
                feature = self._shape_record_to_feature(
                    shape_record,
                    feature_id,
                    to_wgs84,
                    simplify_tolerance,
                    layer_summary["bounds"],
                )
                if feature is None:
                    omitted += 1
                    continue
                features.append(feature)

        return {
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "layer": layer_summary["id"],
                "feature_count": layer_summary["feature_count"],
                "returned": len(features),
                "limit": safe_limit,
                "sampled": sampled,
                "has_more": has_more,
                "bbox": list(bbox) if bbox else None,
                "bounds": layer_summary["bounds"],
                "geometry_type": layer_summary["geometry_type"],
                "source_crs": layer_summary["source_crs"],
                "crs_warning": crs_warning,
                "fields": selected_fields,
                "omitted_invalid_geometries": omitted,
            },
        }

    def _prepare_dataset(
        self,
        bucket_name: str,
        file_name: str,
        storage_type: str | None,
    ) -> PreparedDataset:
        extension = Path(file_name or "").suffix.lower()
        if extension not in {".shp", ".zip"}:
            raise GeospatialDataError("지도 미리보기는 SHP 또는 SHP가 포함된 ZIP 파일만 지원합니다.")

        if storage_type == "nas":
            source_path = self._resolve_nas_path(bucket_name, file_name)
            if not source_path.exists():
                raise FileNotFoundError(f"파일을 찾을 수 없습니다: {source_path}")
            if extension == ".shp":
                return PreparedDataset(source_path.parent, (source_path,))

            fingerprint = f"{source_path.stat().st_size}:{source_path.stat().st_mtime_ns}"
            cache_dir = self._cache_directory("nas", bucket_name, file_name)
            with self._get_cache_lock(cache_dir):
                self._refresh_cache(cache_dir, fingerprint)
                archive_path = cache_dir / "source.zip"
                if not archive_path.exists():
                    shutil.copy2(source_path, archive_path)
                return self._prepare_archive(cache_dir, archive_path)

        if not self.minio_client:
            raise ConnectionError("MinIO 클라이언트가 초기화되지 않았습니다.")
        if not self.minio_client.bucket_exists(bucket_name):
            raise FileNotFoundError(f"MinIO 버킷 '{bucket_name}'을 찾을 수 없습니다.")

        object_name = file_name.lstrip("/")
        try:
            stat = self.minio_client.stat_object(bucket_name, object_name)
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject", "NoSuchBucket"}:
                raise FileNotFoundError(f"파일 '{object_name}'을 찾을 수 없습니다.") from exc
            raise

        fingerprint = ":".join(
            [
                str(getattr(stat, "etag", "")),
                str(getattr(stat, "size", "")),
                str(getattr(stat, "last_modified", "")),
            ]
        )
        cache_dir = self._cache_directory("minio", bucket_name, object_name)
        with self._get_cache_lock(cache_dir):
            self._refresh_cache(cache_dir, fingerprint)

            if extension == ".zip":
                archive_path = cache_dir / "source.zip"
                if not archive_path.exists():
                    self.minio_client.fget_object(bucket_name, object_name, str(archive_path))
                return self._prepare_archive(cache_dir, archive_path)

            layer_path = self._download_minio_shapefile(cache_dir, bucket_name, object_name)
            return PreparedDataset(cache_dir, (layer_path,))

    def _prepare_archive(self, cache_dir: Path, archive_path: Path) -> PreparedDataset:
        extract_root = cache_dir / "archive"
        marker = extract_root / ".complete"
        cache_is_current = (
            marker.exists()
            and marker.read_text(encoding="utf-8", errors="replace") == CACHE_FORMAT_VERSION
        )
        if not cache_is_current:
            if extract_root.exists():
                shutil.rmtree(extract_root)
            extract_root.mkdir(parents=True, exist_ok=True)
            self._extract_archive(archive_path, extract_root)
            marker.write_text(CACHE_FORMAT_VERSION, encoding="utf-8")

        layers = tuple(sorted(extract_root.rglob("*.shp")))
        if not layers:
            raise GeospatialDataError("ZIP 파일 안에서 SHP 파일을 찾지 못했습니다.")
        return PreparedDataset(extract_root, layers)

    def _extract_archive(self, archive_path: Path, destination: Path):
        try:
            with zipfile.ZipFile(archive_path) as archive:
                entries = [entry for entry in archive.infolist() if not entry.is_dir()]
                if len(entries) > self.max_archive_entries:
                    raise GeospatialDataError(
                        f"ZIP 항목 수가 제한({self.max_archive_entries:,}개)을 초과했습니다."
                    )

                total_size = 0
                for entry in entries:
                    archive_name = PurePosixPath(entry.filename)
                    if archive_name.is_absolute() or ".." in archive_name.parts:
                        raise GeospatialDataError("ZIP 내부에 허용되지 않는 경로가 포함되어 있습니다.")
                    if "__MACOSX" in archive_name.parts or archive_name.name.startswith("._"):
                        continue

                    suffix = archive_name.suffix.lower()
                    if suffix not in SHAPEFILE_EXTENSIONS:
                        continue
                    total_size += entry.file_size
                    if total_size > self.max_archive_uncompressed_bytes:
                        raise GeospatialDataError("ZIP 압축 해제 크기가 지도 미리보기 제한을 초과했습니다.")

                    target = destination.joinpath(*archive_name.parts).with_suffix(suffix)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with archive.open(entry) as source, target.open("wb") as output:
                        shutil.copyfileobj(source, output)
        except zipfile.BadZipFile as exc:
            raise GeospatialDataError("올바른 ZIP 파일이 아닙니다.") from exc

    def _download_minio_shapefile(
        self,
        cache_dir: Path,
        bucket_name: str,
        object_name: str,
    ) -> Path:
        object_path = PurePosixPath(object_name)
        object_base = str(object_path.with_suffix(""))
        candidates = self.minio_client.list_objects(
            bucket_name,
            prefix=object_base,
            recursive=True,
        )
        downloaded = {}
        for candidate in candidates:
            candidate_path = PurePosixPath(candidate.object_name)
            if str(candidate_path.with_suffix("")) != object_base:
                continue
            suffix = candidate_path.suffix.lower()
            if suffix not in SHAPEFILE_EXTENSIONS:
                continue
            target = cache_dir / f"layer{suffix}"
            if not target.exists():
                self.minio_client.fget_object(bucket_name, candidate.object_name, str(target))
            downloaded[suffix] = target

        if ".shp" not in downloaded:
            raise FileNotFoundError(f"SHP 본체를 찾을 수 없습니다: {object_name}")
        if ".dbf" not in downloaded:
            logger.warning("SHP 속성 파일(DBF)이 없습니다.", extra={"file": object_name})
        return downloaded[".shp"]

    def _inspect_layer(self, prepared: PreparedDataset, layer_path: Path) -> dict[str, Any]:
        with self._open_reader(layer_path) as reader:
            source_crs, crs_warning = self._read_crs(layer_path, reader.bbox)
            bounds = self._transform_bounds(reader.bbox, source_crs)
            authority = source_crs.to_authority()
            source_crs_name = ":".join(authority) if authority else source_crs.name
            return {
                "id": layer_path.relative_to(prepared.root).as_posix(),
                "name": layer_path.stem,
                "feature_count": len(reader),
                "geometry_type": reader.shapeTypeName,
                "bounds": bounds,
                "source_crs": source_crs_name,
                "crs_warning": crs_warning,
                "fields": self._field_names(reader),
            }

    def _open_reader(self, layer_path: Path):
        try:
            encoding = self._read_dbf_encoding(layer_path)
            return shapefile.Reader(
                layer_path,
                encoding=encoding,
                encodingErrors="replace",
            )
        except (shapefile.ShapefileException, UnicodeError, OSError) as exc:
            raise GeospatialDataError(f"SHP 파일을 읽을 수 없습니다: {exc}") from exc

    def _read_dbf_encoding(self, layer_path: Path) -> str | None:
        cpg_path = layer_path.with_suffix(".cpg")
        if not cpg_path.exists():
            return None

        encoding_name = cpg_path.read_text(
            encoding="ascii",
            errors="ignore",
        ).strip().strip("\x00")
        if not encoding_name:
            return None

        normalized = encoding_name.upper().replace("_", "-")
        aliases = {
            "EUC-KR": "euc_kr",
            "KS-C-5601": "euc_kr",
            "KS-C-5601-1987": "euc_kr",
            "949": "cp949",
            "CP949": "cp949",
            "UTF8": "utf-8",
            "UTF-8": "utf-8",
        }
        encoding = aliases.get(normalized, encoding_name)
        try:
            return codecs.lookup(encoding).name
        except LookupError:
            logger.warning(
                "알 수 없는 CPG 인코딩입니다. 기본 인코딩을 사용합니다.",
                extra={"file": str(layer_path), "encoding": encoding_name},
            )
            return None

    def _read_crs(self, layer_path: Path, source_bounds) -> tuple[CRS, str | None]:
        prj_path = layer_path.with_suffix(".prj")
        if prj_path.exists():
            try:
                return CRS.from_wkt(prj_path.read_text(encoding="utf-8", errors="replace")), None
            except Exception as exc:
                raise GeospatialDataError(f"좌표계(PRJ)를 해석할 수 없습니다: {exc}") from exc

        bounds = [float(value) for value in source_bounds] if source_bounds else []
        if len(bounds) == 4 and self._looks_like_wgs84(bounds):
            return CRS.from_epsg(4326), "PRJ 파일이 없어 좌표를 WGS84로 간주했습니다."
        raise GeospatialDataError(
            "PRJ 파일이 없어 지도 좌표계를 확인할 수 없습니다. SHP와 같은 이름의 PRJ 파일을 포함해주세요."
        )

    def _transform_bounds(self, source_bounds, source_crs: CRS) -> list[float]:
        if not source_bounds:
            return []
        transformer = Transformer.from_crs(source_crs, CRS.from_epsg(4326), always_xy=True)
        transformed = transformer.transform_bounds(
            *[float(value) for value in source_bounds],
            densify_pts=21,
        )
        return [round(float(value), 8) for value in transformed]

    def _source_bbox(
        self,
        bbox: tuple[float, float, float, float],
        source_crs: CRS,
    ) -> tuple[float, float, float, float]:
        transformer = Transformer.from_crs(CRS.from_epsg(4326), source_crs, always_xy=True)
        transformed = transformer.transform_bounds(*bbox, densify_pts=21)
        return tuple(float(value) for value in transformed)

    def _shape_record_to_feature(
        self,
        shape_record,
        feature_id: int,
        to_wgs84: Transformer,
        simplify_tolerance: float,
        layer_bounds: list[float],
    ) -> dict[str, Any] | None:
        if shape_record is None or shape_record.shape is None:
            return None
        try:
            geometry = to_shapely(shape_record.shape.__geo_interface__)
            if geometry.is_empty:
                return None
            geometry = transform_geometry(to_wgs84.transform, geometry)
            geometry = self._simplify_geometry(
                geometry,
                max(0.0, float(simplify_tolerance or 0.0)),
                layer_bounds,
            )
            if geometry.is_empty:
                return None
            properties = self._record_to_dict(shape_record.record)
            return {
                "type": "Feature",
                "id": int(feature_id),
                "geometry": mapping(geometry),
                "properties": properties,
            }
        except Exception:
            logger.warning("지도 형상 변환 실패", exc_info=True)
            return None

    def _simplify_geometry(self, geometry, tolerance: float, layer_bounds: list[float]):
        if tolerance > 0:
            geometry = geometry.simplify(tolerance, preserve_topology=True)

        coordinate_count = get_num_coordinates(geometry)
        if coordinate_count <= self.max_coordinates_per_feature:
            return geometry

        span = 1.0
        if len(layer_bounds) == 4:
            span = max(
                abs(layer_bounds[2] - layer_bounds[0]),
                abs(layer_bounds[3] - layer_bounds[1]),
                1e-9,
            )
        adaptive_tolerance = max(tolerance, span / 100000)
        for _ in range(10):
            geometry = geometry.simplify(adaptive_tolerance, preserve_topology=True)
            if get_num_coordinates(geometry) <= self.max_coordinates_per_feature:
                break
            adaptive_tolerance *= 2
        return geometry

    def _select_layer(self, prepared: PreparedDataset, layer: str | None) -> Path:
        if not layer:
            return prepared.layers[0]
        for candidate in prepared.layers:
            if candidate.relative_to(prepared.root).as_posix() == layer:
                return candidate
        raise GeospatialDataError(f"ZIP 안에서 요청한 SHP 레이어를 찾을 수 없습니다: {layer}")

    def _select_fields(self, all_fields: list[str], requested: list[str] | None) -> list[str]:
        if requested:
            selected = [field for field in requested if field in all_fields]
        else:
            selected = all_fields
        return selected[: self.max_property_fields]

    def _field_names(self, reader) -> list[str]:
        names = []
        for field in reader.fields:
            name = getattr(field, "name", field[0] if field else "")
            if name and name != "DeletionFlag":
                names.append(str(name))
        return names

    def _record_to_dict(self, record) -> dict[str, Any]:
        if record is None:
            return {}
        values = record.as_dict() if hasattr(record, "as_dict") else dict(record)
        return {str(key): self._json_value(value) for key, value in values.items()}

    def _json_value(self, value):
        if value is None or isinstance(value, (str, int, float, bool)):
            if isinstance(value, float) and not math.isfinite(value):
                return None
            return value
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return str(value)

    def _sample_indices(self, total: int, limit: int) -> list[int]:
        if total <= limit:
            return list(range(total))
        step = total / limit
        return [min(total - 1, int(index * step)) for index in range(limit)]

    def _looks_like_wgs84(self, bounds: list[float]) -> bool:
        return (
            -180 <= bounds[0] <= 180
            and -90 <= bounds[1] <= 90
            and -180 <= bounds[2] <= 180
            and -90 <= bounds[3] <= 90
        )

    def _resolve_nas_path(self, bucket_name: str, file_name: str) -> Path:
        safe_bucket = (bucket_name or "").lstrip("/")
        safe_file = (file_name or "").lstrip("/")
        root = Path(self.nas_root_path).resolve()
        resolved = (root / safe_bucket / safe_file).resolve()
        if resolved != root and root not in resolved.parents:
            raise PermissionError("NAS 경로 이탈이 감지되었습니다.")
        return resolved

    def _cache_directory(self, storage: str, bucket_name: str, file_name: str) -> Path:
        namespace = self.nas_root_path if storage == "nas" else ""
        cache_key = hashlib.sha256(
            f"{storage}:{namespace}:{bucket_name}:{file_name}".encode("utf-8")
        ).hexdigest()
        return self.cache_root / cache_key

    def _get_cache_lock(self, cache_dir: Path) -> threading.Lock:
        cache_key = str(cache_dir)
        with self._cache_locks_guard:
            return self._cache_locks.setdefault(cache_key, threading.Lock())

    def _refresh_cache(self, cache_dir: Path, fingerprint: str):
        fingerprint_file = cache_dir / ".fingerprint"
        current = None
        if fingerprint_file.exists():
            current = fingerprint_file.read_text(encoding="utf-8")
        if current == fingerprint:
            return
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        fingerprint_file.write_text(fingerprint, encoding="utf-8")
