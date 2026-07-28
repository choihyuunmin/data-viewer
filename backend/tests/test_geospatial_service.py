import os
import tempfile
import unittest
import zipfile
from pathlib import Path

import shapefile
from pyproj import CRS, Transformer

from errors import GeospatialDataError
from scripts.create_geo_demo import build_demo
from services.geospatial_service import GeospatialService


class GeospatialServiceTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.nas_root = self.root / "nas"
        self.demo_root = self.nas_root / "geo-demo"
        self.cache_root = self.root / "cache"
        os.environ["GEO_CACHE_DIR"] = str(self.cache_root)
        build_demo(self.demo_root, announce=False)
        self.service = GeospatialService(None, str(self.nas_root))

    def tearDown(self):
        os.environ.pop("GEO_CACHE_DIR", None)
        self.temp_dir.cleanup()

    def test_zip_lists_layers_and_returns_representative_sample(self):
        details = self.service.get_dataset_details(
            "geo-demo",
            "korea-spatial-preview.zip",
            "nas",
        )

        self.assertEqual(details["dataset_type"], "geospatial")
        self.assertEqual(
            [layer["id"] for layer in details["map"]["layers"]],
            ["boundaries/regions.shp", "places/major_places.shp"],
        )

        preview = self.service.get_preview(
            "geo-demo",
            "korea-spatial-preview.zip",
            "nas",
            layer="boundaries/regions.shp",
            limit=2,
        )
        self.assertEqual(preview["metadata"]["returned"], 2)
        self.assertTrue(preview["metadata"]["sampled"])
        self.assertEqual(preview["features"][0]["properties"]["name"], "서울권")

    def test_bbox_filters_features(self):
        preview = self.service.get_preview(
            "geo-demo",
            "korea-spatial-preview.zip",
            "nas",
            layer="places/major_places.shp",
            bbox=(126.8, 37.4, 127.2, 37.7),
            limit=100,
        )

        names = [feature["properties"]["name"] for feature in preview["features"]]
        self.assertEqual(names, ["서울역", "광화문"])
        self.assertEqual(preview["metadata"]["bbox"], [126.8, 37.4, 127.2, 37.7])

    def test_direct_shapefile_is_supported(self):
        details = self.service.get_dataset_details("geo-demo", "regions.shp", "nas")

        self.assertEqual(details["total"], 3)
        self.assertEqual(details["map"]["layers"][0]["geometry_type"], "POLYGON")

    def test_projected_coordinates_are_transformed_to_wgs84(self):
        base_path = self.demo_root / "projected_place"
        to_projected = Transformer.from_crs(4326, 5179, always_xy=True)
        x, y = to_projected.transform(126.978, 37.5665)
        with shapefile.Writer(base_path, shapeType=shapefile.POINT) as writer:
            writer.field("name", "C", size=30)
            writer.point(x, y)
            writer.record("서울시청")
        base_path.with_suffix(".prj").write_text(CRS.from_epsg(5179).to_wkt(), encoding="utf-8")

        preview = self.service.get_preview(
            "geo-demo",
            "projected_place.shp",
            "nas",
            limit=10,
        )
        longitude, latitude = preview["features"][0]["geometry"]["coordinates"]
        self.assertAlmostEqual(longitude, 126.978, places=4)
        self.assertAlmostEqual(latitude, 37.5665, places=4)
        self.assertEqual(preview["metadata"]["source_crs"], "EPSG:5179")

    def test_euc_kr_cpg_is_applied_to_dbf_records(self):
        base_path = self.demo_root / "euc_kr_place"
        with shapefile.Writer(
            base_path,
            shapeType=shapefile.POINT,
            encoding="cp949",
        ) as writer:
            writer.field("name", "C", size=30)
            writer.point(129.0756, 35.1796)
            writer.record("부산광역시")
        base_path.with_suffix(".prj").write_text(CRS.from_epsg(4326).to_wkt(), encoding="utf-8")
        base_path.with_suffix(".cpg").write_text("EUC-KR", encoding="ascii")

        preview = self.service.get_preview(
            "geo-demo",
            "euc_kr_place.shp",
            "nas",
            limit=10,
        )
        self.assertEqual(preview["features"][0]["properties"]["name"], "부산광역시")

    def test_zip_path_traversal_is_rejected(self):
        unsafe_archive = self.demo_root / "unsafe.zip"
        with zipfile.ZipFile(unsafe_archive, "w") as archive:
            archive.writestr("../outside.shp", b"not-a-shapefile")

        with self.assertRaisesRegex(GeospatialDataError, "허용되지 않는 경로"):
            self.service.get_dataset_details("geo-demo", "unsafe.zip", "nas")


if __name__ == "__main__":
    unittest.main()
