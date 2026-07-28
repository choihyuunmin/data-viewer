import argparse
import shutil
import zipfile
from pathlib import Path

import shapefile
from pyproj import CRS


REGIONS = [
    ("서울권", "수도권", 126.76, 37.41, 127.19, 37.70, 9500000),
    ("부산권", "동남권", 128.82, 34.98, 129.31, 35.38, 3290000),
    ("제주권", "제주", 126.15, 33.20, 126.95, 33.60, 670000),
]

PLACES = [
    ("서울역", "교통", 126.9707, 37.5547),
    ("광화문", "행정", 126.9769, 37.5759),
    ("부산역", "교통", 129.0403, 35.1151),
    ("해운대", "관광", 129.1588, 35.1587),
    ("제주공항", "교통", 126.4930, 33.5104),
]


def write_projection(base_path: Path):
    base_path.with_suffix(".prj").write_text(
        CRS.from_epsg(4326).to_wkt(),
        encoding="utf-8",
    )
    base_path.with_suffix(".cpg").write_text("UTF-8", encoding="ascii")


def write_regions(base_path: Path):
    with shapefile.Writer(base_path, shapeType=shapefile.POLYGON, encoding="utf-8") as writer:
        writer.field("name", "C", size=40)
        writer.field("region", "C", size=40)
        writer.field("population", "N", size=12, decimal=0)
        for name, region, min_x, min_y, max_x, max_y, population in REGIONS:
            writer.poly(
                [[
                    [min_x, min_y],
                    [min_x, max_y],
                    [max_x, max_y],
                    [max_x, min_y],
                    [min_x, min_y],
                ]]
            )
            writer.record(name, region, population)
    write_projection(base_path)


def write_places(base_path: Path):
    with shapefile.Writer(base_path, shapeType=shapefile.POINT, encoding="utf-8") as writer:
        writer.field("name", "C", size=40)
        writer.field("category", "C", size=20)
        for name, category, longitude, latitude in PLACES:
            writer.point(longitude, latitude)
            writer.record(name, category)
    write_projection(base_path)


def build_demo(target_root: Path, announce: bool = True):
    target_root.mkdir(parents=True, exist_ok=True)
    source_root = target_root / "source"
    if source_root.exists():
        shutil.rmtree(source_root)
    (source_root / "boundaries").mkdir(parents=True)
    (source_root / "places").mkdir(parents=True)

    region_base = source_root / "boundaries" / "regions"
    place_base = source_root / "places" / "major_places"
    write_regions(region_base)
    write_places(place_base)

    archive_path = target_root / "korea-spatial-preview.zip"
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for file_path in sorted(source_root.rglob("*")):
            if file_path.is_file():
                archive.write(file_path, file_path.relative_to(source_root))

    direct_base = target_root / "regions"
    for source_file in region_base.parent.glob(f"{region_base.name}.*"):
        shutil.copy2(source_file, direct_base.with_suffix(source_file.suffix))

    if announce:
        print(archive_path)
        print(direct_base.with_suffix(".shp"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target",
        type=Path,
        default=Path.home() / "Downloads" / "krihs-nas" / "geo-demo",
    )
    args = parser.parse_args()
    build_demo(args.target.expanduser().resolve())


if __name__ == "__main__":
    main()
