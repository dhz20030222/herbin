import pandas as pd
from sqlalchemy import create_engine
import xarray as xr
from shapely.geometry import Polygon
import numpy as np


def get_basin_data():
    config = {
        "database": "water",
        "user": "postgres",
        "password": "water",
        "host": "10.55.0.102",
        "port": "5432"
    }

    engine = create_engine(
        f'postgresql://{config["user"]}:{config["password"]}@{config["host"]}:{config["port"]}/{config["database"]}')

    sql = """
    SELECT 
        basin_code,
        lon_min,
        lon_max,
        lat_min,
        lat_max,
        geotext
    FROM t_basin_geom_area 
    WHERE basin_code = '10310500'
    """

    return pd.read_sql(sql, engine)


def parse_geometry(geotext):
    try:
        coords_str = geotext.strip()
        coordinates = []

        if ',' in coords_str:
            coord_pairs = coords_str.split(' ')
            for pair in coord_pairs:
                if ',' in pair:
                    lat, lon = pair.split(',')
                    coordinates.append([float(lon), float(lat)])

        return Polygon(coordinates)
    except Exception as e:
        print(f"解析几何数据错误: {str(e)}")
        return None


def get_grid_data():
    gfs_file = 'D:/herbin/gfs.t00z.pgrb2.0p25.f004'
    ds = xr.open_dataset(gfs_file, engine='cfgrib', decode_timedelta=True)

    # 打印所有变量及其属性
    for var in ds.variables:
        print(f"\n变量名: {var}")
        print(f"属性: {ds[var].attrs}")

    return ds


def calculate_grid_info():
    # 获取流域数据
    basin_df = get_basin_data()
    basin_geom = parse_geometry(basin_df.iloc[0]['geotext'])

    if basin_geom is None:
        raise Exception("无法解析流域边界")

    # 获取GFS数据
    ds = get_grid_data()

    # 检查可用的降水相关变量
    precip_vars = [var for var in ds.variables if 'tp' in var or 'precip' in var or 'rain' in var]
    print(f"\n找到的降水相关变量: {precip_vars}")

    results = []
    lons = ds.longitude.values
    lats = ds.latitude.values
    dx = abs(lons[1] - lons[0]) / 2
    dy = abs(lats[1] - lats[0]) / 2

    for lon in lons:
        for lat in lats:
            grid_poly = Polygon([
                (lon - dx, lat - dy),
                (lon + dx, lat - dy),
                (lon + dx, lat + dy),
                (lon - dx, lat + dy)
            ])

            if grid_poly.intersects(basin_geom):
                intersection = grid_poly.intersection(basin_geom)
                area = intersection.area

                point_data = {
                    'basin_code': basin_df.iloc[0]['basin_code'],
                    'longitude': lon,
                    'latitude': lat,
                    'intersection_area': area
                }

                # 尝试获取所有降水相关变量的值
                for var in precip_vars:
                    try:
                        value = float(ds[var].sel(longitude=lon, latitude=lat).values)
                        point_data[var] = value * 1000  # 转换为毫米
                    except:
                        point_data[var] = None

                results.append(point_data)

    df = pd.DataFrame(results)

    output_file = 'D:/herbin/grid_info.xlsx'
    df.to_excel(output_file, index=False)
    print(f"\n数据已保存到: {output_file}")
    print(f"包含变量: {', '.join(df.columns)}")
    print(f"格点数: {len(df)}")


if __name__ == "__main__":
    try:
        print("开始处理数据...")
        calculate_grid_info()
        print("处理完成")
    except Exception as e:
        print(f"错误: {str(e)}")