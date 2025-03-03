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
    return ds


def calculate_intersection():
    # 获取流域数据
    basin_df = get_basin_data()
    basin_geom = parse_geometry(basin_df.iloc[0]['geotext'])

    if basin_geom is None:
        raise Exception("无法解析流域边界")

    # 获取GFS数据
    ds = get_grid_data()

    # 切片数据
    ds_slice = ds.sel(
        latitude=slice(basin_df.iloc[0]['lat_min'] - 1, basin_df.iloc[0]['lat_max'] + 1),
        longitude=slice(basin_df.iloc[0]['lon_min'] - 1, basin_df.iloc[0]['lon_max'] + 1)
    )

    # 转换为DataFrame
    grib_df = ds_slice[['tp']].to_dataframe().reset_index()

    # 计算格点大小和相交面积
    dx = abs(ds.longitude.values[1] - ds.longitude.values[0])
    dy = abs(ds.latitude.values[1] - ds.latitude.values[0])
    grid_area = dx * dy

    results = []
    for _, row in grib_df.iterrows():
        grid_poly = Polygon([
            (row['longitude'] - dx / 2, row['latitude'] - dy / 2),
            (row['longitude'] + dx / 2, row['latitude'] - dy / 2),
            (row['longitude'] + dx / 2, row['latitude'] + dy / 2),
            (row['longitude'] - dx / 2, row['latitude'] + dy / 2)
        ])

        if grid_poly.intersects(basin_geom):
            intersection = grid_poly.intersection(basin_geom)
            intersection_area = intersection.area
            intersection_percent = (intersection_area / grid_area) * 100

            results.append({
                'basin_code': basin_df.iloc[0]['basin_code'],
                'longitude': row['longitude'],
                'latitude': row['latitude'],
                'tp': row['tp'] * 1000,  # 转换为毫米
                'intersection_area': intersection_area,
                'intersection_percent': intersection_percent
            })

    result_df = pd.DataFrame(results)

    # 保存结果
    output_file = 'D:/herbin/merged_data.xlsx'
    result_df.to_excel(output_file, index=False)
    print(f"数据已保存到: {output_file}")
    print(f"总格点数: {len(result_df)}")
    print("\n数据示例:")
    print(result_df.head())

    # 输出统计信息
    print("\n统计信息:")
    print(f"平均相交百分比: {result_df['intersection_percent'].mean():.2f}%")
    print(f"最大相交百分比: {result_df['intersection_percent'].max():.2f}%")
    print(f"最小相交百分比: {result_df['intersection_percent'].min():.2f}%")


if __name__ == "__main__":
    try:
        print("开始处理数据...")
        calculate_intersection()
        print("处理完成")
    except Exception as e:
        print(f"错误: {str(e)}")