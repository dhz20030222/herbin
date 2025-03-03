import pandas as pd
from sqlalchemy import create_engine
import xarray as xr
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
        lat_max
    FROM t_basin_geom_area 
    WHERE basin_code = '10310500'
    """

    return pd.read_sql(sql, engine)


def get_grid_slice():
    gfs_file = 'D:/herbin/gfs.t00z.pgrb2.0p25.f004'
    ds = xr.open_dataset(gfs_file, engine='cfgrib', decode_timedelta=True)

    basin_df = get_basin_data()
    basin = basin_df.iloc[0]

    # 扩大边界范围
    lat_min = basin['lat_min'] - 1
    lat_max = basin['lat_max'] + 1
    lon_min = basin['lon_min'] - 1
    lon_max = basin['lon_max'] + 1

    # 切片数据并只保留tp
    ds_slice = ds.sel(
        latitude=slice(lat_min, lat_max),
        longitude=slice(lon_min, lon_max)
    )[['tp']]

    # 转换为DataFrame并只保留需要的列
    df = ds_slice.to_dataframe().reset_index()[['longitude', 'latitude', 'tp']]

    # 转换降水量单位为毫米
    df['tp'] = df['tp'] * 1000

    # 保存为CSV
    output_csv = 'D:/herbin/basin_slice.csv'
    df.to_csv(output_csv, index=False)
    print(f"数据已保存到: {output_csv}")
    print(f"数据范围: 经度 {lon_min}°E - {lon_max}°E, 纬度 {lat_min}°N - {lat_max}°N")
    print(f"总记录数: {len(df)}")


if __name__ == "__main__":
    try:
        print("提取流域区域数据...")
        get_grid_slice()
        print("处理完成")
    except Exception as e:
        print(f"错误: {str(e)}")