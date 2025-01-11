#/usr/bin/env python

from subprocess import run
from pathlib    import Path
from urllib     import request
import shutil
import os

NOAAS3 = 'https://noaa-nws-global-pds.s3.amazonaws.com/fix/sfc_climo/20230925'
dstdir = './ufs_static'

def clean(prefix:str):
    for p in Path(prefix).glob('*.hdr'): p.unlink()
    for p in Path(prefix).glob('*.xml'): p.unlink()

def checkdata(dataname:str):
    if not Path(f'{dstdir}/{dataname}').exists():
        try:
            print(f'{dstdir}/{dataname} not exist, download it from NOAA s3')
            with request.urlopen(f'{NOAAS3}/{dataname}') as response, open(f'{dstdir}/{dataname}', 'wb') as out_file:
                shutil.copyfileobj(response, out_file)
            print(f"File downloaded successfully and saved to {dstdir}/{dataname}")
        except urllib.error.HTTPError as e:
            print(f"HTTP Error: {e.code} - {e.reason}")
        except urllib.error.URLError as e:
            print(f"URL Error: {e.reason}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    else:
        print(f'{dstdir}/{dataname} already exists, skip downloading...')

def gen_ufs_viirs_30s_data(prefix:str):
    dataname     = 'vegetation_type.viirs.v3.igbp.30s.nc'
    nlats, nlons = 21600, 43200
    ytile, xtile = 1200, 1200

    checkdata(dataname)

    os.makedirs(prefix, exist_ok=True)

    for jj in range(0, nlats, ytile):
        for ii in range(0, nlons, xtile):
            run([
                'gdal_translate',
                '-of', 'ENVI',
                '-ot', 'Byte',
                '-srcwin', str(ii), str(jj), str(xtile), str(ytile),
                f'{dstdir}/{dataname}',
                '{prefix}/{xs:05d}-{xe:05d}.{ys:05d}-{ye:05d}'.format(
                    prefix=prefix,
                    xs=ii+1, xe=ii+xtile,
                    ys=jj+1, ye=jj+ytile,
                )
            ])

    clean(prefix)

    index = f'''type=categorical
category_min=1
category_max=20
projection=regular_ll
dx=0.00833333
dy=-0.00833333
known_x=1.0
known_y=1.0
known_lat=89.99583
known_lon=-179.99583
wordsize=1
tile_x={xtile}
tile_y={ytile}
tile_z=1
units="category"
description="Noah-modified 20-category IGBP-MODIS landuse"
mminlu="MODIFIED_IGBP_MODIS_NOAH"
iswater=17
isice=15
isurban=13'''


    with open(f'{prefix}/index', 'w') as f:
        f.write(index)


def gen_ufs_bnu_30s_data(prefix:str):
    dataname     = 'soil_type.bnu.v3.30s.nc'
    nlats, nlons = 21600, 43200
    ytile, xtile = 1200, 1200

    checkdata(dataname)

    os.makedirs(prefix, exist_ok=True)

    for jj in range(0, nlats, ytile):
        for ii in range(0, nlons, xtile):
            run([
                'gdal_translate',
                '-of', 'ENVI',
                '-ot', 'Byte',
                '-srcwin', str(ii), str(jj), str(xtile), str(ytile),
                f'NETCDF:"./{dstdir}/{dataname}":soil_type',
                '{prefix}/{xs:05d}-{xe:05d}.{ys:05d}-{ye:05d}'.format(
                    prefix=prefix,
                    xs=ii+1, xe=ii+xtile,
                    ys=jj+1, ye=jj+ytile,
                )
            ])

    clean(prefix)

    index = f'''type=categorical
category_min=1
category_max=16
projection=regular_ll
dx=0.00833333
dy=-0.00833333
known_x=1.0
known_y=1.0
known_lat=89.99583
known_lon=-179.99583
wordsize=1
tile_x={xtile}
tile_y={ytile}
tile_z=1
units="category"
description="16-category top-layer soil type"'''


    with open(f'{prefix}/index', 'w') as f:
        f.write(index)

def gen_ufs_statsgo_30s_data(prefix:str):
    dataname     = 'soil_type.statsgo.v3.30s.nc'
    nlats, nlons = 21600, 43200
    ytile, xtile = 1200, 1200

    os.makedirs(prefix, exist_ok=True)

    for jj in range(0, nlats, ytile):
        for ii in range(0, nlons, xtile):
            run([
                'gdal_translate',
                '-of', 'ENVI',
                '-ot', 'Byte',
                '-srcwin', str(ii), str(jj), str(xtile), str(ytile),
                f'{dstdir}/{dataname}',
                '{prefix}/{xs:05d}-{xe:05d}.{ys:05d}-{ye:05d}'.format(
                    prefix=prefix,
                    xs=ii+1, xe=ii+xtile,
                    ys=jj+1, ye=jj+ytile,
                )
            ])

    clean(prefix)

    index = f'''type=categorical
category_min=1
category_max=16
projection=regular_ll
dx=0.00833333
dy=-0.00833333
known_x=1.0
known_y=1.0
known_lat=89.99583
known_lon=-179.99583
wordsize=1
tile_x={xtile}
tile_y={ytile}
tile_z=1
units="category"
description="16-category top-layer soil type"'''


    with open(f'{prefix}/index', 'w') as f:
        f.write(index)

def gen_ufs_lai_30s_data(prefix:str):
    dataname     = 'LAI_climo_pnnl.nc'
    nlats, nlons = 21600, 43200
    ytile, xtile = 1200, 1200

    os.makedirs(prefix, exist_ok=True)

    for jj in range(0, nlats, ytile):
        for ii in range(0, nlons, xtile):
            run([
                'gdal_translate',
                '-of', 'ENVI',
                '-ot', 'Byte',
                '-srcwin', str(ii), str(jj), str(xtile), str(ytile),
                f'{dstdir}/{dataname}',
                '{prefix}/{xs:05d}-{xe:05d}.{ys:05d}-{ye:05d}'.format(
                    prefix=prefix,
                    xs=ii+1, xe=ii+xtile,
                    ys=jj+1, ye=jj+ytile,
                )
            ])

    clean(prefix)

    index = f'''type=continuous
projection=regular_ll
dx=0.00833333
dy=-0.00833333
known_x=1.0
known_y=1.0
known_lat=89.995833
known_lon=-179.995833
wordsize=1
missing_value=0
tile_x=1200
tile_y=1200
tile_z=12
scale_factor=0.1
units="m^2/m^2"
description="MODIS LAI"'''

    with open(f'{prefix}/index', 'w') as f:
        f.write(index)


def gen_ufs_maxsnowalb_0p05deg_data(prefix:str):
    dataname     = 'maximum_snow_albedo.0.05.nc'
    nlats, nlons = 3600, 7200
    ytile, xtile =  200,  200

    checkdata(dataname)

    os.makedirs(prefix, exist_ok=True)

    for jj in range(0, nlats, ytile):
        for ii in range(0, nlons, xtile):
            run([
                'gdal_translate',
                '-of', 'ENVI',
                '-ot', 'Byte',
                '-scale', '0', '1', '0', '250',
                '-srcwin', str(ii), str(jj), str(xtile), str(ytile),
                f'{dstdir}/{dataname}',
                '{prefix}/{xs:05d}-{xe:05d}.{ys:05d}-{ye:05d}'.format(
                    prefix=prefix,
                    xs=ii+1, xe=ii+xtile,
                    ys=jj+1, ye=jj+ytile,
                )
            ])

    clean(prefix)

    index = f'''type=continuous
projection=regular_ll
dx=0.05
dy=0.05
known_x=1.0
known_y=1.0
known_lat=-89.975
known_lon=-179.975
wordsize=1
tile_x={xtile}
tile_y={ytile}
tile_z=1
scale_factor=0.4
missing_value=0
units="percent"
description="MODIS maximum snow albedo"'''


    with open(f'{prefix}/index', 'w') as f:
        f.write(index)


def gen_ufs_snowfreealb_0p05deg_data(prefix:str):
    '''It's not surface albedo'''
    dataname     = 'snowfree_albedo.4comp.0.05.nc'
    nlats, nlons = 3600, 7200
    ytile, xtile =  600,  600

    checkdata(dataname)

    os.makedirs(prefix, exist_ok=True)

    for jj in range(0, nlats, ytile):
        for ii in range(0, nlons, xtile):
            run([
                'gdal_translate',
                '-of', 'ENVI',
                '-ot', 'UInt16',
                '-scale', '0', '1', '0', '64000',
                '-srcwin', str(ii), str(jj), str(xtile), str(ytile),
                f'NETCDF:"{dstdir}/{dataname}":near_IR_white_sky_albedo',
                '{prefix}/{xs:05d}-{xe:05d}.{ys:05d}-{ye:05d}'.format(
                    prefix=prefix,
                    xs=ii+1, xe=ii+xtile,
                    ys=jj+1, ye=jj+ytile,
                )
            ])


    clean(prefix)

    index = f'''type=continuous
projection=regular_ll
dx=0.05
dy=0.05
known_x=1.0
known_y=1.0
known_lat=-89.975
known_lon=-179.975
wordsize=2
tile_x={xtile}
tile_y={ytile}
tile_z=12
scale_factor=0.0015625
endian=little
missing_value=0
units="percent"
description="Monthly MODIS surface albedo"'''


    with open(f'{prefix}/index', 'w') as f:
        f.write(index)


if __name__ == '__main__':
    os.makedirs(dstdir, exist_ok=True)
    prefix = '/test'
    gen_ufs_viirs_30s_data(f'{prefix}/ufs_viirs_landuse_20class_30s/')
    gen_ufs_bnu_30s_data(f'{prefix}/ufs_bnu_soiltype_30s/')
    gen_ufs_statsgo_30s_data(f'{prefix}/ufs_statsgo_soiltype_30s/')
    gen_ufs_maxsnowalb_0p05deg_data(f'{prefix}/ufs_maxsnowalb/')
    gen_ufs_snowfreealb_0p05deg_data(f'{prefix}/ufs_snowfreealb/')
    gen_ufs_lai_30s_data(f'{prefix}/ufs_lai_pnnl_30s/')
