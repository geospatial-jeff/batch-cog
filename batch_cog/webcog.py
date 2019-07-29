import tempfile
import subprocess
import shutil
import os
import uuid

import boto3
import numpy as np
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling

s3_client = boto3.client('s3')

def reproject_raster(infile, outfile, out_epsg):

    dst_crs = f"EPSG:{out_epsg}"

    with rasterio.open(infile) as src:
        transform, width, height = calculate_default_transform(
            src.crs, dst_crs, src.width, src.height, *src.bounds
        )
        kwargs = src.meta.copy()
        kwargs.update({
            'crs': dst_crs,
            'transform': transform,
            'width': width,
            'height': height
        })

        with rasterio.open(outfile, 'w', **kwargs) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.nearest
                )

def linear_stretch(src):
    bands = []
    for band in src.read():
        rescaled = (band - band.min()) * (1 / (band.max() - band.min()) * 255)
        bands.append(rescaled.astype('uint8'))
    return np.stack(bands, axis=0)

def create_cog(infile, outfile, profile='deflate', web_optimized=False):
    command = f"rio cogeo create {infile} {outfile} --cog-profile {profile} --add-mask --bidx 1 --nodata 0"
    if web_optimized:
        command += " --web-optimized"
    subprocess.call(command, shell=True)

def cog_pipeline(infile, out_bucket, out_key):
    tempdir = tempfile.mkdtemp()
    projfile = os.path.join(tempdir, str(uuid.uuid4()) + '.tif')
    cogfile = os.path.join(tempdir, str(uuid.uuid4()) + '.tif')
    print("Reprojecting to 3857.")
    reproject_raster(infile, projfile, out_epsg=3857)

    print("Creating COG.")
    create_cog(projfile, cogfile, profile='deflate', web_optimized=True)

    print("Uploading COG to S3.")
    s3_client.upload_file(cogfile, out_bucket, out_key)

    # Cleaning up
    shutil.rmtree(tempdir)


# infile = '/home/slingshot/Documents/Conrad/FireProject/batch-cog/sample_data/Day_1_Merged_transparent_mosaic_green_3857.tif'
# out_bucket = 'fireproject-tiling-test'
# out_key = 'test2/green_cog_test.tif'
#
# cog_pipeline(infile, out_bucket, out_key)

# with rasterio.open('/home/slingshot/Documents/Conrad/FireProject/batch-cog/sample_data/Day_1_Merged_transparent_mosaic_red_3857.tif') as src:
#     profile = src.profile
#     profile['dtype'] = 'uint8'
#     profile['nodata'] = 0
#
#     with rasterio.open('/home/slingshot/Documents/Conrad/FireProject/batch-cog/sample_data/red_8bit.tif', 'w', **profile) as dst:
#         dst.write(linear_stretch(src))
#
# create_cog('/home/slingshot/Documents/Conrad/FireProject/batch-cog/sample_data/red_8bit.tif',
#            '/home/slingshot/Documents/Conrad/FireProject/batch-cog/sample_data/cogs/red_8bit_webp_masked_cog.tif',
#            profile='webp')