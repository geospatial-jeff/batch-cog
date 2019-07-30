import tempfile
import subprocess
import shutil
import os
import uuid

import boto3
import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.warp import calculate_default_transform, reproject, Resampling

s3_client = boto3.client('s3')

def reproject_raster(infile, outfile, out_epsg):

    dst_crs = f"EPSG:{out_epsg}"

    if type(infile) == MemoryFile:
        file_handler = infile.open()
    else:
        file_handler = rasterio.open(infile)

    with file_handler as src:
        transform, width, height = calculate_default_transform(
            src.crs, dst_crs, src.width, src.height, *src.bounds
        )
        kwargs = src.meta.copy()
        kwargs.update({
            'crs': dst_crs,
            'transform': transform,
            'width': width,
            'height': height,
            'nodata': 0,
        })
        if outfile:
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
        else:
            return

def linear_stretch(src):
    bands = []
    for band in src.read():
        rescaled = (band - band.min()) * (1 / (band.max() - band.min()) * 255)
        bands.append(rescaled.astype('uint8'))
    return np.stack(bands, axis=0)[0,:,:]

def read_profile(infile):
    with rasterio.open(infile) as src:
        return src.profile

def create_1band_cog(infile, outfile, profile='deflate', web_optimized=False):
    command = f"rio cogeo create {infile} {outfile} --cog-profile {profile} --bidx 1 --nodata 0"
    if web_optimized:
        command += " --web-optimized"
    subprocess.call(command, shell=True)

def create_3band_cog(infile, outfile, profile='webp', web_optimized=False, mask=False):
    command = f"rio cogeo create {infile} {outfile} --cog-profile {profile} --nodata 0"
    if web_optimized:
        command += " --web-optimized"
    if mask:
        command += " --add-mask --bidx 1,2,3"
    subprocess.call(command, shell=True)

def cog_1band_pipeline(infile, out_bucket, out_key):
    tempdir = tempfile.mkdtemp()
    projfile = os.path.join(tempdir, str(uuid.uuid4()) + '.tif')
    cogfile = os.path.join(tempdir, str(uuid.uuid4()) + '.tif')

    print("Reprojecting to 3857.")
    reproject_raster(infile, projfile, out_epsg=3857)

    print("Creating COG.")
    create_1band_cog(projfile, cogfile, profile='deflate', web_optimized=True)

    print("Uploading COG to S3.")
    s3_client.upload_file(cogfile, out_bucket, out_key)

    # Cleaning up

def cog_3band_pipeline(bands, out_bucket, out_key):
    tempdir = tempfile.mkdtemp()
    try:
        projfile = os.path.join(tempdir, str(uuid.uuid4()) + '.tif')
        cogfile = os.path.join(tempdir, str(uuid.uuid4()) + '.tif')

        profile = read_profile(bands[0])
        profile['dtype'] = 'uint8'
        profile['count'] = 3

        print("Creating 8-bit band stack.")
        memfile = MemoryFile()
        with memfile.open(**profile) as dst:
            for idx, band in enumerate(bands):
                print("Processing band {}: {}".format(idx+1, band))
                with rasterio.open(band) as src:
                    dst.write(linear_stretch(src), idx+1)

        print("Reprojecting to 3857.")
        reproject_raster(memfile, projfile, out_epsg=3857)

        print("Creating COG.")
        create_3band_cog(projfile, cogfile, web_optimized=True, mask=True)

        print("Uploading COG to S3.")
        s3_client.upload_file(cogfile, out_bucket, out_key)
    except:
        shutil.rmtree(tempdir)
    finally:
        shutil.rmtree(tempdir)





bands = [
    '/home/slingshot/Documents/Conrad/FireProject/batch-cog/sample_data/Day_1_Merged_transparent_mosaic_red_3857.tif',
    '/home/slingshot/Documents/Conrad/FireProject/batch-cog/sample_data/Day_1_Merged_transparent_mosaic_green_3857.tif',
    '/home/slingshot/Documents/Conrad/FireProject/batch-cog/sample_data/Day_1_Merged_transparent_mosaic_blue_3857.tif'
]

cog_3band_pipeline(bands, '', '')


        # with rasterio.open(band) as src:
        #     stack.append(linear_stretch(src))
    # band_stack = np.stack(stack, axis=0)



    # stacked = [linear_stretch(x) for x in bands]

    #
    # # Read profile from a single band
    # profile = read_profile(bands[0])
    # profile['dtype'] = 'uint8'
    # profile['count'] = 3
    #
    # # Create 8-bit band stack and save to temp file
    # print("Creating 8-bit band stack.")
    # stack = []
    # for idx, band in enumerate(bands):
    #     print("Processing band {}: {}".format(idx+1, band))
    #     projfile = os.path.join(tempdir, str(uuid.uuid4()) + '.tif')
    #     reproject_raster(band, projfile, out_epsg=3857)
    #
    #     with rasterio.open(projfile) as src:
    #         stretched = linear_stretch(src)
    #         stack.append(stretched)
    # stacked = np.stack(stack, axis=0)
    #
    # with rasterio.open(stackedfile, 'w', **profile) as dst:
    #     dst.write(stacked)
    #
    # print("Creating COG.")
    # # Convert 8-bit band stac to COG
    # create_3band_cog(stackedfile, cogfile, web_optimized=True, mask=True)
    #
    # print("Uploading COG to S3.")
    # s3_client.upload_file(cogfile, out_bucket, out_key)
    #
    # shutil.rmtree(tempdir)

# {'band1': 'https://flyalchemy-raster-ingest.s3.amazonaws.com/Day1_Flight1_20190127_altum_red.tif', 'band2': 'https://flyalchemy-raster-ingest.s3.amazonaws.com/Day1_Flight1_20190127_altum_green.tif', 'band3': 'https://flyalchemy-raster-ingest.s3.amazonaws.com/Day1_Flight1_20190127_altum_blue.tif', 'outfile': 's3://flyalchemy-cogs/Day1_Flight1_20190127_altum_rgb_COG.tif'