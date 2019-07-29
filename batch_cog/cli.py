import click

from batch_cog.webcog import cog_1band_pipeline, cog_3band_pipeline

@click.group(short_help="CLI for creating COGs")
def batch_cog():
    pass

@batch_cog.command(name="create-1band-cog")
@click.argument('input', type=str, required=True)
@click.argument('output', type=str, required=True)
def create_1band_cog(input, output):
    splits = output.split('/')
    out_bucket = splits[2]
    out_key = '/'.join(splits[3:])
    cog_1band_pipeline(input, out_bucket, out_key)

@batch_cog.command(name="create-3band-cog")
@click.argument('band1', type=str, required=True)
@click.argument('band2', type=str, required=True)
@click.argument('band3', type=str, required=True)
@click.argument('output', type=str, required=True)
def create_3band_cog(band1, band2, band3, output):
    splits = output.split('/')
    out_bucket = splits[2]
    out_key = '/'.join(splits[3:])
    cog_3band_pipeline([band1, band2, band3], out_bucket, out_key)

