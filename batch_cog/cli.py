import click

from batch_cog.webcog import cog_pipeline

@click.group(short_help="CLI for creating COGs")
def batch_cog():
    pass

@batch_cog.command()
@click.argument('input', type=str, required=True)
@click.argument('output', type=str, required=True)
def create(input, output):
    splits = output.split('/')
    out_bucket = splits[2]
    out_key = '/'.join(splits[3:])
    cog_pipeline(input, out_bucket, out_key)
