#!/usr/bin/env python3
from osgeo import gdal
import argparse

def create_cog(filepath, fileout, blocksize, projection):
    d = gdal.Open(filepath)
    creation_option = [
        'TARGET_SRS='+projection,
        'SPARSE_OK=TRUE',
        'BLOCKSIZE='+str(blocksize)
    ]

    options = gdal.TranslateOptions(creationOptions=creation_option,format='COG')
    gdal.Translate(fileout,d,options=options)

if __name__ == '__main__':
    args_parser = argparse.ArgumentParser(description='Create cog from geotiff')
    args_parser.add_argument('--filepath', type=str, required=True)
    args_parser.add_argument('--fileout', type=str, required=True)
    args_parser.add_argument('--blocksize', type=str, required=True)
    args_parser.add_argument('--projection', type=str, required=True)
    args = args_parser.parse_args()

    create_cog(args.filepath, args.fileout, args.blocksize, args.projection)

