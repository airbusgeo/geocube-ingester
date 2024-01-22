#!/usr/bin/env python3
from osgeo import gdal
import os
import argparse

def search_file(directory = None, file = None):
    assert os.path.isdir(directory)
    for cur_path, _, files in os.walk(directory):
        if file in files:
            return os.path.join(directory, cur_path, file)
    return None

def extract_dimap_files(workdir, file_ms_out, file_pan_out, constellation):
    creation_option = [
        'TILED=YES'
    ]

    options = gdal.TranslateOptions(creationOptions=creation_option)

    file = ''
    if constellation == 'PHR':
        file = search_file(workdir,'VOL_PHR.XML')
    if constellation == 'SPOT':
        file = search_file(workdir,'SPOT_VOL.XML')

    d = gdal.Open(os.path.join(workdir, file))
    for subDataset in d.GetSubDatasets():
        sd = gdal.Open(subDataset[0])
        metadata = sd.GetMetadata()
        if metadata["SPECTRAL_PROCESSING"] == "P":
            out_file = file_pan_out
        elif metadata["SPECTRAL_PROCESSING"] == "MS":
            out_file = file_ms_out
        else:
            continue
        gdal.Translate(out_file, sd, format='GTiff',options=options)



if __name__ == '__main__':
    args_parser = argparse.ArgumentParser(description='Extract dimap files to geotiff')
    args_parser.add_argument('--workdir', type=str, required=True)
    args_parser.add_argument('--file-ms-out', type=str, required=True)
    args_parser.add_argument('--file-pan-out', type=str, required=True)
    args_parser.add_argument('--constellation', type=str, required=True)
    args = args_parser.parse_args()

    extract_dimap_files(args.workdir, args.file_ms_out, args.file_pan_out, args.constellation)

