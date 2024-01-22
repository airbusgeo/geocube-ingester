#!/usr/bin/env python3
import glob
import shutil
from osgeo import gdal
import os
import argparse

def extract_s2_files(workdir, pattern_out):
    search_path = os.path.join(workdir, "*", "GRANULE", "*", "IMG_DATA", "*_B??.jp2")
    files = glob.glob(search_path, recursive=True)

    print(search_path)
    for file in files:
        print(file)
        shutil.copyfile(file, os.path.join(workdir, pattern_out.replace("*", file[-7:-4])))
        #os.rename(file, os.path.join(workdir, file[-7:]))


if __name__ == '__main__':
    args_parser = argparse.ArgumentParser(description='Extract s2 files')
    args_parser.add_argument('--workdir', type=str, required=True)
    args_parser.add_argument('--pattern-out', type=str, required=True)
    args = args_parser.parse_args()

    extract_s2_files(args.workdir, args.pattern_out)

