#!/usr/bin/env python3
import sys
import argparse
import math
import rasterio
from scipy import ndimage
from rasterio import features
from shapely.geometry import shape
from shapely.ops import unary_union
import numpy as np


def erode_mask(filein, fileout, no_data, iterations):
    if iterations == 0:
        if fileout != filein:
            import shutil
            shutil.copyfile(filein, fileout)
        exit()

    with rasterio.open(filein) as src:
        array     = src.read(1)
        profile   = src.profile
        transform = src.transform

    # Create mask
    if math.isnan(no_data):
        mask = np.isnan(array)
    else:
        mask = array == no_data

    # Find the convex_hull of the image
    sh = [shape(s).convex_hull for i, (s, v) in enumerate(features.shapes(mask.astype(np.uint8), transform=transform))]
    if len(sh) > 1:
        convex_hull = unary_union(sh[0:-1]).convex_hull

        # Create new mask from convex_hull
        mask = features.geometry_mask([convex_hull], array.shape, transform)

    # Dilate no_data mask
    struct = ndimage.generate_binary_structure(2, 2)
    mask = ndimage.binary_dilation(mask, structure=struct, iterations=iterations)

    # Apply dilated mask
    array[mask] = no_data

    if np.count_nonzero(array != no_data) == 0:
        sys.exit("FATAL: {} empty".format(filein))

    # Set the nodata value as SNAP is not able to do it
    profile['nodata'] = no_data

    # Save
    if len(array.shape) == 2:
        array = array.reshape(1, array.shape[0], array.shape[1])

    with rasterio.open(fileout, 'w', **profile) as dst:
        dst.write(array)


if __name__ == '__main__':
    args_parser = argparse.ArgumentParser(description='Erode area with N iterations of 3x3 kernel, setting no-data')
    args_parser.add_argument('--file-in', type=str, required=True)
    args_parser.add_argument('--file-out', type=str, default=None)
    args_parser.add_argument('--no-data', type=float, default=0)
    args_parser.add_argument('--iterations', type=int, required=True)
    args = args_parser.parse_args()

    if args.file_out is None:
        args.file_out = args.file_in

    erode_mask(args.file_in, args.file_out, args.no_data, args.iterations)
