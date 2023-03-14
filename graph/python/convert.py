#!/usr/bin/env python3
import argparse
from osgeo import gdal
import gdalconst
import numpy as np
import uuid


def cast_and_convert(filein, fileout, range_in, dformat_out, tiling):
    if len(range_in) != 2:
        print("ERROR: wrong format for rangein (Expecting rangein=src_min,src_max")
        exit(1)

    if len(dformat_out) != 4:
        print("ERROR: wrong format for dformat-out (Expecting dformat-out=GDALdtype,nodata,min,max")
        exit(1)

    dtype = np.dtype(dformat_out[0]).name
    gdal_dtype = {
        "uint8": gdalconst.GDT_Byte,
        "uint16": gdalconst.GDT_UInt16,
        "int16": gdalconst.GDT_Int16,
        "uint32": gdalconst.GDT_UInt32,
        "int32": gdalconst.GDT_Int32,
        "float32": gdalconst.GDT_Float32,
        "float64": gdalconst.GDT_Float64,
        "complex64": gdalconst.GDT_CFloat32,
        "complex128": gdalconst.GDT_CFloat64,
    }
    if dtype in gdal_dtype:
        dtype = gdal_dtype[dtype]
    else:
        print("ERROR: wrong format for dformat-out (Expecting dformat-out=GDALdtype,nodata,min,max")
        exit(1)

    options = {
        "outputType": dtype,
        "scaleParams": [range_in + dformat_out[2:4]],
        "noData": dformat_out[1],
    }

    gtiff_options = {
        "format": "GTiff",
        "creationOptions": [f"BLOCKXSIZE={tiling}", f"BLOCKYSIZE={tiling}", "TILED=YES", "SPARSE_OK=TRUE"]
    }

    if fileout != filein:
        gdal.Translate(fileout, filein, **options, **gtiff_options)
    else:
        ds = gdal.Translate("/vsimem/" + uuid.uuid4().hex, filein, **options)
        gdal.Translate(fileout, ds, **gtiff_options)


if __name__ == '__main__':
    args_parser = argparse.ArgumentParser(description='Cast and convert dataset to geotiff')
    args_parser.add_argument('--file-in', type=str, required=True)
    args_parser.add_argument('--file-out', type=str, default=None)
    args_parser.add_argument('--range-in', type=str, required=True)
    args_parser.add_argument('--dformat-out', type=str, required=True)
    args_parser.add_argument('--tiling', type=int, default=256)
    args = args_parser.parse_args()

    if args.file_out is None:
        args.file_out = args.file_in

    cast_and_convert(args.file_in, args.file_out, args.range_in.split(","), args.dformat_out.split(","), args.tiling)
