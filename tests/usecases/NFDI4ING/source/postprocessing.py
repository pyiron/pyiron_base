"""
$ pvbatch postprocessing.py -h
"""

import sys
import argparse
from paraview.simple import (
    PlotOverLine,
    PVDReader,
    SaveData,
    UpdatePipeline,
)


def main(args):
    pvd_file = args.pvd
    source = PVDReader(registrationName="poisson.pvd", FileName=[pvd_file])
    source.PointArrays = [args.field]
    UpdatePipeline()

    (xmin, xmax, ymin, ymax, zmin, zmax) = source.GetDataInformation().GetBounds()
    # init the 'Line' selected for 'Source'
    plotOverLine1 = PlotOverLine(
        registrationName="PlotOverLine1", Input=source, Source="Line"
    )
    plotOverLine1.Source.Point1 = [xmin, ymin, zmin]
    plotOverLine1.Source.Point2 = [xmax, ymax, zmax]
    UpdatePipeline()

    # save data
    SaveData(
        args.csv,
        proxy=plotOverLine1,
        ChooseArraysToWrite=1,
        PointDataArrays=["arc_length", args.field, "vtkValidPointMask"],
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog=f"pvbatch {__file__}",
        description="Plots the solution over a line and writes the data to file.",
        usage="%(prog)s [options] pvd csv",
    )
    parser.add_argument("pvd", type=str, help="The source pvd filepath.")
    parser.add_argument("csv", type=str, help="The target csv filepath.")
    parser.add_argument(
        "--field",
        type=str,
        default="u",
        help="Field variable to plot (default: u)",
    )
    args = parser.parse_args(sys.argv[1:])
    main(args)
