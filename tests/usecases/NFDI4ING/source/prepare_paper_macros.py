"""
places the correct values into the paper macros template file
"""

import string
import argparse

PARSER = argparse.ArgumentParser(
    description="Place computed values in the paper macros template file"
)
PARSER.add_argument(
    "-f", "--macro-template-file",
    required=False,  default="macros.tex.template",
    help="The macro template file"
)
PARSER.add_argument(
    "-o", "--output-macro-file",
    required=False, default="macros.tex",
    help="The macro file to be written"
)
PARSER.add_argument(
    "-s", "--domain-size",
    required=True, help="The computed domain size",
)
PARSER.add_argument(
    "-n", "--num-dofs",
    required=True, help="The computed number of dofs",
)
PARSER.add_argument(
    "-p", "--plot-data-path",
    required=True, help="The path to the data for the plot over line"
)
ARGS = vars(PARSER.parse_args())


with open(ARGS["output_macro_file"], "w") as out_file:
    with open(ARGS["macro_template_file"], "r") as in_file:
        raw = string.Template(in_file.read())
        out_file.write(raw.substitute({
            "DOMAINSIZE": ARGS["domain_size"],
            "NUMDOFS": ARGS["num_dofs"],
            "PLOTDATAPATH": ARGS["plot_data_path"]
        }))
