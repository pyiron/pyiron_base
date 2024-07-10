import numpy

from xmlschema import XMLSchema
from qe_tools import CONSTANTS

from ase import Atoms
from importlib.resources import files

from . import schemas


def parse_pw(xml_file):
    """Parse a Quantum Espresso XML output file."""

    xml_dict = XMLSchema(str(files(schemas) / "qes_230310.xsd")).to_dict(xml_file)

    parsed_results = {}

    try:
        cell = (
            numpy.array(
                [v for v in xml_dict["output"]["atomic_structure"]["cell"].values()]
            )
            * CONSTANTS.bohr_to_ang
        )
        symbols = [
            el["@name"]
            for el in xml_dict["output"]["atomic_structure"]["atomic_positions"]["atom"]
        ]
        positions = (
            numpy.array(
                [
                    el["$"]
                    for el in xml_dict["output"]["atomic_structure"][
                        "atomic_positions"
                    ]["atom"]
                ]
            )
            * CONSTANTS.bohr_to_ang
        )

        parsed_results["ase_structure"] = Atoms(
            cell=cell,
            positions=positions,
            symbols=symbols,
            pbc=True,
        )
    except KeyError:
        pass

    try:
        parsed_results["energy"] = (
            xml_dict["output"]["total_energy"]["etot"] * CONSTANTS.ry_to_ev
        )
    except KeyError:
        pass

    try:
        parsed_results["forces"] = (
            numpy.array(xml_dict["output"]["forces"]["$"]).reshape(
                xml_dict["output"]["forces"]["@dims"]
            )
            * 2
            * CONSTANTS.ry_to_ev
            / CONSTANTS.bohr_to_ang
        )
    except KeyError:
        pass

    return parsed_results
