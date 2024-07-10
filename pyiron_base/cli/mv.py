# coding: utf-8
# Copyright (c) Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department
# Distributed under the terms of "New BSD License", see the LICENSE file.
"""
Move a pyiron project to a new location.
"""

from pyiron_base.project.generic import Project


def register(parser):
    parser.add_argument("src", help="source project")
    parser.add_argument("dst", help="destination project")


def main(args):
    src = Project(args.src)
    dst = Project(args.dst)
    src.move_to(dst)
