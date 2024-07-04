import math
import os

import pint


def _size_conversion(size: pint.Quantity):
    sign_prefactor = 1
    if size < 0:
        sign_prefactor = -1
        size *= -1
    elif size == 0:
        return size

    prefix_index = math.floor(math.log2(size) / 10) - 1
    prefix = ["Ki", "Mi", "Gi", "Ti", "Pi"]

    size *= sign_prefactor
    if prefix_index < 0:
        return size
    elif prefix_index < 5:
        return size.to(f"{prefix[prefix_index]}byte")
    else:
        return size.to(f"{prefix[-1]}byte")


def get_folder_size(path):
    size = (
        sum(
            [
                sum([os.path.getsize(os.path.join(path, f)) for f in files])
                for path, dirs, files in os.walk(path)
            ]
        )
        * pint.UnitRegistry().byte
    )
    return _size_conversion(size)
