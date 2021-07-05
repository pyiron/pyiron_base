import json
import re
import sys


def convert_package_name(name, name_conv_dict):
    if name.startswith('pyiron-'):
        return name.replace('pyiron-', 'pyiron_')
    try:
        result = name_conv_dict[name]
    except KeyError:
        result = name
    return result


if not (sys.argv[0] == 'Bump' and sys.argv[2] == 'from' and sys.argv[4] == 'to'):
    raise ValueError("Title of a dependabot PR expected")

package_to_update = sys.argv[1]
from_version = sys.argv[3]
to_version = sys.argv[5]

with open('.ci_support/pypi_vs_conda_names.json', 'r') as f:
    name_conversion_dict = json.load(f)

package_name = convert_package_name(package_to_update, name_conversion_dict)
with open('.ci_support/environment.yml', 'r') as f:
    environment = f.readlines()

with open('.ci_support/environment.yml', 'w') as f:
    for line in environment:
        f.write(re.sub(
            r'(' + package_name + '.*)' + from_version,
            r'\g<1>' + to_version,
            line
        ))