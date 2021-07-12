import json
import re
import sys

import yaml


def convert_package_name(name, name_conv_dict):
    if name.startswith('pyiron-'):
        return name.replace('pyiron-', 'pyiron_')
    try:
        result = name_conv_dict[name]
    except KeyError:
        result = name
    return result


if len(sys.argv) != 7 or not (sys.argv[1] == 'Bump' and sys.argv[3] == 'from' and sys.argv[5] == 'to'):
    raise ValueError(f"Title of a dependabot PR 'Bump <package> from <version> to <version>' expected, "
                     f"but got {' '.join(sys.argv[1:])}")

package_to_update = sys.argv[2]
from_version = sys.argv[4]
to_version = sys.argv[6]

with open('.ci_support/pypi_vs_conda_names.json', 'r') as f:
    name_conversion_dict = json.load(f)

package_name = convert_package_name(package_to_update, name_conversion_dict)
with open('.ci_support/environment.yml', 'r') as f:
    environment = yaml.safe_load(f)

updated_dependencies = []

for dep in environment['dependencies']:
    updated_dependencies.append(re.sub(
         r'(' + package_name + '.*)' + from_version,
         r'\g<1>' + to_version,
         dep
    ))

environment['dependencies'] = updated_dependencies

with open('.ci_support/environment.yml', 'w') as f:
    yaml.safe_dump(environment, f)
