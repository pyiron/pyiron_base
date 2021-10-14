#!/bin/bash

versions=('0.1.48' '0.2.24' '0.3.0')

for v in ${versions[@]}; do
    pip install --no-deps pyiron_base==$v
    echo "Before save";
    for t in tests/backwards/*save.py; do
        echo "Running $t";
        python $t
    done
done

pip install --no-deps .
i=0;
echo "Before loading";
for t in tests/backwards/*load.py; do
    echo "Running $t";
    python $t || i=$((i+1));
done

# push error to next level
if [ "$i" -gt 0 ]; then
    exit 1;
fi;
