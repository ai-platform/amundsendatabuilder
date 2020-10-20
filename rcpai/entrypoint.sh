#!/bin/bash

if [ -z "${scriptpath_env}" ]; then
    echo "entrypoint unset"
    exit 1
fi
python3 ${scriptpath_env} $@
