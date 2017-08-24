#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

PYTHONPATH="$DIR:$PYTHONPATH" $DIR/bin/jobman "$@"

