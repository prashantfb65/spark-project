#!/bin/sh
mkdir -p $HOME/.pythonvenv
python3 -m venv $HOME/.pythonvenv/spark-project
source $HOME/.pythonvenv/spark-project/bin/activate
export PATH="$HOME/.pythonvenv/spark-project/bin:$PATH"
export PYTHONDONTWRITEBYTECODE=1
