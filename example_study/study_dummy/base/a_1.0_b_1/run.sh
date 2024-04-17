#!/bin/bash
source /afs/cern.ch/work/c/cdroin/private/study-sub/.venv/bin/activate
cd /afs/cern.ch/work/c/cdroin/private/study-sub/example_study/study_dummy/base/a_1.0_b_1
python some_more_computations.py > output_python.txt 2> error_python.txt

# Ensure job run was successful and tag as finished
if [ $? -eq 0 ]; then
    python -m study_sub.scripts.log_finish /afs/cern.ch/work/c/cdroin/private/study-sub/example_study/study_dummy/tree.yaml base a_1.0_b_1 some_more_computations
fi
