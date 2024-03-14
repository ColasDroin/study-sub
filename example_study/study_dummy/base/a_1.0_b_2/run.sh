#!/bin/bash
source /Users/cdroin/Desktop/study-sub/.venv/bin/activate
cd /Users/cdroin/Desktop/study-sub/example_study/study_dummy/base/a_1.0_b_2
python some_more_computations.py > output_python.txt 2> error_python.txt
rm -rf final_* modules optics_repository optics_toolkit tools tracking_tools temp mad_collider.log __pycache__ twiss* errors fc* optics_orbit_at*
