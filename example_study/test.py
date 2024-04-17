# %%
from study_sub import StudySub

# %%
study_sub = StudySub(
    path_tree="study_dummy/tree.yaml",
    path_python_environment="/afs/cern.ch/work/c/cdroin/private/study-sub/.venv",
)

# %%
study_sub.configure_jobs()

# %%
study_sub.generate_run_files()

# %%
study_sub.submit()

# %%
