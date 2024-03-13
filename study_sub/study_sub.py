# Standard library imports
import copy
from typing import Any, Self

# Third party imports
from filelock import SoftFileLock

# Local imports
from study_gen._nested_dicts import nested_get
from .dict_yaml_utils import load_yaml, write_yaml

class StudySub:
    def __init__(self: Self, path_tree: str):
        # Path to study files
        self.path_tree = path_tree

        # Lock file to avoid concurrent access
        self.lock = SoftFileLock(f"{self.path_tree}.lock", timeout=5)

        # Load the corresponding yaml as dicts
        self.dict_tree = load_yaml(self.path_tree)


    def get_job_status(self: Self, l_keys: list[str]):
        # Refresh the tree dict
        self.dict_tree = load_yaml(self.path_tree)

        # Return the job status
        return nested_get(self.dict_tree, l_keys + ["status"])

    def get_dic_all_jobs(self):
        # TODO

    def submit(self: Self):
        for job in self.dic_all_jobs:
            l_keys = self.dic_all_jobs[job]["l_keys"]
            self.get_job_status(l_keys)

        print(self.dic_all_jobs)
