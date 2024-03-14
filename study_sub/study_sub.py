# Standard library imports
from typing import Self

# Third party imports
from filelock import SoftFileLock

# Local imports
from study_gen._nested_dicts import nested_get

from .config import ConfigJobs
from .dict_yaml_utils import load_yaml


class StudySub:
    def __init__(self: Self, path_tree: str):
        # Path to study files
        self.path_tree = path_tree

        # Lock file to avoid concurrent access (softlock as several platforms are used)
        self.lock = SoftFileLock(f"{self.path_tree}.lock", timeout=5)

    # dict_tree as a property so that it is reloaded every time it is accessed
    @property
    def dict_tree(self: Self):
        return load_yaml(self.path_tree)

    # Property for the same reason
    def configure_jobs(self: Self):
        # Lock since we are modifying the tree
        with self.lock:
            ConfigJobs(self.dict_tree).find_and_configure_jobs()

    def get_all_jobs(self: Self):
        return ConfigJobs(self.dict_tree).find_all_jobs()

    def get_job_status(self: Self, l_keys: list[str], dict_tree=None):
        # Passing the dict_tree as an argument to avoid reloading it
        if dict_tree is None:
            dict_tree = self.dict_tree
        # Return the job status
        return nested_get(dict_tree, l_keys + ["status"])

    def submit(self: Self):
        dict_tree = self.dict_tree
        dic_all_jobs = self.get_all_jobs()
        for job in dic_all_jobs:
            l_keys = dic_all_jobs[job]["l_keys"]
            status = self.get_job_status(l_keys, dict_tree)
            print(status)
