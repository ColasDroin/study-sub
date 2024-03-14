# ==================================================================================================
# --- Imports
# ==================================================================================================
# Standard library imports
import os
from typing import Self

# Third party imports
from filelock import SoftFileLock

# Local imports
from study_gen._nested_dicts import nested_get

from .config import ConfigJobs
from .dict_yaml_utils import load_yaml, write_yaml
from .generate_run import generate_run_sh, generate_run_sh_htc


# ==================================================================================================
# --- Class
# ==================================================================================================
class StudySub:
    def __init__(
        self: Self,
        path_tree: str,
        path_python_environment: str,
        path_container_image: str | None = None,
    ):
        # Path to study files
        self.path_tree = path_tree

        # Absolute path to the study (get from the path_tree)
        self.abs_path = os.path.abspath(path_tree).split("/tree.yaml")[0]

        # Path to the python environment, activate with `source path_python_environment/bin/activate`
        self.path_python_environment = path_python_environment

        # Container image (Docker or Singularity, if any)
        self.path_container_image = path_container_image

        # Lock file to avoid concurrent access (softlock as several platforms are used)
        self.lock = SoftFileLock(f"{self.path_tree}.lock", timeout=5)

    # dic_tree as a property so that it is reloaded every time it is accessed
    @property
    def dic_tree(self: Self):
        return load_yaml(self.path_tree)

    # Setter for the dic_tree property
    @dic_tree.setter
    def dic_tree(self: Self, value: dict):
        write_yaml(self.path_tree, value)

    # Property for the same reason
    def configure_jobs(self: Self):
        # Lock since we are modifying the tree
        with self.lock:
            dic_tree = ConfigJobs(self.dic_tree).find_and_configure_jobs()

            # Add the python environment, container image and absolute path of the study to the tree
            dic_tree["python_environment"] = self.path_python_environment
            dic_tree["container_image"] = self.path_container_image
            dic_tree["absolute_path"] = self.abs_path

            # Explicitly set the dic_tree property to force rewrite
            self.dic_tree = dic_tree

    def get_all_jobs(self: Self):
        return ConfigJobs(self.dic_tree).find_all_jobs()

    def generate_run_files(self: Self):
        dic_all_jobs = self.get_all_jobs()
        dic_tree = self.dic_tree
        for job in dic_all_jobs:
            l_keys = dic_all_jobs[job]["l_keys"]

            # Get the absolute path to the job

            # Check if htcondor is the configuration
            if "htc" in nested_get(dic_tree, l_keys + ["run_on"]):
                generate_run_sh_htc()
            else:
                generate_run_sh()

    def get_job_status(self: Self, l_keys: list[str], dic_tree=None):
        # Using dic_tree as an argument allows to avoid reloading it
        if dic_tree is None:
            dic_tree = self.dic_tree
        # Return the job status
        return nested_get(dic_tree, l_keys + ["status"])

    def submit(self: Self):
        dic_all_jobs = self.get_all_jobs()
        dic_tree = self.dic_tree
        for job in dic_all_jobs:
            l_keys = dic_all_jobs[job]["l_keys"]
            # Passing the dic_tree as an argument to avoid reloading it
            status = self.get_job_status(l_keys, dic_tree)
            print(status)
