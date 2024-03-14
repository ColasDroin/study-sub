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

from .config_utils import ConfigJobs
from .dict_yaml_utils import load_yaml, write_yaml
from .generate_run import generate_run_file


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

        # Name of the study folder
        self.study_name = os.path.dirname(path_tree)

        # Absolute path to the study folder (get from the path_tree)
        self.abs_path = os.path.abspath(self.study_name).split(f"/{self.study_name}")[0]

        # Path to the python environment, activate with `source path_python_environment`
        # Turn to abolute path if it is not already
        if not os.path.isabs(path_python_environment):
            self.path_python_environment = os.path.abspath(path_python_environment)
        else:
            self.path_python_environment = path_python_environment

        # Add /bin/activate to the path_python_environment if needed
        if not self.path_python_environment.endswith("/bin/activate"):
            self.path_python_environment += "/bin/activate"

        # Container image (Docker or Singularity, if any)
        # Turn to abolute path if it is not already
        if path_container_image is None:
            self.path_container_image = None
        elif not os.path.isabs(path_container_image):
            self.path_container_image = os.path.abspath(path_container_image)
        else:
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
            job_name = job.split("/")[-1]
            relative_job_folder = "/".join(job.split("/")[:-1])
            absolute_job_folder = f"{self.abs_path}/{relative_job_folder}"
            generation_number = dic_all_jobs[job]["gen"]
            run_str = generate_run_file(
                absolute_job_folder,
                job_name,
                self.path_python_environment,
                generation_number,
                htc="htc" in nested_get(dic_tree, l_keys + ["submission_type"]),
            )
            # Write the run file
            with open(f"{absolute_job_folder}/run.sh", "w") as f:
                f.write(run_str)

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
