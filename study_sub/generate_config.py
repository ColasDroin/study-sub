# Standard library imports
import copy
from typing import Any, Self

# Local imports
from .dict_yaml_utils import load_yaml, write_yaml


def ask_and_set_context(dic_gen: dict[str, Any]):
    while True:
        try:
            context = input(
                f"What type of context do you want to use for job {dic_gen['file']}?"
                " 1: cpu, 2: cupy, 3: opencl. Default is cpu."
            )
            context = 1 if context == "" else int(context)
            if context in range(1, 4):
                break
            else:
                raise ValueError
        except ValueError:
            print("Invalid input. Please enter a number between 1 and 3.")

    dict_context = {
        1: "cpu",
        2: "cupy",
        3: "opencl",
    }
    dic_gen["context"] = dict_context[context]


def ask_and_set_run_on(dic_gen: dict[str, Any]):
    while True:
        try:
            submission_type = input(
                f"What type of submission do you want to use for job {dic_gen['file']}?"
                " 1: local, 2: htc, 3: htc_docker, 4: slurm, 5: slurm_docker. Default is local."
            )
            submission_type = 1 if submission_type == "" else int(submission_type)
            if submission_type in range(1, 6):
                break
            else:
                raise ValueError
        except ValueError:
            print("Invalid input. Please enter a number between 1 and 5.")

    dict_submission_type = {
        1: "local",
        2: "htc",
        3: "htc_docker",
        4: "slurm",
        5: "slurm_docker",
    }
    dic_gen["submission_type"] = dict_submission_type[submission_type]


def ask_keep_setting() -> bool:
    keep_setting = input(
        "Do you want to keep the same setting for identical jobs? (y/n). Default is y."
    )
    while keep_setting not in ["", "y", "n"]:
        keep_setting = input("Invalid input. Please enter y, n or skip question.")
    if keep_setting == "":
        keep_setting = "y"
    return keep_setting == "y"


def ask_skip_configured_jobs() -> bool:
    skip_configured_jobs = input(
        "Some jobs to submit seem to be configured already. Do you want to skip them? (y/n). "
        "Default is y."
    )
    while skip_configured_jobs not in ["", "y", "n"]:
        skip_configured_jobs = input("Invalid input. Please enter y, n or skip question.")
    if skip_configured_jobs == "":
        skip_configured_jobs = "y"
    return skip_configured_jobs == "y"


class StudyConfig:
    def __init__(self, dict_tree: dict):
        # Load the corresponding yaml as dicts
        self.dict_tree = dict_tree

    def _find_and_configure_jobs_recursion(
        self: Self, dic_gen: dict[str, Any], depth: int = 0, l_keys: list[str] | None = None
    ):
        if l_keys is None:
            l_keys = []

        # Recursively look for job key in the tree, keeping track of the depth
        # of the job in the tree
        for key, value in dic_gen.items():
            if isinstance(value, dict):
                self._find_and_configure_jobs_recursion(value, depth + 1, l_keys + [key])
            elif key == "file":
                # Add job the the list of all jobs
                # In theory, the list of keys can be obtained from the job path
                # but it's safer to keep it in the dict
                self.dic_all_jobs[value] = {
                    "gen": depth,
                    "l_keys": copy.copy(l_keys),
                }
                job_name = value.split("/")[-1]

                # Ensure configuration is not already set
                if "submission_type" in dic_gen:
                    if self.skip_configured_jobs is None:
                        self.skip_configured_jobs = ask_skip_configured_jobs()
                    if self.skip_configured_jobs:
                        return

                # If it's the first time we find the job, ask for context and run_on
                if job_name not in self.dic_config_jobs:
                    print(f"Found job at depth {depth}: {value}")
                    # Set context and run_on
                    ask_and_set_context(dic_gen)
                    ask_and_set_run_on(dic_gen)
                    dic_gen["status"] = status = "to_submit"
                    if ask_keep_setting():
                        self.dic_config_jobs[job_name] = {
                            "context": dic_gen["context"],
                            "submission_type": dic_gen["submission_type"],
                            "status": status,
                        }

                # If the job is already in the dict, set the previous context and run_on
                else:
                    dic_gen["context"] = self.dic_config_jobs[job_name]["context"]
                    dic_gen["submission_type"] = self.dic_config_jobs[job_name]["submission_type"]
                    dic_gen["status"] = self.dic_config_jobs[job_name]["status"]

    def find_and_configure_jobs(self: Self, dic_tree: dict[str, Any]):
        # Variables to store the jobs and their configuration
        self.dic_config_jobs = {}
        self.dic_all_jobs = {}
        self.skip_configured_jobs = None

        self._find_and_configure_jobs_recursion(dic_tree, depth=-1)

        return self.dic_all_jobs, self.dic_config_jobs
