# Standard library imports
from typing import Any, Self

# Third party imports
from filelock import FileLock, Timeout
from ruamel import yaml


class StudySub:
    def __init__(self: Self, path_tree: str):
        # Path to study files
        self.path_tree = path_tree

        # Load the corresponding yaml as dicts
        self.dict_tree = self.load_yaml(self.path_tree)

    def load_yaml(self: Self, path_yaml: str) -> dict[str, Any]:
        ryaml = yaml.YAML()
        with open(path_yaml, "r") as f:
            dict_yaml = ryaml.load(f)
        return dict_yaml

    @staticmethod
    def ask_and_set_context(dict_gen: dict[str, Any]):
        while True:
            try:
                context = input(
                    f"What type of context do you want to use for file {dict_gen['file']}?"
                    " 1: cpu, 2: cupy, 3: opencl. Default is cpu."
                )
                context = 1 if context == "" else int(context)
                if context in range(1, 4):
                    break
                else:
                    raise ValueError
            except ValueError:
                print("Invalid input. Please enter a number between 1 and 3.")

        dict_context = {1: "cpu", 2: "cupy", 3: "opencl"}
        dict_gen["context"] = dict_context[context]

    @staticmethod
    def ask_and_set_run_on(dict_gen: dict[str, Any]):
        while True:
            try:
                submission_type = input(
                    f"What type of submission do you want to use for file {dict_gen['file']}?"
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
        dict_gen["submission_type"] = dict_submission_type[submission_type]

    @staticmethod
    def ask_keep_setting() -> bool:
        keep_setting = input(
            "Do you want to keep the same setting for identical files? (y/n). Default is y."
        )
        while keep_setting not in ["", "y", "n"]:
            keep_setting = input("Invalid input. Please enter y, n or skip question.")
        if keep_setting == "":
            keep_setting = "y"
        return keep_setting == "y"

    @staticmethod
    def ask_skip_configured_files() -> bool:
        skip_configured_files = input(
            "Some files to submit seem to be configured already. Do you want to skip them? (y/n). Default is y."
        )
        while skip_configured_files not in ["", "y", "n"]:
            skip_configured_files = input("Invalid input. Please enter y, n or skip question.")
        if skip_configured_files == "":
            skip_configured_files = "y"
        return skip_configured_files == "y"

    def generate_config_sub(self: Self):
        # Recursively look for file key in the tree, keeping track of the depth
        # of the file in the tree
        self.dic_config_files_found = {}
        self.skip_configured_files = None

        def find_file(dict_gen: dict[str, Any], depth: int = 0):
            for key, value in dict_gen.items():
                if isinstance(value, dict):
                    find_file(value, depth + 1)
                elif key == "file":
                    # Ensure configuration is not already set
                    if "submission_type" in dict_gen:
                        if self.skip_configured_files is None:
                            self.skip_configured_files = self.ask_skip_configured_files()
                        if self.skip_configured_files:
                            return

                    # If it's the first time we find the file, ask for context and run_on
                    if value.split("/")[-1] not in self.dic_config_files_found:
                        print(f"Found file at depth {depth}: {value}")
                        # Set context and run_on
                        self.ask_and_set_context(dict_gen)
                        self.ask_and_set_run_on(dict_gen)
                        if self.ask_keep_setting():
                            self.dic_config_files_found[value.split("/")[-1]] = {
                                "context": dict_gen["context"],
                                "submission_type": dict_gen["submission_type"],
                            }

                    # If the file is already in the dict, set the previous context and run_on
                    else:
                        dict_gen["context"] = self.dic_config_files_found[value.split("/")[-1]][
                            "context"
                        ]
                        dict_gen["submission_type"] = self.dic_config_files_found[
                            value.split("/")[-1]
                        ]["submission_type"]

        find_file(self.dict_tree)

        # Write the new config file
        ryaml = yaml.YAML()
        with open(self.path_tree, "w") as f:
            ryaml.dump(self.dict_tree, f)
