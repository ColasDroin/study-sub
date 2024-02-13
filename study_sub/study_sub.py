# Standard library imports 
from typing import Any, Self

# Third party imports
from filelock import Timeout, FileLock
from ruamel import yaml


class StudySub:
    def __init__(self: Self, path_tree: str, path_config: str):

        # Path to study files
        self.path_tree = path_tree
        self.path_config = path_config

        # Load the corresponding yaml as dicts
        self.dict_tree = self.load_yaml(self.path_tree)

    def load_yaml(self: Self, path_yaml: str) -> dict[str, Any]:
        ryaml = yaml.YAML()
        with open(path_yaml, "r") as f:
            dict_yaml = ryaml.load(f)
        return dict_yaml
    
    @staticmethod
    def ask_and_set_context(self: Self, dict_gen: dict[str, Any]):
        while True:
            try:
                context = input(f"What type of context do you want to use for file {dict_gen['file']}? 1: cpu, 2: cupy, 3: opencl. Default is cpu.")
                context = 1 if context == "" else int(context)
                if context in range(1, 4):
                    break
                else:
                    raise ValueError
            except ValueError:
                print("Invalid input. Please enter a number between 1 and 3.")
        dict_gen['context'] = context

    @staticmethod
    def ask_and_set_run_on(self: Self, dict_gen: dict[str, Any]):
        while True:
            try:
                submission_type = input(f"What type of submission do you want to use for file {dict_gen['file']}? 1: local, 2: htc, 3: htc_docker, 4: slurm, 5: slurm_docker. Default is local.")
                submission_type = 1 if submission_type == "" else int(submission_type)
                if submission_type in range(1, 6):
                    break
                else:
                    raise ValueError
            except ValueError:
                print("Invalid input. Please enter a number between 1 and 5.")
        dict_gen['submission_type'] = submission_type

    def ask_keep_setting(self: Self) -> bool:
        keep_setting = input("Do you want to keep the same setting for identical files? (y/n). Default is y.")
        while keep_setting not in ["", "y", "n"]:
            keep_setting = input("Invalid input. Please enter y, n or skip question.")
        if keep_setting == "":
            keep_setting = "y"
        return keep_setting == "y"
            

    def generate_config_sub(self: Self, path_config_sub: str):
        # Recursively look for file key in the tree, keeping track of the depth
        # of the file in the tree
        set_files_found = set()
        def find_file(dict_gen: dict[str, Any], depth: int = 0):
            for key, value in dict_gen.items():
                if isinstance(value, dict):
                    find_file(value, depth + 1)
                elif key == "file":
                    if value not in set_files_found:
                        print(f"Found file at depth {depth}: {value}")
                        # Set context and run_on
                        self.ask_and_set_context(dict_gen)
                        self.ask_and_set_run_on(dict_gen)
                        if not self.ask_keep_setting():
                            set_files_found.add(value)

        find_file(self.dict_tree)
