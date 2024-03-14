# ==================================================================================================
# --- Imports
# ==================================================================================================
# Standard library imports
import os
from typing import Self

# Third party imports
from rich import print
from study_gen._nested_dicts import nested_get

# Local imports


# ==================================================================================================
# --- Class
# ==================================================================================================
class DependencyGraph:
    def __init__(
        self: Self,
        dic_tree: dict,
        dic_all_jobs: dict,
    ):
        self.dic_tree = dic_tree
        self.dic_all_jobs = dic_all_jobs

    def build_dependency_graph(self):
        self.dependency_graph = {}
        self.set_l_keys = {
            tuple(self.dic_all_jobs[job]["l_keys"][:-1]) for job in self.dic_all_jobs
        }
        for job in self.dic_all_jobs:
            l_keys = self.dic_all_jobs[job]["l_keys"]
            self.dependency_graph[job] = set()
            # Add all parents to the dependency graph
            for i in range(len(l_keys) - 1):
                l_keys_parent = l_keys[:i]
                if tuple(l_keys_parent) in self.set_l_keys:
                    parent = nested_get(self.dic_tree, l_keys_parent)
                    # Look for all the jobs in the parent (but not the generations below)
                    for name_parent, sub_dict in parent.items():
                        if "file" in sub_dict:
                            self.dependency_graph[job].add(sub_dict["file"])

        print(self.dependency_graph)
