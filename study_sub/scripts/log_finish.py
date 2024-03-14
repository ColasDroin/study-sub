# ==================================================================================================
# --- Imports
# ==================================================================================================
# Standard library imports
import sys

# Third party imports
from filelock import SoftFileLock
from study_gen._nested_dicts import nested_set

# Local imports
from ..utils.dict_yaml_utils import load_yaml, write_yaml

# ==================================================================================================
# --- Script
# ==================================================================================================

# Get path of the tree and keys to tag
tree_path = sys.argv[1]
l_keys = sys.argv[2:]

# Define lock
lock = SoftFileLock(f"{tree_path}.lock", timeout=5)

# Update tag
with lock:
    # Load the tree
    tree = load_yaml(tree_path)

    # Update the tree
    nested_set(tree, l_keys + ["status"], "finished")

    # Write the tree
    write_yaml(tree_path, tree)
