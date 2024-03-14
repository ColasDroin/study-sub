# ==================================================================================================
# --- Imports
# ==================================================================================================
# Third party imports
from ruamel import yaml


# ==================================================================================================
# --- Functions
# ==================================================================================================
def write_yaml(path, dictionary):
    ryaml = yaml.YAML()
    with open(path, "w") as f:
        ryaml.dump(dictionary, f)


def load_yaml(path):
    ryaml = yaml.YAML()
    with open(path, "r") as f:
        dictionary = ryaml.load(f)
    return dictionary
