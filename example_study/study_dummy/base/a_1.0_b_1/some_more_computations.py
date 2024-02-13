# ==================================================================================================
# --- Imports
# ==================================================================================================

import numpy as np
from typing import Any
import pickle

# ==================================================================================================
# --- Blocks
# ==================================================================================================


def load_npy_function(path: str) -> Any:
    return np.load(path)


def multiply_function(a: float, b: float) -> float:
    # Multiply a and b
    return a * b


def add_function(a: float, b: float) -> float:
    """Dummy docstring"""
    # Add a and b
    return a + b


def save_pkl_function(output: Any, path_output: str) -> None:
    # Get output name
    # output_str = f"{output=}".split("=")[0]
    with open(path_output, "wb") as f:
        pickle.dump(output, f)


def power_function(b: float, c: float) -> float:
    # Returns a at the power of b
    return np.power(b, c)


def add_power_function(y: float, z: float, x: float) -> float:
    """This is a merge test."""

    x_y = power_function(x, y)
    x_y = power_function(x_y, x_y)
    x_y_z = add_function(x_y, z)
    return x_y_z


def add_power_multiply_function(x: float, y: float, z: float, w: float) -> float:

    x_y_z = add_power_function(x, y, z)
    x_y_z_w = multiply_function(x_y_z, w)
    return x_y_z_w


# ==================================================================================================
# --- Main
# ==================================================================================================


def main(
    path_fact_a_bc: str, b: float, c: float, a: float, d: float, path_result: str
) -> None:

    fact_a_bc = load_npy_function(path_fact_a_bc)
    bc_c = add_power_function(b, c, c)
    a_bc_c = multiply_function(a, bc_c)
    c_c_d = add_power_function(c, c, d)
    a_bc_c_c_d = add_function(a_bc_c, c_c_d)
    a_bc_c_c_d_new = add_power_multiply_function(a, b, c, a_bc_c_c_d)
    result = multiply_function(fact_a_bc, a_bc_c_c_d_new)
    save_pkl_function(result, path_result)


# ==================================================================================================
# --- Parameters
# ==================================================================================================

# Declare parameters
path_fact_a_bc = "../fact_a_bc.npy"
b = 1
c = 4
a = 1.0
d = 0.5
path_result = "result.pkl"


# ==================================================================================================
# --- Script
# ==================================================================================================

if __name__ == "__main__":
    main(path_fact_a_bc, b, c, a, d, path_result)
