# ==================================================================================================
# --- Imports
# ==================================================================================================

import math
from typing import Any

import numpy as np

# ==================================================================================================
# --- Blocks
# ==================================================================================================


def multiply_function(a: float, b: float) -> float:
    # Multiply a and b
    return a * b


def add_function(a: float, b: float) -> float:
    """Dummy docstring"""
    # Add a and b
    return a + b


def gamma_function(a: float) -> float:
    """Dummy docstring"""
    # Compute gamma function of a
    return math.gamma(a)


def save_npy_function(output: Any, path_output: str) -> None:
    # path_output = f"{output=}".split("=")[0]
    np.save(path_output, output)


# ==================================================================================================
# --- Main
# ==================================================================================================


def main(b: float, c: float, a: float, path_fact_a_bc: str) -> None:
    bc = multiply_function(b, c)
    a_bc = add_function(a, bc)
    fact_a_bc = gamma_function(a_bc)
    save_npy_function(fact_a_bc, path_fact_a_bc)


# ==================================================================================================
# --- Parameters
# ==================================================================================================

# Declare parameters
b = 10
c = 4
a = 2
path_fact_a_bc = "../fact_a_bc.npy"


# ==================================================================================================
# --- Script
# ==================================================================================================

if __name__ == "__main__":
    main(b, c, a, path_fact_a_bc)
