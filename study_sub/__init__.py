# ==================================================================================================
# --- Imports
# ==================================================================================================

# Standard library imports
import importlib.metadata

# Local imports
from .study_sub import StudySub

__all__ = ["StudySub"]

# ==================================================================================================
# --- Package version
# ==================================================================================================
try:
    __version__ = importlib.metadata.version("study-sub")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"
