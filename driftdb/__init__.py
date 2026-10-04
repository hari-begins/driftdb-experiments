"""DriftDB: workload-drift-aware query optimization for Spark SQL."""
from .service import DriftDBService
from .config import DriftDBConfig

__all__ = ["DriftDBService", "DriftDBConfig"]
__version__ = "0.1.0"
