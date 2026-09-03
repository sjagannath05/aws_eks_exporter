import importlib.util
import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


@pytest.fixture(scope="session")
def exporter_module():
    """Import eks-config-exporter.py (hyphenated filename) as a module."""
    spec = importlib.util.spec_from_file_location("eks_config_exporter", os.path.join(ROOT, "eks-config-exporter.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod
