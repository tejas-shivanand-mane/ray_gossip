#!/usr/bin/env python3
"""Paired immutable holder recipe experiment; native rebuild required."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / "_support"))
from shared_holder_recipe import main

if __name__ == "__main__":
    main()
