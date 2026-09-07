#!/usr/bin/env python3
"""Exercise generalized piggybacks at independent positive R and W."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / "_support"))
from generalized_succession import main

if __name__ == "__main__":
    main()
