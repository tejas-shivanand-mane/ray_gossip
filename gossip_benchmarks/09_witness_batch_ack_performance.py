#!/usr/bin/env python3
"""Paired opt-in owner witness acknowledgement experiment; see README.md."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / "_support"))
from batch_ack import main

if __name__ == "__main__":
    main()
