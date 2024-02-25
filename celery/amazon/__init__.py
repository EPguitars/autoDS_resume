import os
import sys

script_dir = os.path.dirname(os.path.abspath(__file__))
paths = [
    os.path.abspath(os.path.join(script_dir, "../../")),
    os.path.abspath(os.path.join(script_dir, "."))
    ]

sys.path.extend(paths)