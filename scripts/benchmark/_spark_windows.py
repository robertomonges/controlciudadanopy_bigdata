import os
import sys

_HADOOP_HOME = "C:/hadoop"

if sys.platform == "win32":
    os.environ.setdefault("HADOOP_HOME", _HADOOP_HOME)
    hadoop_bin = os.path.join(_HADOOP_HOME, "bin").replace("/", "\\")
    path = os.environ.get("PATH", "")
    if hadoop_bin not in path:
        os.environ["PATH"] = hadoop_bin + ";" + path
