# GPI Survey QC Tool

This repository contains the Tkinter-based desktop application defined in
[`gpi_qc_tool.py`](gpi_qc_tool.py). The script expects to be executed with a
regular Python 3 interpreter; it is **not** packaged as an executable and it
does not vendor any of its third-party libraries. When you run the script it
imports the libraries directly from the interpreter's site-packages directory.
If those packages are missing on a machine, Python will raise `ModuleNotFoundError`
exceptions and the program will fail to start.

## Runtime requirements

The following dependencies must be installed in the Python environment before
launching the tool:

* Core: `pandas`, `numpy`
* Excel/HTML export helpers: `openpyxl`, `xlsxwriter`
* Optional drag-and-drop support: `tkinterdnd2`

The GUI itself relies on the `tkinter` module that ships with many Python
installations. Some minimal Python distributions (for example, the Microsoft
Store build on Windows) omit Tk, so confirm that Tkinter is available or install
an interpreter build that bundles it.

You can install the Python packages with pip:

```bash
python -m pip install --upgrade pandas numpy openpyxl xlsxwriter tkinterdnd2
```

## Running the tool

1. Ensure you are using a Python 3 environment that has the dependencies above
   available.
2. From the repository directory run:

   ```bash
   python gpi_qc_tool.py
   ```

3. The GUI window should appear. If you receive import errors on another
   machine, install the missing dependency in that environment and retry.

Because this repository only contains the source file, each machine that runs
it must have the required dependencies installed separately. Creating a frozen
executable (with tools such as PyInstaller) is outside the scope of the current
project.
