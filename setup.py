#setup.py
# from cmd digit: python setup.py build
import sys, os
from cx_Freeze import setup, Executable

__version__ = "1.1.0"

#include_files = ['logging.ini', 'config.ini', 'running.png']
#excludes = ["tkinter"]
#packages = ["os", "idna", "requests","json","base64","pyodbc"]
#packages = ["datetime", "time", "jdcal","pyodbc","sys","inspect", "math","os"]

setup(
    name = "appname",
    description='App Description',
    version=__version__,
    options = {"build_exe": {
    #'packages': packages,
    #'include_files': include_files,
    #'excludes': excludes,
    'include_msvcr': True,
}},
#executables = [Executable("hello.py",base="Win32GUI")]
executables = [Executable("TOPmeltMain.py",base="Console")]
#executables = [Executable("TOPmeltMain.py",base="Win32GUI")]
)