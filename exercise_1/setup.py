import pip

def install(package):
	print("Installing " + package + " through pip")
	from pip._internal import main as pip
	pip(['install', '--user', package])

try: import duckdb
except ImportError:
	install('duckdb')
try: import urllib
except ImportError:
	install('urllib')
try: import sqlite3
except ImportError:
	install('sqlite3')
try: import pandas
except ImportError:
	install('pandas')
try: import bz2
except ImportError:
	install('bz2')
	import bz2
try: import inspect
except ImportError:
	install('inspect')
	import inspect
try: import os
except ImportError:
	install('os')
	import os

SCRIPT_PATH =  os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) # script directory
os.chdir(SCRIPT_PATH)

print("Downloading dataset")
if not os.path.isfile("ncvoter_allc_utf.txt.bz2"):
	urllib.urlretrieve("https://zenodo.org/record/2589451/files/ncvoter_allc_utf.txt.bz2?download=1", "ncvoter_allc_utf.txt.bz2")
if not os.path.isfile("ncvoter_allc_utf.txt.bz2"):
	urllib.urlretrieve("https://zenodo.org/record/2589451/files/precinct_votes.tsv?download=1", "rprecinct_votes.tsv")

print("Decompressing files")
if not os.path.isfile("ncvoter_allc_utf.txt"):
	filepath = "ncvoter_allc_utf.txt.bz2"
	zipfile = bz2.BZ2File(filepath)
	data = zipfile.read() 
	newfilepath = filepath[:-4] 
	open(newfilepath, 'wb').write(data)