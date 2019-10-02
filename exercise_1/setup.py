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
try: import numpy
except ImportError:
	install('numpy')
try: import sklearn
except ImportError:
	install('sklearn')
try: import zipfile
except ImportError:
	install('zipfile')
	import zipfile
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
if not os.path.isfile("ncvoter_sample.tsv.zip"):
	urllib.request.urlretrieve("https://zenodo.org/record/3466870/files/ncvoter_sample.tsv.zip?download=1", "ncvoter_sample.tsv.zip")
if not os.path.isfile("rprecinctvotes.tsv.zip"):
	urllib.request.urlretrieve("https://zenodo.org/record/3466870/files/precinct_votes.tsv?download=1", "precinctvotes.tsv")
if not os.path.isfile("voters_sqlite.db.zip"):
	urllib.request.urlretrieve("https://zenodo.org/record/3466870/files/voters_sqlite.db.zip?download=1", "voters_sqlite.db.zip")

print("Decompressing files")
if not os.path.isfile("ncvoter_sample.tsv"):
	with zipfile.ZipFile("ncvoter_sample.tsv.zip","r") as zip_ref:
		zip_ref.extractall(SCRIPT_PATH)

if not os.path.isfile("voters_sqlite.db"):
	with zipfile.ZipFile("voters_sqlite.db.zip","r") as zip_ref:
		zip_ref.extractall(SCRIPT_PATH)