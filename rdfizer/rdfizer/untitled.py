import os
import re
import csv
import sys
import uuid
import rdflib
import getopt
import subprocess
from rdflib.plugins.sparql import prepareQuery
from configparser import ConfigParser, ExtendedInterpolation
from rdfizer.triples_map import TriplesMap as tm
import traceback

def semantify(data, mappings):
	triples_map_list = mapping_parser(mappings)