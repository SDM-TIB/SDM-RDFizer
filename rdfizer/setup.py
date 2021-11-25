import setuptools
import argparse
import time
import sys

v_time = str(int(time.time()))

parser = argparse.ArgumentParser()
parser.add_argument('-k', help="Release type", dest="kind")
#parser.add_argument('-t', help="Version tag", dest="tag")
parsed, rest = parser.parse_known_args()
sys.argv = [sys.argv[0]] + rest


with open("README.md", "r") as fh:
    long_description = fh.read()

with open("VERSION", "r") as fh:
    v = fh.read().replace("\n", "")
    if parsed.kind == "rel":
        vers_taged = v
    else:
        vers_taged = v+".dev"+v_time


with open("requirements.txt") as r:
    requirements = list(filter(None, r.read().split("\n")[0:]))

setuptools.setup(
    name="rdfizer",
    version=vers_taged,
    author="Maria-Esther Vidal",
    author_email="maria.vidal@tib.eu",
    license="Apache 2.0",
    description="This project presents the SDM-RDFizer, an interpreter of mapping rules that allows the transformation of (un)structured data into RDF knowledge graphs. The current version of the SDM-RDFizer assumes mapping rules are defined in the RDF Mapping Language (RML) by Dimou et al.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/SDM-TIB/SDM-RDFizer",
    include_package_data=True,
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "Topic :: Utilities",
        "Topic :: Software Development :: Pre-processors",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator"
    ],
    install_requires=requirements,
    python_requires='>=3.6',
    entry_points={
        'console_scripts': [
            'rdfizer=rdfizer.__main__:main',
        ],
    },
)
