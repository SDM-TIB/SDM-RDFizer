import setuptools
import time

v_time = str(int(time.time()))

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt") as r:
    requirements = list(filter(None, r.read().split("\n")[0:]))

setuptools.setup(
    name="rdfizer",
    version="3.2."+v_time,
    author="Maria-Esther Vidal",
    author_email="maria.vidal@tib.eu",
    license="Apache 2.0",
    description="This project presents the SDM-RDFizer, an interpreter of mapping rules that allows the transformation of (un)structured data into RDF knowledge graphs. The current version of the SDM-RDFizer assumes mapping rules are defined in the RDF Mapping Language (RML) by Dimou et al.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/daniel-dona/test-integration",
    include_package_data=True,
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    install_requires=requirements,
    python_requires='>=3.6',
)
