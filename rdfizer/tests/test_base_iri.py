"""The base IRI of the relative IRIs of a mapping: the option base_iri of the
configuration file, else http://example.com/base/ (RML-Core test cases
RMLTC0019a, RMLTC0022c and RMLTC0026b, kg-construct/rml-core).

    python -m unittest discover -s tests
"""

import os
import tempfile
import unittest

from rdfizer import semantify

MAPPING = """@prefix rml: <http://w3id.org/rml/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

<http://example.com/base/TriplesMap1> a rml:TriplesMap;
  rml:logicalSource [ a rml:LogicalSource;
      rml:referenceFormulation rml:CSV;
      rml:source [ a rml:RelativePathSource; rml:root rml:MappingDirectory; rml:path "persons.csv" ]
    ];
  rml:subjectMap [ rml:reference "Name"; rml:termType rml:IRI ];
  rml:predicateObjectMap [ rml:predicate foaf:name; rml:objectMap [ rml:reference "Name" ] ] .
"""


def run(base_iri=None):
    d = tempfile.mkdtemp()
    with open(os.path.join(d, "persons.csv"), "w") as f:
        f.write("Name\nCarlos\n")
    with open(os.path.join(d, "mapping.ttl"), "w") as f:
        f.write(MAPPING)
    out = os.path.join(d, "out")
    config = ("[default]\nmain_directory: %s\n\n[datasets]\nnumber_of_datasets: 1\n"
              "output_folder: %s\nall_in_one_file: no\nremove_duplicate: yes\nenrichment: yes\n"
              "name: out\nordered: no\nlarge_file: false\nnew_formulation: yes\n%s\n"
              "[dataset1]\nname: out\nmapping: %s\n"
              % (d, out, "base_iri: %s\n" % base_iri if base_iri else "", os.path.join(d, "mapping.ttl")))
    with open(os.path.join(d, "config.ini"), "w") as f:
        f.write(config)
    cwd = os.getcwd()
    os.chdir(d)
    try:
        semantify(os.path.join(d, "config.ini"))
    finally:
        os.chdir(cwd)
    return open(os.path.join(out, "out.nt")).read()


class BaseIRI(unittest.TestCase):
    def test_base_iri_option(self):
        self.assertIn("<http://example.com/Carlos>", run("http://example.com/"))

    def test_default_base_iri(self):
        self.assertIn("<http://example.com/base/Carlos>", run())


if __name__ == "__main__":
    unittest.main()
