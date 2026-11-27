"""Natural datatypes: a JSON integer is typed xsd:integer in a mapping of the W3C KG-Construct
CG (RML-Core); not in the legacy RML vocabulary (RMLTC0002a-JSON of RMLio/rml-test-cases), and
not from an XML source, whose values are strings (RMLSTC0007c of kg-construct/rml-io).

    python -m unittest discover -s tests
"""

import os
import tempfile
import unittest

from rdfizer import semantify

NEW = """@prefix rml: <http://w3id.org/rml/> .
<http://example.com/base/TM> a rml:TriplesMap;
  rml:logicalSource [ a rml:LogicalSource; rml:referenceFormulation rml:%s; rml:iterator "%s";
      rml:source [ a rml:RelativePathSource; rml:root rml:MappingDirectory; rml:path "%s" ] ];
  rml:subjectMap [ rml:template "http://example.com/{%s}" ];
  rml:predicateObjectMap [ rml:predicate <http://example.com/age>; rml:objectMap [ rml:reference "%s" ] ] .
"""

LEGACY = """@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix rml: <http://semweb.mmlab.be/ns/rml#> .
@prefix ql: <http://semweb.mmlab.be/ns/ql#> .
<http://example.com/base/TM> rml:logicalSource [ rml:source "people.json";
      rml:referenceFormulation ql:JSONPath; rml:iterator "$.people[*]" ];
  rr:subjectMap [ rr:template "http://example.com/{id}" ];
  rr:predicateObjectMap [ rr:predicate <http://example.com/age>; rr:objectMap [ rml:reference "age" ] ] .
"""


def run(mapping, new_formulation):
    d = tempfile.mkdtemp()
    with open(os.path.join(d, "people.json"), "w") as f:
        f.write('{"people": [{"id": "a", "age": 33}]}')
    with open(os.path.join(d, "people.xml"), "w") as f:
        f.write('<people><person><id>a</id><age>33</age></person></people>')
    with open(os.path.join(d, "mapping.ttl"), "w") as f:
        f.write(mapping)
    out = os.path.join(d, "out")
    with open(os.path.join(d, "config.ini"), "w") as f:
        f.write("[default]\nmain_directory: %s\n\n[datasets]\nnumber_of_datasets: 1\n"
                "output_folder: %s\nall_in_one_file: no\nremove_duplicate: yes\nenrichment: yes\n"
                "name: out\nordered: no\nlarge_file: false\nnew_formulation: %s\n\n"
                "[dataset1]\nname: out\nmapping: %s\n" % (d, out, new_formulation, os.path.join(d, "mapping.ttl")))
    cwd = os.getcwd()
    os.chdir(d)
    try:
        semantify(os.path.join(d, "config.ini"))
    finally:
        os.chdir(cwd)
    return open(os.path.join(out, "out.nt")).read()


class NaturalDatatypes(unittest.TestCase):
    def test_json_integer_in_rml_core(self):
        self.assertIn('"33"^^<http://www.w3.org/2001/XMLSchema#integer>',
                      run(NEW % ("JSONPath", "$.people[*]", "people.json", "$.id", "$.age"), "yes"))

    def test_json_integer_in_legacy_rml(self):
        output = run(LEGACY, "no")
        self.assertIn('"33"', output)
        self.assertNotIn("XMLSchema#", output)

    def test_xml_value_in_rml_core(self):
        output = run(NEW % ("XPath", "/people/person", "people.xml", "id", "age"), "yes")
        self.assertIn('"33"', output)
        self.assertNotIn("XMLSchema#", output)


if __name__ == "__main__":
    unittest.main()
