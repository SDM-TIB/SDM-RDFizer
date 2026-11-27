"""A reference-valued term map with the term type IRI gives its value as is:
R2RML and RML-Core percent-encode only the values inserted in a template
(issue #128, GTFS-Madrid-Bench agency_fare_url).

    python -m unittest discover -s tests
"""

import json
import os
import tempfile
import unittest

from rdfizer import semantify

PREFIXES = """@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix rml: <http://semweb.mmlab.be/ns/rml#> .
@prefix ql: <http://semweb.mmlab.be/ns/ql#> .
@prefix ex: <http://example.com/> .
"""

CSV_MAPPING = PREFIXES + """
<#Agency> a rr:TriplesMap ;
  rml:logicalSource [ rml:source "agency.csv" ; rml:referenceFormulation ql:CSV ] ;
  rr:subjectMap [ rr:template "http://example.com/agency/{agency_id}" ] ;
  rr:predicateObjectMap [
    rr:predicate ex:fareUrl ;
    rr:objectMap [ rml:reference "agency_fare_url" ; rr:termType rr:IRI ]
  ] , [
    rr:predicate ex:name ;
    rr:objectMap [ rr:template "http://example.com/name/{agency_name}" ]
  ] .

<#Page> a rr:TriplesMap ;
  rml:logicalSource [ rml:source "agency.csv" ; rml:referenceFormulation ql:CSV ] ;
  rr:subjectMap [ rml:reference "agency_fare_url" ; rr:termType rr:IRI ] ;
  rr:predicateObjectMap [ rr:predicate ex:id ; rr:objectMap [ rml:reference "agency_id" ] ] .
"""

JSON_MAPPING = PREFIXES + """
<#Agency> a rr:TriplesMap ;
  rml:logicalSource [ rml:source "agency.json" ; rml:referenceFormulation ql:JSONPath ;
                      rml:iterator "$.agencies[*]" ] ;
  rr:subjectMap [ rr:template "http://example.com/agency/{agency_id}" ] ;
  rr:predicateObjectMap [
    rr:predicate ex:fareUrl ;
    rr:objectMap [ rml:reference "agency_fare_url" ; rr:termType rr:IRI ]
  ] .
"""

RELATIVE_MAPPING = PREFIXES + """
<#Student> a rr:TriplesMap ;
  rml:logicalSource [ rml:source "student.csv" ; rml:referenceFormulation ql:CSV ] ;
  rr:subjectMap [ rml:reference "Name" ; rr:termType rr:IRI ] ;
  rr:predicateObjectMap [ rr:predicate ex:id ; rr:objectMap [ rml:reference "ID" ] ] .
"""

URL = "https://www.crtm.es/billetes-y-tarifas"


def run(mapping, files):
    d = tempfile.mkdtemp()
    for name, content in files.items():
        with open(os.path.join(d, name), "w") as f:
            f.write(content)
    with open(os.path.join(d, "mapping.ttl"), "w") as f:
        f.write(mapping)
    out = os.path.join(d, "out")
    config = ("[default]\nmain_directory: %s\n\n[datasets]\nnumber_of_datasets: 1\n"
              "output_folder: %s\nall_in_one_file: no\nremove_duplicate: yes\nenrichment: yes\n"
              "name: out\nordered: no\nlarge_file: false\n\n"
              "[dataset1]\nname: out\nmapping: %s\n"
              % (d, out, os.path.join(d, "mapping.ttl")))
    with open(os.path.join(d, "config.ini"), "w") as f:
        f.write(config)
    cwd = os.getcwd()
    os.chdir(d)
    try:
        semantify(os.path.join(d, "config.ini"))
    finally:
        os.chdir(cwd)
    return open(os.path.join(out, "out.nt")).read()


class ReferenceIRI(unittest.TestCase):
    def setUp(self):
        self.csv = run(CSV_MAPPING, {"agency.csv": "agency_id,agency_name,agency_fare_url\nA1,Metro/Bus,%s\n" % URL})

    def test_csv_object_reference(self):
        self.assertIn("<http://example.com/fareUrl> <%s>" % URL, self.csv)

    def test_csv_subject_reference(self):
        self.assertIn("<%s> <http://example.com/id>" % URL, self.csv)

    def test_template_still_encoded(self):
        self.assertIn("<http://example.com/name/Metro%2FBus>", self.csv)

    def test_json_object_reference(self):
        data = json.dumps({"agencies": [{"agency_id": "A1", "agency_fare_url": URL}]})
        out = run(JSON_MAPPING, {"agency.json": data})
        self.assertIn("<http://example.com/fareUrl> <%s>" % URL, out)

    def test_relative_subject_reference(self):
        # RMLTC0020b (RMLio/rml-test-cases), resolved against the default base IRI
        out = run(RELATIVE_MAPPING, {"student.csv": "ID,Name\n1,Bob/Charles\n"})
        self.assertIn("<http://example.com/base/Bob/Charles> <http://example.com/id>", out)


if __name__ == "__main__":
    unittest.main()
