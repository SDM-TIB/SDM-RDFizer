"""A language map with a reference gives the language tag of the literal: @en, not @"en"
(RMLTC0031b, kg-construct/rml-core).

    python -m unittest discover -s tests
"""

import os
import tempfile
import unittest

from rdfizer import semantify

MAPPING = """@prefix rml: <http://w3id.org/rml/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
<http://example.com/base/TM> a rml:TriplesMap;
  rml:logicalSource [ a rml:LogicalSource; rml:referenceFormulation rml:JSONPath; rml:iterator "$[*]";
      rml:source [ a rml:RelativePathSource; rml:root rml:MappingDirectory; rml:path "labels.json" ] ];
  rml:subjectMap [ rml:template "http://example.com/{$.ID}" ];
  rml:predicateObjectMap [ rml:predicate rdfs:label;
      rml:objectMap [ rml:reference "$.label"; rml:languageMap [ rml:reference "$.language" ] ] ] .
"""


class LanguageMap(unittest.TestCase):
    def test_reference_language_map(self):
        d = tempfile.mkdtemp()
        with open(os.path.join(d, "labels.json"), "w") as f:
            f.write('[{"ID": 10, "label": "apple", "language": "en"}]')
        with open(os.path.join(d, "mapping.ttl"), "w") as f:
            f.write(MAPPING)
        out = os.path.join(d, "out")
        with open(os.path.join(d, "config.ini"), "w") as f:
            f.write("[default]\nmain_directory: %s\n\n[datasets]\nnumber_of_datasets: 1\n"
                    "output_folder: %s\nall_in_one_file: no\nremove_duplicate: yes\nenrichment: yes\n"
                    "name: out\nordered: no\nlarge_file: false\nnew_formulation: yes\n\n"
                    "[dataset1]\nname: out\nmapping: %s\n" % (d, out, os.path.join(d, "mapping.ttl")))
        cwd = os.getcwd()
        os.chdir(d)
        try:
            semantify(os.path.join(d, "config.ini"))
        finally:
            os.chdir(cwd)
        self.assertIn('"apple"@en', open(os.path.join(out, "out.nt")).read())


if __name__ == "__main__":
    unittest.main()
