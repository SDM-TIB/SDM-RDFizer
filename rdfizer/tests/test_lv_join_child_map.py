"""Logical view joins with a template or constant childMap must look up the
evaluated join value in the join table instead of indexing the row by the
child_condition dict.

    python -m unittest discover -s tests
"""

import os
import tempfile
import unittest

from rdfizer import semantify


CSV_TEMPLATE_MAPPING = """@prefix rml: <http://w3id.org/rml/> .
@prefix ex: <http://example.org/> .

<http://example.org/CsvParentView> a rml:LogicalView ;
    rml:viewOn [
        a rml:LogicalSource ;
        rml:source [
            a rml:RelativePathSource ;
            rml:root rml:MappingDirectory ;
            rml:path "parents.csv" ;
        ] ;
        rml:referenceFormulation rml:CSV ;
    ] ;
    rml:field [
        a rml:ExpressionField ;
        rml:fieldName "pid" ;
        rml:reference "id" ;
    ] ;
    rml:field [
        a rml:ExpressionField ;
        rml:fieldName "label" ;
        rml:reference "label" ;
    ] .

<http://example.org/CsvChildView> a rml:LogicalView ;
    rml:viewOn [
        a rml:LogicalSource ;
        rml:source [
            a rml:RelativePathSource ;
            rml:root rml:MappingDirectory ;
            rml:path "children.csv" ;
        ] ;
        rml:referenceFormulation rml:CSV ;
    ] ;
    rml:field [
        a rml:ExpressionField ;
        rml:fieldName "cid" ;
        rml:reference "id" ;
    ] ;
    rml:innerJoin [
        a rml:LogicalViewJoin ;
        rml:parentLogicalView <http://example.org/CsvParentView> ;
        rml:joinCondition [
            rml:parentMap [ rml:reference "pid" ] ;
            rml:childMap [ rml:template "P-{id}" ] ;
        ] ;
        rml:field [
            a rml:ExpressionField ;
            rml:fieldName "plabel" ;
            rml:reference "label" ;
        ] ;
    ] .

<http://example.org/CsvTM> a rml:TriplesMap ;
    rml:logicalSource <http://example.org/CsvChildView> ;
    rml:subjectMap [ rml:template "http://example.org/item/{cid}" ] ;
    rml:predicateObjectMap [
        rml:predicate ex:label ;
        rml:objectMap [ rml:reference "plabel" ] ;
    ] .
"""

JSON_TEMPLATE_MAPPING = """@prefix rml: <http://w3id.org/rml/> .
@prefix ex: <http://example.org/> .

<http://example.org/JsonParentView> a rml:LogicalView ;
    rml:viewOn [
        a rml:LogicalSource ;
        rml:source [
            a rml:RelativePathSource ;
            rml:root rml:MappingDirectory ;
            rml:path "parents.json" ;
        ] ;
        rml:referenceFormulation rml:JSONPath ;
        rml:iterator "$[*]" ;
    ] ;
    rml:field [
        a rml:ExpressionField ;
        rml:fieldName "pid" ;
        rml:reference "$.id" ;
    ] ;
    rml:field [
        a rml:ExpressionField ;
        rml:fieldName "label" ;
        rml:reference "$.label" ;
    ] .

<http://example.org/JsonChildView> a rml:LogicalView ;
    rml:viewOn [
        a rml:LogicalSource ;
        rml:source [
            a rml:RelativePathSource ;
            rml:root rml:MappingDirectory ;
            rml:path "children.json" ;
        ] ;
        rml:referenceFormulation rml:JSONPath ;
        rml:iterator "$[*]" ;
    ] ;
    rml:field [
        a rml:ExpressionField ;
        rml:fieldName "cid" ;
        rml:reference "$.id" ;
    ] ;
    rml:innerJoin [
        a rml:LogicalViewJoin ;
        rml:parentLogicalView <http://example.org/JsonParentView> ;
        rml:joinCondition [
            rml:parentMap [ rml:reference "pid" ] ;
            rml:childMap [ rml:template "P-{id}" ] ;
        ] ;
        rml:field [
            a rml:ExpressionField ;
            rml:fieldName "plabel" ;
            rml:reference "label" ;
        ] ;
    ] .

<http://example.org/JsonTM> a rml:TriplesMap ;
    rml:logicalSource <http://example.org/JsonChildView> ;
    rml:subjectMap [ rml:template "http://example.org/item/{cid}" ] ;
    rml:predicateObjectMap [
        rml:predicate ex:label ;
        rml:objectMap [ rml:reference "plabel" ] ;
    ] .
"""

CSV_CONSTANT_MAPPING = """@prefix rml: <http://w3id.org/rml/> .
@prefix ex: <http://example.org/> .

<http://example.org/ConstParentView> a rml:LogicalView ;
    rml:viewOn [
        a rml:LogicalSource ;
        rml:source [
            a rml:RelativePathSource ;
            rml:root rml:MappingDirectory ;
            rml:path "parents.csv" ;
        ] ;
        rml:referenceFormulation rml:CSV ;
    ] ;
    rml:field [
        a rml:ExpressionField ;
        rml:fieldName "pid" ;
        rml:reference "id" ;
    ] ;
    rml:field [
        a rml:ExpressionField ;
        rml:fieldName "label" ;
        rml:reference "label" ;
    ] .

<http://example.org/ConstChildView> a rml:LogicalView ;
    rml:viewOn [
        a rml:LogicalSource ;
        rml:source [
            a rml:RelativePathSource ;
            rml:root rml:MappingDirectory ;
            rml:path "children.csv" ;
        ] ;
        rml:referenceFormulation rml:CSV ;
    ] ;
    rml:field [
        a rml:ExpressionField ;
        rml:fieldName "cid" ;
        rml:reference "id" ;
    ] ;
    rml:leftJoin [
        a rml:LogicalViewJoin ;
        rml:parentLogicalView <http://example.org/ConstParentView> ;
        rml:joinCondition [
            rml:parentMap [ rml:reference "pid" ] ;
            rml:childMap [ rml:constant "K1" ] ;
        ] ;
        rml:field [
            a rml:ExpressionField ;
            rml:fieldName "plabel" ;
            rml:reference "label" ;
        ] ;
    ] .

<http://example.org/ConstTM> a rml:TriplesMap ;
    rml:logicalSource <http://example.org/ConstChildView> ;
    rml:subjectMap [ rml:template "http://example.org/item/{cid}" ] ;
    rml:predicateObjectMap [
        rml:predicate ex:label ;
        rml:objectMap [ rml:reference "plabel" ] ;
    ] .
"""


def _run_mapping(files, mapping_ttl):
    d = tempfile.mkdtemp()
    for name, content in files.items():
        with open(os.path.join(d, name), "w") as f:
            f.write(content)
    with open(os.path.join(d, "mapping.ttl"), "w") as f:
        f.write(mapping_ttl)
    out = os.path.join(d, "out")
    config = (
        "[default]\nmain_directory: %s\n\n[datasets]\nnumber_of_datasets: 1\n"
        "output_folder: %s\nall_in_one_file: no\nremove_duplicate: yes\nenrichment: yes\n"
        "name: out\nordered: no\nlarge_file: false\nnew_formulation: yes\n\n"
        "[dataset1]\nname: out\nmapping: %s\n"
        % (d, out, os.path.join(d, "mapping.ttl"))
    )
    with open(os.path.join(d, "config.ini"), "w") as f:
        f.write(config)
    cwd = os.getcwd()
    os.chdir(d)
    try:
        semantify(os.path.join(d, "config.ini"))
    finally:
        os.chdir(cwd)
    return open(os.path.join(out, "out.nt")).read()


class LogicalViewJoinChildMap(unittest.TestCase):
    def test_csv_inner_join_template_child_map(self):
        output = _run_mapping(
            {
                "parents.csv": "id,label\nP-1,Alice\nP-2,Bob\n",
                "children.csv": "id,extra\n1,x\n2,y\n3,z\n",
            },
            CSV_TEMPLATE_MAPPING,
        )
        self.assertIn('<http://example.org/item/1> <http://example.org/label> "Alice".', output)
        self.assertIn('<http://example.org/item/2> <http://example.org/label> "Bob".', output)
        self.assertNotIn("<http://example.org/item/3>", output)

    def test_csv_single_column_template_child_map(self):
        output = _run_mapping(
            {
                "parents.csv": "id,label\nP-1,Alice\n",
                "children.csv": "id\n1\n",
            },
            CSV_TEMPLATE_MAPPING,
        )
        self.assertIn('<http://example.org/item/1> <http://example.org/label> "Alice".', output)

    def test_json_inner_join_template_child_map(self):
        output = _run_mapping(
            {
                "parents.json": '[{"id": "P-1", "label": "Alice"}, {"id": "P-2", "label": "Bob"}]',
                "children.json": '[{"id": "1"}, {"id": "2"}, {"id": "3"}]',
            },
            JSON_TEMPLATE_MAPPING,
        )
        self.assertIn('<http://example.org/item/1> <http://example.org/label> "Alice".', output)
        self.assertIn('<http://example.org/item/2> <http://example.org/label> "Bob".', output)
        self.assertNotIn("<http://example.org/item/3>", output)

    def test_csv_left_join_constant_child_map(self):
        output = _run_mapping(
            {
                "parents.csv": "id,label\nK1,Shared\n",
                "children.csv": "id,extra\n10,x\n",
            },
            CSV_CONSTANT_MAPPING,
        )
        self.assertIn('<http://example.org/item/10> <http://example.org/label> "Shared".', output)


if __name__ == "__main__":
    unittest.main()
