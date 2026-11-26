"""A PostgreSQL source is reached on the port of the configuration file (RML-IO test case
RMLSTC0006a, kg-construct/rml-io, on a server that does not listen on 5432). No server is
needed: psycopg2.connect is replaced by a stub that records its arguments.

    python -m unittest discover -s tests
"""

import os
import tempfile
import unittest
from unittest import mock

import rdfizer
from rdfizer import semantify

MAPPING = """@prefix rml: <http://w3id.org/rml/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> .
@base <http://example.com/rules/> .

<#DB> a rml:Source, d2rq:Database;
  d2rq:jdbcDSN "jdbc:postgresql://127.0.0.1:6543/test"; d2rq:jdbcDriver "org.postgresql.Driver";
  d2rq:username "postgres"; d2rq:password "" .

<#TriplesMap> a rml:TriplesMap;
  rml:logicalSource [ a rml:LogicalSource; rml:source <#DB>;
    rml:referenceFormulation rml:SQL2008Table; rml:iterator "Friends" ];
  rml:subjectMap [ rml:template "http://example.org/{id}" ];
  rml:predicateObjectMap [ rml:predicate foaf:name; rml:objectMap [ rml:reference "name" ] ] .
"""


class Connected(Exception):
    pass


class PostgresPort(unittest.TestCase):
    def test_port_of_the_configuration(self):
        d = tempfile.mkdtemp()
        with open(os.path.join(d, "mapping.ttl"), "w") as f:
            f.write(MAPPING)
        with open(os.path.join(d, "config.ini"), "w") as f:
            f.write("[default]\nmain_directory: %s\n\n[datasets]\nnumber_of_datasets: 1\n"
                    "output_folder: %s\nall_in_one_file: no\nremove_duplicate: yes\nenrichment: yes\n"
                    "name: out\nordered: no\nlarge_file: false\nnew_formulation: yes\ndbType: postgres\n\n"
                    "[dataset1]\nname: out\nmapping: %s\nuser: postgres\npassword: \nhost: 127.0.0.1\n"
                    "port: 6543\ndb: test\n" % (d, os.path.join(d, "out"), os.path.join(d, "mapping.ttl")))
        calls = []

        def connect(**kwargs):
            calls.append(kwargs)
            raise Connected()

        cwd = os.getcwd()
        os.chdir(d)
        try:
            with mock.patch.object(rdfizer.psycopg2, "connect", side_effect=connect):
                try:
                    semantify(os.path.join(d, "config.ini"))
                except (Connected, SystemExit):
                    pass
        finally:
            os.chdir(cwd)
        self.assertTrue(calls)
        self.assertEqual(str(calls[0].get("port")), "6543")


if __name__ == "__main__":
    unittest.main()
