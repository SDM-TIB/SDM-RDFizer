"""A join of a logical source with itself on the same reference is replaced by the
parent subject of the same row only when one of the subjects depends on the join
references only; otherwise each row is joined with every row with the same join values
(RMLTC0033a, proposed to kg-construct/rml-core; issue #128, GTFS-Madrid-Bench).

    python -m unittest discover -s tests
"""

import json
import os
import tempfile
import unittest

import rdflib
from rdfizer import semantify

PREFIXES = """@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix rml: <http://semweb.mmlab.be/ns/rml#> .
@prefix ql: <http://semweb.mmlab.be/ns/ql#> .
@prefix ex: <http://example.com/> .
"""

CSV_SOURCE = 'rml:logicalSource [ rml:source "stops.csv" ; rml:referenceFormulation ql:CSV ]'
JSON_SOURCE = ('rml:logicalSource [ rml:source "stops.json" ; rml:referenceFormulation ql:JSONPath ;'
               ' rml:iterator "$.stops[*]" ]')

# RMLTC0033a: neither subject depends on the join reference only
SAME_TRIP = PREFIXES + """
<#Stop> a rr:TriplesMap ;
  %s ;
  rr:subjectMap [ rr:template "http://example.com/stop/{stop}" ] ;
  rr:predicateObjectMap [
    rr:predicate ex:sameTripAs ;
    rr:objectMap [ rr:parentTriplesMap <#Stop> ;
                   rr:joinCondition [ rr:child "trip" ; rr:parent "trip" ] ]
  ] .
"""

# the parent subject depends on the join reference only
ON_TRIP = PREFIXES + """
<#Stop> a rr:TriplesMap ;
  %s ;
  rr:subjectMap [ rr:template "http://example.com/stop/{stop}" ] ;
  rr:predicateObjectMap [
    rr:predicate ex:onTrip ;
    rr:objectMap [ rr:parentTriplesMap <#Trip> ;
                   rr:joinCondition [ rr:child "trip" ; rr:parent "trip" ] ]
  ] .

<#Trip> a rr:TriplesMap ;
  %s ;
  rr:subjectMap [ rr:template "http://example.com/trip/{trip}" ] .
"""

# the child subject depends on the join reference only
HAS_STOP = PREFIXES + """
<#Trip> a rr:TriplesMap ;
  %s ;
  rr:subjectMap [ rr:template "http://example.com/trip/{trip}" ] ;
  rr:predicateObjectMap [
    rr:predicate ex:hasStop ;
    rr:objectMap [ rr:parentTriplesMap <#Stop> ;
                   rr:joinCondition [ rr:child "trip" ; rr:parent "trip" ] ]
  ] .

<#Stop> a rr:TriplesMap ;
  %s ;
  rr:subjectMap [ rr:template "http://example.com/stop/{stop}" ] .
"""

STOPS = [("T1", "S1"), ("T1", "S2"), ("T2", "S3")]
CSV_FILES = {"stops.csv": "trip,stop\n" + "".join("%s,%s\n" % s for s in STOPS)}
EX = rdflib.Namespace("http://example.com/")
JSON_FILES = {"stops.json": json.dumps({"stops": [{"trip": t, "stop": s} for t, s in STOPS]})}


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
    return set(rdflib.Graph().parse(os.path.join(out, "out.nt"), format="nt"))


def triples(predicate, pairs):
    return set((EX[s], EX[predicate], EX[o]) for s, o in pairs)


SAME_TRIP_TRIPLES = triples("sameTripAs", [
    ("stop/S1", "stop/S1"), ("stop/S1", "stop/S2"),
    ("stop/S2", "stop/S1"), ("stop/S2", "stop/S2"),
    ("stop/S3", "stop/S3")])


class SelfJoin(unittest.TestCase):
    def test_csv_same_trip(self):
        # each stop with every stop of its trip, not only with itself
        self.assertEqual(SAME_TRIP_TRIPLES, run(SAME_TRIP % CSV_SOURCE, CSV_FILES))

    def test_csv_on_trip(self):
        self.assertEqual(triples("onTrip", [("stop/S1", "trip/T1"), ("stop/S2", "trip/T1"),
                                            ("stop/S3", "trip/T2")]),
                         run(ON_TRIP % (CSV_SOURCE, CSV_SOURCE), CSV_FILES))

    def test_csv_has_stop(self):
        self.assertEqual(triples("hasStop", [("trip/T1", "stop/S1"), ("trip/T1", "stop/S2"),
                                             ("trip/T2", "stop/S3")]),
                         run(HAS_STOP % (CSV_SOURCE, CSV_SOURCE), CSV_FILES))


if __name__ == "__main__":
    unittest.main()
