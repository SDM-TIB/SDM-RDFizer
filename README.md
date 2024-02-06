# SDM-RDFizer
[![License](https://img.shields.io/pypi/l/rdfizer.svg)](https://github.com/SDM-TIB/SDM-RDFizer/blob/master/LICENSE)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.6225573.svg)](https://doi.org/10.5281/zenodo.6225573)
[![Latest PyPI version](https://img.shields.io/pypi/v/rdfizer?style=flat)](https://pypi.org/project/rdfizer/)
[![Python Version](https://img.shields.io/pypi/pyversions/rdfizer.svg)](https://pypi.org/project/rdfizer/)
[![PyPI status](https://img.shields.io:/pypi/status/rdfizer?)](https://pypi.org/project/rdfizer/)

This project presents the SDM-RDFizer, an interpreter of mapping rules that allows the transformation of (un)structured data into RDF knowledge graphs. The current version of the SDM-RDFizer assumes mapping rules are defined in the [RDF Mapping Language (RML) by Dimou et al](https://rml.io/specs/rml/). The SDM-RDFizer implements optimized data structures and relational algebra operators that enable an efficient execution of RML triple maps even in the presence of Big data. SDM-RDFizer is able to process data from heterogeneous data sources (CSV, JSON, RDB, XML) processing each set of RML rules (TriplesMap) in a multi-thread safe procedure.

![SDM-RDFizer workflow](https://raw.githubusercontent.com/SDM-TIB/SDM-RDFizer/beta/images/architecture.png "SDM-RDFizer workflow")

# The new features presented by SDM-RDFizer version4.0

In version 4.0 of SDM-RDFizer, we have addressed the problem of efficiency in KG creation in terms of memory storage. SDM-RDFizer version4.0 includes a new module called "TriplesMap Planning" a.k.a. TMP which defines an optimized evaluation plan for the execution of triples maps. Additionally, version 4.0 extends the previously included module (i.e. TriplesMap Execution a.k.a. TME) by introducing a new operator for compressing data stored in the data structures. These new features can be configured using two new parameters added to the configuration file, named "large_file" and "ordered". 

We have performed extensive empirical evaluation on SDM-RDFizer version4.0 in terms of execution time and memory usage. The experiments are set up to empirically compare the impact of data duplicate rates, data size, and the complexity and the execution order of the triples maps on two versions of SDM-RDFizer (i.e. version4.0 and version3.6) and other existing engines including [RMLMapper v4.7](https://github.com/RMLio/rmlmapper-java) and [RocketRML](https://github.com/semantifyit/RocketRML) ), in terms of execution time and memory usage. The experiments are performed on two different benchmarks: 
- From [SDM-Genomic-datasets](https://figshare.com/articles/dataset/SDM-Genomic-Datasets/14838342/1), datasets including 10k, 100k, and 1M records with 25% and 75% duplicates rates, over six mapping rules with different complexities (1/4 simple object map, 2/5 object reference maps, 2/5 object join maps)
- From [GTFS-Madrid](https://github.com/oeg-upm/gtfs-bench), datasets with scale values of 1-csv, 5-csv, 10-csv, and 50-csv, over two different mapping rules (72 simple object maps and 11 object join maps). 

The results of explained experiments can be summarized as the following:
![Overview of Results (Execution Time Comparison)](https://raw.githubusercontent.com/SDM-TIB/SDM-RDFizer/beta/images/time.png "Execution Time Comparison")
As observed in the figures above, both versions of SDM-RDFizer completed all the testbeds successfully while the other two engines have cases of timeout. SDM-RDFizer version3.6 and RocketRML version 1.7.0 are competitive in simple testbeds, however, SDM-RDFizer version4.0 shows the best performance in all the testbeds. 
![Overview of Results (Memory Consumption Comparison)](https://raw.githubusercontent.com/SDM-TIB/SDM-RDFizer/beta/images/memory.png "Memory Consumption Comparison")
As illustrated in the figures above, SDM-RDFizer version4.0 has the smallest peak in memory usage compared to the previous version of SDM-RDFizer.  


The results of the execution of SDM-RDFizer has been described in the following research reports:

- Enrique Iglesias, Samaneh Jozashoori, David Chaves-Fraga, Diego Collarana, and Maria-Esther Vidal. 2020. SDM-RDFizer: An RML Interpreter for the Efficient Creation of RDF Knowledge Graphs. The 29th ACM International Conference on Information and Knowledge Management (CIKM ’20).

- Samaneh Jozashoori, David Chaves-Fraga, Enrique Iglesias, Oscar Corcho, and Maria-Esther Vidal. 2020. FunMap: Efficient Execution of Functional Mappings for Knowledge Graph Creation. The 19th International Semantic Web Conference - Research Track (ISWC 2020).

- Samaneh Jozashoori and Maria-Esther Vidal. MapSDI: A Scaled-up Semantic Data Integration Framework for Knowledge Graph Creation. The 27th International Conference on Cooperative Information Systems (CoopIS 2019). 

- David Chaves-Fraga, Kemele M. Endris, Enrique Iglesias, Oscar Corcho, and Maria-Esther Vidal. What are the Parameters that Affect the Construction of a Knowledge Graph?. The 18th International Conference on Ontologies, DataBases, and Applications of Semantics (ODBASE 2019).

- David Chaves-Fraga, Antón Adolfo, Jhon Toledo, and Oscar Corcho. ONETT: Systematic Knowledge Graph Generation for National Access Points. The 1st International Workshop on Semantics for Transport co-located with SEMANTiCS 2019.

- David Chaves-Fraga, Freddy Priyatna, Andrea Cimmino, Jhon Toledo, Edna Ruckhaus, and Oscar Corcho. GTFS-Madrid-Bench: A benchmark for virtual knowledge graph access in the transport domain. Journal of Web Semantics, 2020.

Additional References:

- Dimou et al. 2014. Dimou, A., Sande, M.V., Colpaert, P., Verborgh, R., Mannens, E., de Walle, R.V.:RML: A generic language for integrated RDF mappings of heterogeneous data. In:Proceedings of the Workshop on Linked Data on the Web co-located with the 23rdInternational World Wide Web Conference (WWW 2014) 

# Projects where the SDM-RDFizer has been used

The SDM-RDFizer is used in the creation of the knowledge graphs of EU H2020 projects and national projects where the Scientific Data Management group participates. These projects include:

- iASiS (http://project-iasis.eu/): big data for precision medicine, based on patient data insights. The iASiS RDF knowledge graph comprises more than 1.2B RDF triples collected from more than 40 heterogeneous sources using over 1300 RML triple maps. 
- BigMedilytics (https://www.bigmedilytics.eu/): lung cancer pilot. 800 RML triple maps are used to create the lung cancer knowledge graph from around 25 data sources with 500M RDF triples.
- CLARIFY (https://www.clarify2020.eu/): predict poor health status after specific oncological treatments
- P4-LUCAT (https://www.tib.eu/de/forschung-entwicklung/projektuebersicht/projektsteckbrief/p4-lucat)
- ImProVIT (https://www.tib.eu/de/forschung-entwicklung/projektuebersicht/projektsteckbrief/improvit)
- PLATOON (https://platoon-project.eu/) 
- EUvsVirus Hackathon (April 2020) (https://blogs.tib.eu/wp/tib/2020/05/06/how-do-knowledge-graphs-contribute-to-understanding-covid-19-related-treatments/). SDM-RDFizer created the Knowledge4COVID-19 knowledge graph during the participation of the team of the Scientific Data Management group. By June 7th, 2020, this KG comprises 28M RDF triples describing at a fine-grained level 63527 COVID-19 scientific publications and COVID-19 related concepts (e.g., 5802 substances, 1.2M drug-drug interactions, and 103 molecular dysfunctions). 

The SDM-RDFizer is also used in EU H2020, EIT-Digital and Spanish national projects where the Ontology Engineering Group (Technical University of Madrid) participates. These projects, mainly focused on the domain of transportation and smart cities, include:

- H2020 - SPRINT (http://sprint-transport.eu/): performance and scalability to test a semantic architecture for the Interoperability Framework on Transport across Europe.
- EIT-SNAP (https://www.snap-project.eu/): innovation project on the application of semantic technologies for national access points.
- Open Cities (https://ciudades-abiertas.es/): national project on creating common and shared vocabularies for Spanish Cities
- Drugs4Covid (https://drugs4covid.oeg.fi.upm.es/): NLP annotations and metadata from more than 60,000 scientific papers about COVID viruses are integrated in a KG with almost 44M of facts (triples). SDM-RDFizer was used for creating this KG.

Other projects were the SDM-RDFizer is also used:
-  Virtual Platform for the H2020 European Joint Programme on Rare Disease (https://www.ejprarediseases.org)


# Installing and Running the SDM-RDFizer 
From PyPI (https://pypi.org/project/rdfizer/):
```
python3 -m pip install rdfizer
python3 -m rdfizer -c /path/to/config/file
```

From Github/Docker:
Visit the [wiki](https://github.com/SDM-TIB/SDM-RDFizer/wiki) of the repository to learn how to install and run the SDM-RDFizer. You can also take a look to our demo at: https://www.youtube.com/watch?v=DpH_57M1uOE
- Install and run the SDM-RDFizer: https://github.com/SDM-TIB/SDM-RDFizer/wiki/Install&Run
- Parameters to configure SDM-RDFizer: https://github.com/SDM-TIB/SDM-RDFizer/wiki/The-Parameters-of-the-Configuration-file
- FAQ: https://github.com/SDM-TIB/SDM-RDFizer/wiki/FAQ

## Configurations
You can easily customize your own configurations from the set of features that SDM-RDFizer offers by changing the values of the parameters in the config file. The descriptions of each parameter and the possible values are provided [here](https://github.com/SDM-TIB/SDM-RDFizer/wiki/The-Parameters-of-the-Configuration-file); "ordered" and "large_file" are the new features provided by SDM-RDFizer version 4.0.  


## Version 
```
4.7.3
```

## RML-Test Cases
See the results of the SDM-RDFizer over the RML test-cases at the [RML Implementation Report](http://rml.io/implementation-report/). SDM-RDFizer version 4.0 is tested over the latest published [test cases](https://rml.io/test-cases/) before the release.

## Experimental Evaluations
See the results of the experimental evaluations of SDM-RDFizer version 3.* at [SDM-RDFizer-Experiments repository](https://github.com/SDM-TIB/SDM-RDFizer-Experiments)

## License
This work is licensed under Apache 2.0

# Papers
[1. Conference paper published as a resource at CIKM2020](https://dl.acm.org/doi/abs/10.1145/3340531.3412881)

[2. Journal paper under the review](https://arxiv.org/pdf/2201.09694.pdf)

# Authors
The SDM-RDFizer has been developed by members of the Scientific Data Management Group at TIB, as an ongoing research effort. The development is coordinated and supervised by Maria-Esther Vidal (maria.vidal@tib.eu). We strongly encourage you to please report any issues you have with the SDM-RDFizer. You can do that over our contact email or creating a new issue here on GitHub. The SDM-RDFizer has been implemented by Enrique Iglesias (current version, iglesias@l3s.de) and Guillermo Betancourt (version 0.1, guillermojbetancourt@gmail.com) under the supervision of Samaneh Jozashoori (samaneh.jozashoori@tib.eu), David Chaves-Fraga (dchaves@fi.upm.es), and Kemele Endris (kemele.endris@tib.eu)

