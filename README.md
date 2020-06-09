# SDM-RDFizer
This project presents the SDM-RDFizer, an interpreter of mapping rules that allows the transformation of (un)structured data into RDF knowledge graphs. The current version of the SDM-RDFizer assumes mapping rules are defined in the [RDF Mapping Language (RML) by Dimou et al](https://rml.io/specs/rml/). The SDM-RDFizer implements optimized data structures and relational algebra operators that enable an efficient execution of RML triple maps even in the presence of Big data. SDM-RDFizer is able to process data from Heterogeneous data sources (CSV, JSON, RDB, XML). The results of the execution of SDM-RDFizer has been described in the following research reports:

- Samaneh Jozashoori and Maria-Esther Vidal. MapSDI: A Scaled-up Semantic Data Integrationframework for Knowledge Graph Creation. The 27th International Conference on Cooperative Information Systems (CoopIS 2019). 

- David Chaves-Fraga, Kemele M. Endris, Enrique Iglesias, Oscar Corcho, and Maria-Esther Vidal. What are the Parameters that Affect the Construction of a Knowledge Graph?. The 18th International Conference on Ontologies, DataBases, and Applications of Semantics (ODBASE 2019).

- David Chaves-Fraga, Antón Adolfo, Jhon Toledo, Oscar Corcho. ONETT: Systematic Knowledge Graph Generation for National Access Points. Accepted at 1st International Workshop on Semantics for Transport co-located with SEMANTiCS 2019

Additional References:
Dimou et al. 2014. Dimou, A., Sande, M.V., Colpaert, P., Verborgh, R., Mannens, E., de Walle, R.V.:RML: A generic language for integrated RDF mappings of heterogeneous data. In:Proceedings of the Workshop on Linked Data on the Web co-located with the 23rdInternational World Wide Web Conference (WWW 2014) (2014)

# Projects where the SDM-RDFizer has been used
The SDM-RDFizer is used in the creation of the knowledge graphs of EU H2020 projects and national projects where the Scientific Data Management group participates. These projects include: iASiS (http://project-iasis.eu/), BigMedilytics - lung cancer pilot (https://www.bigmedilytics.eu/), CLARIFY (https://www.clarify2020.eu/), P4-LUCAT (https://www.tib.eu/de/forschung-entwicklung/projektuebersicht/projektsteckbrief/p4-lucat), ImProVIT (https://www.tib.eu/de/forschung-entwicklung/projektuebersicht/projektsteckbrief/improvit), PLATOON (https://platoon-project.eu/). The iASiS RDF knowledge graph comprises more than 1.2B RDF triples collected from more than 40 heterogeneous sources using over 1300 RML triple maps. Further, around 800 RML triple maps are used to create the lung cancer knowledge graph from around 25 data sources with 500M RDF triples. The SDM-RDFizer has also created the Knowledge4COVID-19 knowledge graph during the participation of the team of the Scientific Data Management group in the EUvsVirus Hackathon (April 2020) (https://blogs.tib.eu/wp/tib/2020/05/06/how-do-knowledge-graphs-contribute-to-understanding-covid-19-related-treatments/). By June 7th, 2020, the Knowledge4COVID-19 knowledge graph is comprised of 28M RDF triples describing at a fine-grained level 63527 COVID-19 scientific publications and COVID-19 related concepts (e.g., 5802 substances, 1.2M drug-drug interactions, and 103 molecular disfunctions). The SDM-RDFizer is also used in EU H2020, EIT-Digital and Spanish national projects where the Ontology Engineering Group (Technical University of Madrid) participates. These projects, mainly focused on the transportation and smart cities domain, include: SPRINT (http://sprint-transport.eu/), SNAP (https://www.snap-project.eu/) and Open Cities (https://ciudades-abiertas.es/). Similar to the Knowledge4COVID-19 knowledge graph, SDM-RDFizer has also been used for creating the Knowledge Graph of the Drugs4Covid project (https://drugs4covid.oeg-upm.net/) where NLP annotations and metadata from more than 60,000 scientific papers about COVID viruses are integrated in almost 44M of facts (triples). 


# Installing and Running the SDM-RDFizer 
Visit the [wiki](https://github.com/SDM-TIB/SDM-RDFizer/wiki) of the repository to learn how to install and run the SDM-RDFizer.

## Version 
```
3.2
```
## RML-Test Cases
See the results of the SDM-RDFizer over the RML test-cases at the [RML Implementation Report](http://rml.io/implementation-report/). Last test date: 08/06/2020

## Experimental Evaluations
See the results of the experimental evaluations of SDM-RDFizer at [SDM-RDFizer-Experiments repository](https://github.com/SDM-TIB/SDM-RDFizer-Experiments)


## License
This work is licensed under Apache 2.0

# Authors
The SDM-RDFizer has been developed by members of the Scientific Data Management Group at TIB, as an ongoing research effort. The development is coordinated and supervised by Maria-Esther Vidal (maria.vidal@tib.eu). We strongly encourage you to please report any issues you have with the SDM-RDFizer. You can do that over our contact email or creating a new issue here on Github. The SDM-RDFizer has been implemented by Enrique Iglesias (current version, s6enigle@uni-bonn.de) and Guillermo Betancourt (version 0.1, guillermojbetancourt@gmail.com) under the supervision of David Chaves-Fraga (dchaves@fi.upm.es), Samaneh Jozashoori (samaneh.jozashoori@tib.eu), and Kemele Endris (kemele.endris@tib.eu)

