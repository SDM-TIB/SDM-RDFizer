# SDM-RDFizer
This project presents the SDM-RDFizer, an interpreter of mapping rules that allows the transformation of (un)structured data into RDF knowledge graphs. The current version of the SDM-RDFizer assumes mapping rules are defined in the [RDF Mapping Language (RML) by Dimou et al](https://rml.io/specs/rml/). The SDM-RDFizer implements optimized data structures and relational algebra operators that enable an efficient execution of RML triple maps even in the presence of Big data. SDM-RDFizer is able to process data from Heterogeneous data sources (CSV, JSON, RDB, XML). The results of the execution of SDM-RDFizer has been described in the following research reports:

- Samaneh Jozashoori and Maria-Esther Vidal. MapSDI: A Scaled-up Semantic Data Integrationframework for Knowledge Graph Creation. The 27th International Conference on Cooperative Information Systems (CoopIS 2019). 

- David Chaves-Fraga, Kemele M. Endris, Enrique Iglesias, Oscar Corcho, and Maria-Esther Vidal. What are the Parameters that Affect the Construction of a Knowledge Graph?. The 18th International Conference on Ontologies, DataBases, and Applications of Semantics (ODBASE 2019).

- David Chaves-Fraga, Antón Adolfo, Jhon Toledo, Oscar Corcho. ONETT: Systematic Knowledge Graph Generation for National Access Points. Accepted at 1st International Workshop on Semantics for Transport co-located with SEMANTiCS 2019

Additional References:
Dimou et al. 2014. Dimou, A., Sande, M.V., Colpaert, P., Verborgh, R., Mannens, E., de Walle, R.V.:RML: A generic language for integrated RDF mappings of heterogeneous data. In:Proceedings of the Workshop on Linked Data on the Web co-located with the 23rdInternational World Wide Web Conference (WWW 2014) (2014)

# Projects where the SDM-RDFizer has been used
The SDM-RDFizer is used in the creation of the knowledge graphs of EU H2020 projects and national projects where the Scientific Data Management group participates. These projects include: iASiS (http://project-iasis.eu/), BigMedilytics - lung cancer pilot (https://www.bigmedilytics.eu/), CLARIFY (https://www.clarify2020.eu/), P4-LUCAT (https://www.tib.eu/de/forschung-entwicklung/projektuebersicht/projektsteckbrief/p4-lucat), ImProVIT (https://www.tib.eu/de/forschung-entwicklung/projektuebersicht/projektsteckbrief/improvit), PLATOON (https://platoon-project.eu/). The iASiS RDF knowledge graph comprises more than 1.2B RDF triples collected from more than 40 heterogeneous sources using over 1300 RML triple maps. Further, around 800 RML triple maps are used to create the lung cancer knowledge graph from around 25 data sources with 500M RDF triples. The SDM-RDFizer has also created the Knowledge4COVID-19 knowledge graph during the participation of the team of the Scientific Data Management group in the EUvsVirus Hackathon (April 2020) (https://blogs.tib.eu/wp/tib/2020/05/06/how-do-knowledge-graphs-contribute-to-understanding-covid-19-related-treatments/). By June 7th, 2020, the Knowledge4COVID-19 knowledge graph is comprised of 28M RDF triples describing at a fine-grained level 63527 COVID-19 scientific publications and COVID-19 related concepts (e.g., 5802 substances, 1.2M drug-drug interactions, and 103 molecular disfunctions). The SDM-RDFizer is also used in EU H2020, EIT-Digital and spanish national projects where the Ontology Engineering Group (Technical University of Madrid) participates. These projects, mainly focused on the transportation and smart cities domain, include: SPRINT (http://sprint-transport.eu/), SNAP (https://www.snap-project.eu/) and Open Cities (https://ciudades-abiertas.es/). Similar as the Knowledge4COVID-19 knowledge graph, SDM-RDFizer has also used for creating the Knowledge Graph of the Drugs4Covid project (https://drugs4covid.oeg-upm.net/) where NLP annotations and metadata from more than 60,000 scientific papers about COVID viruses are integrated in almost 44M of facts (triples). 


# About and Authors
The SDM-RDFizer has been developed by members of the Scientific Data Management Group at TIB, as an ongoing research effort. The development is coordinated and supervised by Maria-Esther Vidal (maria.vidal@tib.eu). We strongly encourage you to please report any issues you have with the SDM-RDFizer. You can do that over our contact email or creating a new issue here on Github. The SDM-RDFizer has been implemented by Enrique Iglesias (current version, s6enigle@uni-bonn.de) and Guillermo Betancourt (version 0.1, guillermojbetancourt@gmail.com) under the supervision of David Chaves-Fraga (dchaves@fi.upm.es), Samaneh Jozashoori (samaneh.jozashoori@tib.eu), and Kemele Endris (kemele.endris@tib.eu)

# Installing and Running the SDM-RDFizer 
The SDM-RDFizer can run by building a docker container or by installing the RDFfizer locally. 


## Accessing the SDM-RDFizer via a docker

Building docker container.
Note: All documents in the same folder of the Dockerfile will be copied to the container.

```
docker build -t rdfizer .
```

To run the application, you need to map your data volume to `/data` folder of the container where data, mappings and config files should be located:

```
docker run -d -p 4000:4000 -v /path/to/yourdata:/data rdfizer
```

Send a POST request with the configuration file to RDFizer the file

```
curl localhost:4000/graph_creation/data/your-config-file.ini
```

Get the results from container (if output folder is inside data folder, results are already in your host)

```
docker cp CONTAINER_ID:/app/path/to/output .
```

### Example of executing SDM-RDFizer in docker

Note: All documents in the same folder of the Dockerfile will be copied to the container.

```
docker build -t rdfizer .
docker run -d -p 4000:4000 -v /path/../SDM-RDFizer/example:/data rdfizer
curl http://localhost:4000/graph_creation/data/config.ini
ls /path/../SDM-RDFizer/example/output
```

## Running the SDM-RDFizer locally

```
pip install -r requeriments.txt
python3 rdfizer/run_rdfizer.py /path/to/config/FILE
```

# Parameters to Run the SDM-RDFizer
The SDM-RDFizer receives as input a configuration file that indicates the location of the RML triple maps and the output RDF knowledge graph. This file indicates the values of additional variables required during the process of RDF knowledge graph creation:
```
main_directory: path to where the file RML triple maps are located. 
number_of_datasets: number of datasets to be semantified
output_folder: path and file where the output RDF knowledge graph will be stored
all_in_one_file: in case multiple datasets are semantified, if the resulting RDF knowledge graphs will be stored in one or multiple file. Options: yes: all the RDF triples will be stored in one file; no: otherwise.
remove_duplicate: indicates if duplicates will be removed from the output RDF knowledge graph. Options: yes: duplicated RDF triples will be eliminated; no: otherwise.
name: name of the file that will store the integrated RDF knowledge graph, whenever the all_in_one_file option is yes.
enrichment: If optimized duplicate removal functionalities are turned on or off. Options: yes: optimized duplicate removal functionalities are turned on; no: otherwise.
```

Additionally, for each dataset an entry of the dataset needs to be defined; the variables to be defined are as follows:

```
name: name of the file that will store the RDF knowledge graph resulting from executing the mappings indicated in the variable mapping in the entry dataset.
mapping: location of the file where the RML triple maps are defined and will be executed over the dataset in the entry dataset.
dbType: the type of database management system from where data is collected. Options: mysql, postgres
```

Finally, if the data sources are stored in a database, the following properties have to be also defined for each dataset:
```
user: the name of the user in the database
password: the password of the account 
host: host of the database management system
port: port to access the database management system

```

## Examples of configurations

#### Example of a config file for accessing two CSV datasets integrating them in an unique RDF knowledge graph

This configuration file indicates that one RDF knowledge graphs will be created from the execution of the RDF triple maps
${default:main_directory}/mappingDataset1.ttl and  ${default:main_directory}/mappingDataset2.ttl. Duplicates are eliminated in the RDF knowledge graph.

```
[default]
main_directory: /path/to/datasets

[datasets]
number_of_datasets: 2
output_folder: ${default:main_directory}/graph
all_in_one_file: yes
remove_duplicate: yes
enrichment: yes
name: OutputRDFkg1

[dataset1]
mapping: ${default:main_directory}/mappingDataset1.ttl 

[dataset2]
mapping: ${default:main_directory}/mappingDataset2.ttl 
```

#### Example of a config file for accessing two CSV datasets with separated RDF knowledge graph creation

This configuration file indicates that two RDF knowledge graphs will be created from the execution of the RDF triple maps
${default:main_directory}/mappingDataset1.ttl and  ${default:main_directory}/mappingDataset2.ttl. Duplicates are eliminated in both RDF knowledge graphs.

```
[default]
main_directory: main_directory: /path/to/datasets

[datasets]
number_of_datasets: 2
output_folder: ${default:main_directory}/graph
all_in_one_file: no
remove_duplicate: yes
enrichment: yes

[dataset1]
name: OutputRDFkg1
mapping: ${default:main_directory}/mappingDataset1.ttl 

[dataset2]
name: OutputRDFkg2
mapping: ${default:main_directory}/mappingDataset2.ttl 
```

#### Example of a config file for accessing two datasets in MySQL
This configuration file indicates that two RDF knowledge graphs will be created from the execution of the RDF triple maps
${default:main_directory}/mappingDataset1.ttl and  ${default:main_directory}/mappingDataset2.ttl. Duplicates are eliminated in both RDF knowledge graphs. The following variables need to be defined for accessing each dataset from the database management system.


```
[default]
main_directory: /path/to/datasets

[datasets]
number_of_datasets: 2
output_folder: ${default:main_directory}/graph
all_in_one_file: no
remove_duplicate: yes
enrichment: yes
dbType: mysql


[dataset1]
user: root
password: 06012009mj
host: localhost
port: 3306
name: OutputRDFkg1
mapping: ${default:main_directory}/mappingDataset1.ttl

[dataset2]
user: root
password: 06012009mj
host: localhost
port: 3306
name: OutputRDFkg2
mapping: ${default:main_directory}/mappingDataset2.ttl

```

#### Example of a config file for accessing data in Postgres
This configuration file indicates that one RDF knowledge graph will be created from the execution of the RDF triple map
${default:main_directory}/mappingDataset1.ttl. Duplicates are eliminated from the RDF knowledge graph. The variable ```db``` indicates the database in Postgres that will be accessed. 

```
[default]
main_directory:  $HOME$/rml-test-cases/test-cases

[datasets]
number_of_datasets: 1
output_folder: ${default:main_directory}/graph
all_in_one_file: no
remove_duplicate: yes
enrichment: yes
dbType: postgres


[dataset1]
user: postgres
password: postgres
host: localhost
db: databaseInProgess 
name: OutputRDFkg
mapping: ${default:main_directory}/mappingDataset1.ttl 

```
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
