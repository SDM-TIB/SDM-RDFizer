# SDM-RDFizer
This project presents the SDM-RDFizer, an interpreter of mapping rules that allow the transformation of (un)structured data into RDF knowledge graphs. The current version of the SDM-RDFizer assumes mapping rules are defined in the RDF Mapping Language (RML) by Dimou et al. The SDM-RDFizer implements optimized data structures and relational algebra operators that enable an efficient execution of RML triple maps even in the presence of Big data. SDM-RDFizer is able to process data from Heterogeneous data sources (CSV, JSON, RDB, XML). The results of the execution of the SDM-RDFizer has been reported in the following research reports:

- Samaneh Jozashoori and Maria-Esther Vidal. MapSDI: A Scaled-up Semantic Data Integrationframework for Knowledge Graph Creation. Accepted at the 27th International Conference on Cooperative Information Systems (CoopIS 2019). 

- David Chaves-Fraga, Kemele M. Endris, Enrique Iglesias, Oscar Corcho, and Maria-Esther Vidal. What are the Parameters that Affect the Construction of a Knowledge Graph?. Accepted at the 18th International Conference on Ontologies, DataBases, and Applications of Semantics (ODBASE 2019).

- David Chaves-Fraga, Antón Adolfo, Jhon Toledo, Oscar Corcho. ONETT: Systematic Knowledge Graph Generation for National Access Points. Accepted at 1st International Workshop on Semantics for Transport co-located with SEMANTiCS 2019

The SDM-RDFizer is used in the creation of the knowledge graphs of the EU H2020 projects iASiS (http://project-iasis.eu/) and BigMedilytics - lung cancer pilot (https://www.bigmedilytics.eu/)


References:
Dimou et al. 2014. Dimou, A., Sande, M.V., Colpaert, P., Verborgh, R., Mannens, E., de Walle, R.V.:RML: A generic language for integrated RDF mappings of heterogeneous data. In:Proceedings of the Workshop on Linked Data on the Web co-located with the 23rdInternational World Wide Web Conference (WWW 2014) (2014)


# About and Authors
The SDM-RDFizer has been developed by members of the Scientific Data Management Group at TIB, as an ongoing research effort. The development is coordinated and supervised by Maria-Esther Vidal (maria.vidal@tib.eu). We strongly encourage you to please report any issues you have with the SDM-RDFizer. You can do that over our contact email or creating a new issue here on Github. The SDM-RDFizer has been implemented by

Enrique Iglesias (current version)
e-mail: s6enigle@uni-bonn.de 

Guillermo Betancourt (version 0.1)
e-mail: guillermojbetancourt@gmail.com
Under the supervision of 

David Chaves 
e-mail: dchaves@fi.upm.es 

Kemele Endris
e-mail: Kemele.Endris@tib.eu 

# RML-Test Cases
See the results of the SDM-RDFizer over the RML test-cases at the [RML Implementation Report](http://rml.io/implementation-report/)

# Installing and Running the SDM-RDFizer 
The SDM-RDFizer can run by building a docker container or by installing the RDFfizer locally. 


## Accessing the SDM-RDFizer via a Docker Container

Building docker container.
Note: All documents in the same folder of the Dockerfile will be copied to the container.

```
docker build -t rdfizer .
```

Run the Application

To run the application, you need to map your data volume to `/data` folder of the container as follows:

```
docker run -d -p 4000:4000 -v /path/to/yourdata:/data rdfizer
```

Send a POST request with the configuration file to RDFizer the file

```
curl localhost:4000/graph_creation/data/path/to/config/file/inyourdata
```

Pull document from container

```
docker cp CONTAINER_ID:/app/path/to/output .
```

## Executing SDM-RDFizer Docker Container DEMO

Note: All documents in the same folder of the Dockerfile will be copied to the container.

```
docker build -t rdfizer .
```

Run the Application

```
docker run -d -p 4000:4000 -v /path/../SDM-RDFizer/example:/data rdfizer
```

Send a POST request with the configuration file to RDFizer the file

```
curl http://localhost:4000/graph_creation/data/config.ini
```

Output results can be found in example folder of SDM-RDFizer

```
/path/../SDM-RDFizer/example/output
```

## Accessing the SDM-RDFizer locally

```
python3 rdfizer/run_rdfizer.py /path/to/config/FILE
```

# Parameters to Run the SDM-RDFizer
The SDM-RDFizer receives as input a configuration file that indicates the location of the RML triple maps and the output RDF knowledge graph. This file indicates the values of additional variables required during the process of RDF knowledge graph creation:
```
main_directory: path to where the file RML triple maps are located. 
```
```
number_of_datasets: number of datasets to be semantified
```

```
output_folder: path and file where the output RDF knowledge graph will be stored
```

```
all_in_one_file: in case multiple datasets are semantified, if the resulting RDF knowledge graphs will be stored in one or multiple file. Options: yes: all the RDF triples will be stored in one file; no: otherwise.
```

```
remove_duplicate: indicates if duplicates will be removed from the output RDF knowledge graph. Options: yes: duplicated RDF triples will be eliminated; no: otherwise.
```

```
name: name of the file that will store the integrated RDF knowledge graph, whenever the all_in_one_file option is yes.
```

```
enrichment: If optimized duplicate removal functionalities are turned on or off. Options: yes: optimized duplicate removal functionalities are turned on; no: otherwise.
```
For each dataset I, an entry of the datasetI needs to be defined; the variables to be defined are as follows:

```
name: name of the file that will store the RDF knowledge graph resulting from executing the mappings indicated in the variable mapping in the entry datasetI.
```
```
mapping: location of the file where the RML triple maps are defined and will be executed over the dataset in the entry datasetI.
```
```
dbType: the type of database management system from where data is collected. Options: mysql, postgres
```
## Example of a confFILE for accessing two CSV datasets and one RDF knowledge graph is created

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

## Example of a confFILE for accessing two CSV datasets and two RDF knowledge graphs are created

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

### Example of a confFILE for accessing two datasets in MySQL
This configuration file indicates that two RDF knowledge graphs will be created from the execution of the RDF triple maps
${default:main_directory}/mappingDataset1.ttl and  ${default:main_directory}/mappingDataset2.ttl. Duplicates are eliminated in both RDF knowledge graphs. The following variables need to be defined for accessing each dataset from the database management system.

```
user: the name of the user in the database
```

```
password: the password of the account 
```
```
host: host of the database management system
```
```
port: port to access the database management system

```
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

### Example of a confFILE for accessing data in Postgres
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
3.0
```
## Test Date
```
12/07/2019
```

# License
This work is licensed under GNU/GPL v2
