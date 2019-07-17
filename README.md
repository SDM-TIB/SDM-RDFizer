# RDFizer
RML Engine for the generation of RDF Knowledge Graphs from Heterogeneous Datasources (CSV, JSON, RDB, XML)

## Authors
Enrique Iglesias (University of Bonn)
Kemele M. Endris (L3S Institute & TIB)
David Chaves Fraga (OEG-UPM)
Guillermo Betancourt (TIB)
Maria-Esther Vidal (L3S Institute & TIB)


## RML-Test Cases
See the results of the SDM-RDFizer over the RML test-cases at the [RML Implementation Report](http://rml.io/implementation-report/)

## How to run it

### Create the Docker Container

```
docker build -t rdfizer .
```

Run the Application

```
docker run -p 4000:80 rdfizer
```

Send a POST request with the configuration file to RDFizer the file

```
localhost:4000/graph_creation/path/to/config/file
```

### Running the RDFizer directly

```
python3 rdfizer/run_rdfizer.py /path/to/config/file
```

## How to configurate the SDM-RDFizer

### Example of a config file for CSV

```
[default]
main_directory: /Users/maria-esthervidal/Documents/docker/rdfizer/rdfizer/1-csv

[datasets]
number_of_datasets: 1
output_folder: ${default:main_directory}/graph
all_in_one_file: no
remove_duplicate: yes
name: 1-csv
enrichment: yes

[dataset1]
name: 1-csv
mapping: ${default:main_directory}/gtfs-csv.rml.ttl 
```

### Example of a Config file for MySQL

```
[default]
main_directory: /home/mvidal/Downloads/rml-test-cases/test-cases

[datasets]
number_of_datasets: 1
output_folder: ${default:main_directory}/graph
all_in_one_file: no
remove_duplicate: yes
name: RMLTC0011b-MySql
enrichment: yes
dbType: mysql


[dataset1]
user: root
password: 06012009mj
host: localhost
port: 3306
name: RMLTC0011b-MySQL
mapping: ${default:main_directory}/RMLTC0011b-MySQL/mapping.ttl
```

### Example of a Config file for Postgres

```
[default]
main_directory: /home/mvidal/Downloads/rml-test-cases/test-cases

[datasets]
number_of_datasets: 1
output_folder: ${default:main_directory}/graph
all_in_one_file: no
remove_duplicate: yes
name: RMLTC0020b-PostgreSQL
enrichment: yes
dbType: postgres


[dataset1]
user: postgres
password: postgres
host: localhost
db: 
name: RMLTC0020b-PostgreSQL
mapping: ${default:main_directory}/RMLTC0020b-PostgreSQL/mapping.ttl 
```


## Contact
```
Enrique Iglesias
e-mail: eiglesias@gmail.com
