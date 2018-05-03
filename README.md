# RDFizer

Create the Docker Container

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

## Example of a Config file

```
[default]
main_directory: /home/guillermobet/Documentos/Fraunhofer/ProjectIASIS/SemanticEnrichment

[datasets]
number_of_datasets: 1
output_folder: ${default:main_directory}/graph

[dataset1]
name: ADSampleDataWP4CO
format: csv
path: ${default:main_directory}/data/csv/ADSampleDataWP4CO.csv
mapping: ${default:main_directory}/mappings/AD_CO.ttl
remove_duplicate_triples_in_memory: yes
```