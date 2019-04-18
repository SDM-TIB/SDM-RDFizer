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
main_directory: /Users/maria-esthervidal/Documents/docker/rdfizer/rdfizer/1-csv

[datasets]
number_of_datasets: 1
output_folder: ${default:main_directory}/graph
all_in_one_file: no
remove_duplicate: yes
name: 1-csv

[dataset1]
name: 1-csv
mapping: ${default:main_directory}/gtfs-csv.rml.ttl 
```

## Running the RDFizer directly

```
$ (env) python3 run_rdfizer.py configfile.ini
```