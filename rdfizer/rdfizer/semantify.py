import os
import sys
import csv
import rdflib
from rdflib.plugins.sparql import prepareQuery
from configparser import ConfigParser, ExtendedInterpolation
from mysql import connector
from concurrent.futures import ThreadPoolExecutor
import time
import traceback
import json
import psycopg2
import pandas as pd
import xml.etree.ElementTree as ET
from urllib.request import urlopen
import gzip
import requests
import shutil
import zipfile
import io
import tarfile
from SPARQLWrapper import SPARQLWrapper, JSON
from .functions import *
from .fnml_functions import *
from .mapping_functions import *
from .inner_functions import *
import logging

try:
    from triples_map import TriplesMap as tm
except:
    from .triples_map import TriplesMap as tm

# Work in the rr:sqlQuery (change mapping parser query, add sqlite3 support, etc)
# Work in the "when subject is empty" thing (uuid.uuid4(), dependency graph over the )

global new_formulation
new_formulation = "no"
global subject_id
subject_id = 0
global generated_subjects
generated_subjects = {}
global id_number
id_number = 0
global g_triples
g_triples = {}
global number_triple
number_triple = 0
global triples
triples = []
global duplicate
duplicate = ""
global output_format
output_format = "n-triples"
global start_time
start_time = time.time()
global user, password, port, host, datab
user, password, port, host, db = "", "", "", "", ""
global join_table
join_table = {}
global po_table
po_table = {}
global prefixes
prefixes = {}
global enrichment
enrichment = ""
global ignore
ignore = "yes"
global mapping_partitions
mapping_partitions = "no"
global dic_table
dic_table = {}
global base
base = ""
global blank_message
blank_message = True
global delimiter
delimiter = {}
global logical_dump
logical_dump = {}
global current_logical_dump
current_logical_dump = ""
global general_predicates
general_predicates = {"http://www.w3.org/2000/01/rdf-schema#subClassOf": "",
                      "http://www.w3.org/2002/07/owl#sameAs": "",
                      "http://www.w3.org/2000/01/rdf-schema#seeAlso": "",
                      "http://www.w3.org/2000/01/rdf-schema#subPropertyOf": ""}

logger: logging.Logger


def get_logger(log_path='error.log'):
    logger_ = logging.getLogger('rdfizer')
    logger_.handlers.clear()
    logger_.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.ERROR)  # the log should only contain errors
    file_handler.setFormatter(formatter)  # include the time and error level in the log
    logger_.addHandler(file_handler)
    logger_.addHandler(logging.StreamHandler())  # directly print to the console
    return logger_


def prefix_extraction(original):
    string_prefixes = ""
    f = open(original, "r")
    original_mapping = f.readlines()
    for prefix in original_mapping:
        if ("prefix" in prefix) or ("base" in prefix):
            elements = prefix.split(" ")
            if "base" not in elements[0]:
                if ">" in elements[2][:-1]:
                    prefixes[elements[2][1:-1].split(">")[0]] = elements[1][:-1]
                else:
                    prefixes[elements[2][1:-1]] = elements[1][:-1]
            string_prefixes += prefix
        elif prefix == "\n":
            pass
        else:
            break
    string_prefixes += "\n"
    f.close()
    return string_prefixes


def determine_prefix(uri):
    url = ""
    value = ""
    if "#" in uri:
        url, value = uri.split("#")[0] + "#", uri.split("#")[1]
    else:
        value = uri.split("/")[len(uri.split("/")) - 1]
        char = ""
        temp = ""
        temp_string = uri
        while char != "/":
            temp = temp_string
            temp_string = temp_string[:-1]
            char = temp[len(temp) - 1]
        url = temp
    if "<" in url:
        url = url[1:]
    if ">" in value:
        value = value[:-1]
    return prefixes[url] + ":" + value


def release_subjects(triples_map, generated_subjects):
    if "_" in triples_map.triples_map_id:
        componets = triples_map.triples_map_id.split("_")[:-1]
        triples_map_id = ""
        for name in componets:
            triples_map_id += name + "_"
        triples_map_id = triples_map_id[:-1]
    else:
        triples_map_id = triples_map.triples_map_id
    generated_subjects[triples_map_id]["number_predicates"] -= 1
    if generated_subjects[triples_map_id]["number_predicates"] == 0:
        generated_subjects.pop(triples_map_id)
    return generated_subjects


def release_PTT(triples_map, predicate_list):
    for po in triples_map.predicate_object_maps_list:
        if po.predicate_map.value in general_predicates:
            if po.predicate_map.value in predicate_list:
                predicate_list[po.predicate_map.value + "_" + po.object_map.value] -= 1
                if predicate_list[po.predicate_map.value + "_" + po.object_map.value] == 0:
                    predicate_list.pop(po.predicate_map.value + "_" + po.object_map.value)
                    resource = "<" + po.predicate_map.value + ">" + "_" + po.object_map.value
                    if resource in dic_table:
                        if dic_table[resource] in g_triples:
                            g_triples.pop(dic_table[resource])
        else:
            if po.predicate_map.value in predicate_list:
                predicate_list[po.predicate_map.value] -= 1
                if predicate_list[po.predicate_map.value] == 0:
                    predicate_list.pop(po.predicate_map.value)
                    resource = "<" + po.predicate_map.value + ">"
                    if resource in dic_table:
                        if dic_table[resource] in g_triples:
                            g_triples.pop(dic_table[resource])
    if triples_map.subject_map.rdf_class != [None]:
        for rdf_type in triples_map.subject_map.rdf_class:
            resource = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" + "_" + "<{}>".format(rdf_type)
            if resource in predicate_list:
                predicate_list[resource] -= 1
                if predicate_list[resource] == 0:
                    predicate_list.pop(resource)
                    rdf_class = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" + "_" + "<{}>".format(rdf_type)
                    if rdf_class in dic_table:
                        if dic_table[rdf_class] in g_triples:
                            g_triples.pop(dic_table[rdf_class])
    return predicate_list


def dictionary_table_update(resource):
    if resource not in dic_table:
        global id_number
        dic_table[resource] = base36encode(id_number)
        id_number += 1


def join_iterator(data, iterator, parent, child, triples_map_list):
    if iterator != "":
        new_iterator = ""
        temp_keys = iterator.split(".")
        row = data
        executed = True
        for tp in temp_keys:
            new_iterator += tp + "."
            if "$" != tp and "" != tp:
                if "[*][*]" in tp:
                    if tp.split("[*][*]")[0] in row:
                        row = row[tp.split("[*][*]")[0]]
                    else:
                        row = []
                elif "[*]" in tp:
                    if tp.split("[*]")[0] in row:
                        row = row[tp.split("[*]")[0]]
                    else:
                        row = []
                elif "*" == tp:
                    pass
                else:
                    if tp in row:
                        row = row[tp]
                    else:
                        row = []
            elif tp == "":
                if len(row.keys()) == 1:
                    while list(row.keys())[0] not in temp_keys:
                        if list(row.keys())[0] not in temp_keys:
                            row = row[list(row.keys())[0]]
                            if isinstance(row, list):
                                for sub_row in row:
                                    join_iterator(sub_row, iterator, parent, child, triples_map_list)
                                executed = False
                                break
                            elif isinstance(row, str):
                                row = []
                                break
                        else:
                            join_iterator(row[list(row.keys())[0]], "", parent, child, triples_map_list)
                else:
                    path = jsonpath_find(temp_keys[len(temp_keys) - 1], row, "", [])
                    for key in path[0].split("."):
                        if key in temp_keys:
                            join_iterator(row[key], "", parent, child, triples_map_list)
                        elif key in row:
                            row = row[key]
                            if isinstance(row, list):
                                for sub_row in row:
                                    join_iterator(sub_row, iterator, parent, child, triples_map_list)
                                executed = False
                                break
                            elif isinstance(row, dict):
                                join_iterator(row, iterator, parent, child, triples_map_list)
                                executed = False
                                break
                            elif isinstance(row, str):
                                row = []
                                break
            if new_iterator != ".":
                if "*" == new_iterator[-2]:
                    for sub_row in row:
                        join_iterator(sub_row, iterator.replace(new_iterator[:-1], ""), parent, child, triples_map_list)
                    executed = False
                    break
                if "[*][*]" in new_iterator:
                    for sub_row in row:
                        for sub_sub_row in row[sub_row]:
                            join_iterator(sub_sub_row, iterator.replace(new_iterator[:-1], ""), parent, child, triples_map_list)
                    executed = False
                    break
                if isinstance(row, list):
                    for sub_row in row:
                        join_iterator(sub_row, iterator.replace(new_iterator[:-1], ""), parent, child, triples_map_list)
                    executed = False
                    break
    else:
        if parent.triples_map_id + "_" + child.child[0] not in join_table:
            hash_maker([data], parent, child,"", triples_map_list)
        else:
            hash_update([data], parent, child, parent.triples_map_id + "_" + child.child[0])


def hash_update(parent_data, parent_subject, child_object, join_id):
    hash_table = {}
    for row in parent_data:
        if child_object.parent[0] in row.keys():
            if row[child_object.parent[0]] in hash_table:
                if duplicate == "yes":
                    if parent_subject.subject_map.subject_mapping_type == "reference":
                        value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore,
                                                    parent_subject.iterator)
                        if value != None:
                            if "http" in value and "<" not in value:
                                value = "<" + value[1:-1] + ">"
                            elif "http" in value and "<" in value:
                                value = value[1:-1]
                        if value not in hash_table[row[child_object.parent[0]]]:
                            hash_table[row[child_object.parent[0]]].update({value: "object"})
                    else:
                        if string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore,
                                               parent_subject.iterator) is not None:
                            if "<" + string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object",
                                                         ignore, parent_subject.iterator) + ">" not in hash_table[
                                row[child_object.parent[0]]]:
                                hash_table[row[child_object.parent[0]]].update({"<" + string_substitution(
                                    parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore,
                                    parent_subject.iterator) + ">": "object"})
                else:
                    if parent_subject.subject_map.subject_mapping_type == "reference":
                        value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore)
                        if "http" in value and "<" not in value:
                            value = "<" + value[1:-1] + ">"
                        elif "http" in value and "<" in value:
                            value = value[1:-1]
                        hash_table[row[child_object.parent[0]]].update({value: "object"})
                    else:
                        if string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore,
                                               parent_subject.iterator) is not None:
                            hash_table[row[child_object.parent[0]]].update({"<" + string_substitution(
                                parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore,
                                parent_subject.iterator) + ">": "object"})

            else:
                if parent_subject.subject_map.subject_mapping_type == "reference":
                    value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore,
                                                parent_subject.iterator)
                    if value != None:
                        if "http" in value and "<" not in value:
                            value = "<" + value[1:-1] + ">"
                        elif "http" in value and "<" in value:
                            value = value[1:-1]
                    hash_table.update({row[child_object.parent[0]]: {value: "object"}})
                else:
                    if string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore,
                                           parent_subject.iterator) is not None:
                        hash_table.update({row[child_object.parent[0]]: {
                            "<" + string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object",
                                                      ignore, parent_subject.iterator) + ">": "object"}})
    join_table[join_id].update(hash_table)


def hash_maker(parent_data, parent_subject, child_object, quoted, triples_map_list):
    global blank_message
    hash_table = {}
    for row in parent_data:
        if quoted == "":
            if child_object.parent[0] in row.keys():
                if row[child_object.parent[0]] in hash_table:
                    if duplicate == "yes":
                        if parent_subject.subject_map.subject_mapping_type == "reference":
                            value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore,
                                                        parent_subject.iterator)
                            if value != None:
                                if "http" in value and "<" not in value:
                                    value = "<" + value[1:-1] + ">"
                                elif "http" in value and "<" in value:
                                    value = value[1:-1]
                            if value not in hash_table[row[child_object.parent[0]]]:
                                hash_table[row[child_object.parent[0]]].update({value: "object"})
                        else:
                            if string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore,
                                                   parent_subject.iterator) != None:
                                value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object",
                                                            ignore, parent_subject.iterator)
                                if value != None:
                                    if parent_subject.subject_map.term_type != None:
                                        if "BlankNode" in parent_subject.subject_map.term_type:
                                            if "/" in value:
                                                value = "_:" + encode_char(value.replace("/", "2F")).replace("%", "")
                                                if "." in value:
                                                    value = value.replace(".", "2E")
                                                if blank_message:
                                                    logger.warning(
                                                        "Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                                                    blank_message = False
                                            else:
                                                value = "_:" + encode_char(value).replace("%", "")
                                                if "." in value:
                                                    value = value.replace(".", "2E")
                                    else:
                                        value = "<" + value + ">"
                                    hash_table[row[child_object.parent[0]]].update({value: "object"})
                    else:
                        if parent_subject.subject_map.subject_mapping_type == "reference":
                            value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore,
                                                        parent_subject.iterator)
                            if "http" in value and "<" not in value:
                                value = "<" + value[1:-1] + ">"
                            elif "http" in value and "<" in value:
                                value = value[1:-1]
                            hash_table[row[child_object.parent[0]]].update({value: "object"})
                        else:
                            value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore,
                                                        parent_subject.iterator)
                            if value != None:
                                if parent_subject.subject_map.term_type != None:
                                    if "BlankNode" in parent_subject.subject_map.term_type:
                                        if "/" in value:
                                            value = "_:" + encode_char(value.replace("/", "2F")).replace("%", "")
                                            if "." in value:
                                                value = value.replace(".", "2E")
                                            if blank_message:
                                                logger.warning(
                                                    "Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                                                blank_message = False
                                        else:
                                            value = "_:" + encode_char(value).replace("%", "")
                                            if "." in value:
                                                value = value.replace(".", "2E")
                                else:
                                    value = "<" + value + ">"
                                hash_table[row[child_object.parent[0]]].update({value: "object"})

                else:
                    if parent_subject.subject_map.subject_mapping_type == "reference":
                        value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore,
                                                    parent_subject.iterator)
                        if value != None:
                            if "http" in value and "<" not in value:
                                value = "<" + value[1:-1] + ">"
                            elif "http" in value and "<" in value:
                                value = value[1:-1]
                        hash_table.update({row[child_object.parent[0]]: {value: "object"}})
                    else:
                        value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore,
                                                    parent_subject.iterator)
                        if value != None:
                            if parent_subject.subject_map.term_type != None:
                                if "BlankNode" in parent_subject.subject_map.term_type:
                                    if "/" in value:
                                        value = "_:" + encode_char(value.replace("/", "2F")).replace("%", "")
                                        if "." in value:
                                            value = value.replace(".", "2E")
                                        if blank_message:
                                            logger.warning(
                                                "Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                                            blank_message = False
                                    else:
                                        value = "_:" + encode_char(value).replace("%", "")
                                        if "." in value:
                                            value = value.replace(".", "2E")
                            else:
                                value = "<" + value + ">"
                            hash_table.update({row[child_object.parent[0]]: {value: "object"}})
        else:
            for triples in inner_semantify_file(parent_subject, triples_map_list, ",", row, base):
                if triples != None:
                    if isinstance(child_object.parent,list):
                        parent = child_object.parent[0]
                    else:
                        parent = child_object.parent
                    if row[parent] in hash_table:
                        if duplicate == "yes":
                            if triples not in hash_table[row[parent]]:
                                hash_table[row[parent]].update({triples : "subject"})
                        else:
                            hash_table[row[parent]].update({triples : "subject"})
                    else:
                        hash_table.update({row[parent] : {triples : "subject"}})
    if isinstance(child_object.child,list):
        join_table.update({parent_subject.triples_map_id + "_" + child_object.child[0] : hash_table})
    else:
        join_table.update({"quoted_" + parent_subject.triples_map_id + "_" + child_object.child : hash_table})


def hash_maker_list(parent_data, parent_subject, child_object):
    hash_table = {}
    global blank_message
    for row in parent_data:
        if sublist(child_object.parent, row.keys()):
            if child_list_value(child_object.parent, row) in hash_table:
                if duplicate == "yes":
                    if parent_subject.subject_map.subject_mapping_type == "reference":
                        value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore,
                                                    parent_subject.iterator)
                        if value != None:
                            if "http" in value and "<" not in value:
                                value = "<" + value[1:-1] + ">"
                            elif "http" in value and "<" in value:
                                value = value[1:-1]
                        hash_table[child_list_value(child_object.parent, row)].update({value: "object"})
                    else:
                        value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore,
                                                    parent_subject.iterator)
                        if value != None:
                            if parent_subject.subject_map.term_type != None:
                                if "BlankNode" in parent_subject.subject_map.term_type:
                                    if "/" in value:
                                        value = "_:" + encode_char(value.replace("/", "2F")).replace("%", "")
                                        if "." in value:
                                            value = value.replace(".", "2E")
                                        if blank_message:
                                            logger.warning(
                                                "Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                                            blank_message = False
                                    else:
                                        value = "_:" + encode_char(value).replace("%", "")
                                        if "." in value:
                                            value = value.replace(".", "2E")
                            else:
                                value = "<" + value + ">"
                            hash_table[child_list_value(child_object.parent, row)].update({value: "object"})


                else:
                    if parent_subject.subject_map.subject_mapping_type == "reference":
                        value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore,
                                                    parent_subject.iterator)
                        if "http" in value and "<" not in value:
                            value = "<" + value[1:-1] + ">"
                        elif "http" in value and "<" in value:
                            value = value[1:-1]
                        hash_table[child_list_value(child_object.parent, row)].update({value: "object"})
                    else:
                        value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore,
                                                    parent_subject.iterator)
                        if value != None:
                            if parent_subject.subject_map.term_type != None:
                                if "BlankNode" in parent_subject.subject_map.term_type:
                                    if "/" in value:
                                        value = "_:" + encode_char(value.replace("/", "2F")).replace("%", "")
                                        if "." in value:
                                            value = value.replace(".", "2E")
                                        if blank_message:
                                            logger.warning(
                                                "Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                                            blank_message = False
                                    else:
                                        value = "_:" + encode_char(value).replace("%", "")
                                        if "." in value:
                                            value = value.replace(".", "2E")
                            else:
                                value = "<" + value + ">"
                            hash_table[child_list_value(child_object.parent, row)].update({value: "object"})

            else:
                if parent_subject.subject_map.subject_mapping_type == "reference":
                    value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore,
                                                parent_subject.iterator)
                    if value != None:
                        if "http" in value and "<" not in value:
                            value = "<" + value[1:-1] + ">"
                        elif "http" in value and "<" in value:
                            value = value[1:-1]
                    hash_table.update({child_list_value(child_object.parent, row): {value: "object"}})
                else:
                    value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore,
                                                parent_subject.iterator)
                    if value != None:
                        if parent_subject.subject_map.term_type != None:
                            if "BlankNode" in parent_subject.subject_map.term_type:
                                if "/" in value:
                                    value = "_:" + encode_char(value.replace("/", "2F")).replace("%", "")
                                    if "." in value:
                                        value = value.replace(".", "2E")
                                    if blank_message:
                                        logger.warning(
                                            "Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                                        blank_message = False
                                else:
                                    value = "_:" + encode_char(value).replace("%", "")
                                    if "." in value:
                                        value = value.replace(".", "2E")
                        else:
                            value = "<" + value + ">"
                        hash_table.update({child_list_value(child_object.parent, row): {value: "object"}})
    join_table.update({parent_subject.triples_map_id + "_" + child_list(child_object.child): hash_table})


def hash_maker_xml(parent_data, parent_subject, child_object, parent_map, namespace):
    hash_table = {}
    global blank_message
    parent_parent_map = {c: p for p in parent_data.iter() for c in p}
    if "[" not in parent_subject.iterator:
        level = parent_subject.iterator.split("/")[len(parent_subject.iterator.split("/")) - 1]
    else:
        temp = parent_subject.iterator.split("[")[0]
        level = temp.split("/")[len(temp.split("/")) - 1]
    namespace = dict([node for _, node in ET.iterparse(str(parent_subject.data_source), events=['start-ns'])])
    if namespace:
        for name in namespace:
            ET.register_namespace(name, namespace[name])
    if "/" in parent_subject.iterator:
        parent_level = 2
        while len(list(parent_data.iterfind(level, namespace))) == 0:
            if parent_subject.iterator != level:
                level = parent_subject.iterator.split("/")[len(parent_subject.iterator.split("/")) - parent_level] + "/" + level
                parent_level += 1
            else:
                break
    else:
        level = "."
    for row in parent_data.iterfind(level, namespace):
        if ".." in child_object.parent[0]:
            if child_object.parent[0].count("..") - level.count("/") == 0:
                data = row
            else:
                i = 0
                data = []
                while i < child_object.parent[0].count(".."):
                    if data == []:
                        data = parent_parent_map[row]
                    else:
                        data = parent_parent_map[data]
                    i += 1
            parent_condition = child_object.parent[0].split("..")[len(child_object.parent[0].split(".."))-1][1:]
        else:
            data = row
            parent_condition = child_object.parent[0]
        if "@" in parent_condition:
            parent_condition = parent_condition.split("@")[len(parent_condition.split("@"))-1]
            if parent_condition in data.attrib:
                if data.attrib[parent_condition] in hash_table:
                    if duplicate == "yes":
                        if parent_subject.subject_map.subject_mapping_type == "reference":
                            value = string_substitution_xml(parent_subject.subject_map.value, ".+", row, "object",
                                                            parent_subject.iterator, parent_parent_map, namespace)
                            if value[0] != None:
                                if "http" in value[0]:
                                    value[0] = "<" + value[0][1:-1] + ">"
                            if value[0] not in hash_table[data.attrib[parent_condition]]:
                                hash_table[data.attrib[parent_condition]].update({value[0]: "object"})
                        else:
                            value = string_substitution_xml(parent_subject.subject_map.value, "{(.+?)}", row, "subject",
                                                            parent_subject.iterator, parent_parent_map, namespace)
                            if value != None:
                                if parent_subject.subject_map.term_type != None:
                                    if "BlankNode" in parent_subject.subject_map.term_type:
                                        if "/" in value:
                                            value = "_:" + encode_char(value.replace("/", "2F")).replace("%", "")
                                            if "." in value:
                                                value = value.replace(".", "2E")
                                            if blank_message:
                                                logger.warning(
                                                    "Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                                                blank_message = False
                                        else:
                                            value = "_:" + encode_char(value).replace("%", "")
                                            if "." in value:
                                                value = value.replace(".", "2E")
                                else:
                                    value = "<" + value + ">"
                                if value not in hash_table[data.attrib[parent_condition]]:
                                    hash_table[data.attrib[parent_condition]].update({value: "object"})
                    else:
                        if parent_subject.subject_map.subject_mapping_type == "reference":
                            value = string_substitution_xml(parent_subject.subject_map.value, ".+", row, "object",
                                                            parent_subject.iterator, parent_parent_map, namespace)
                            if value[0] != None:
                                if "http" in value:
                                    value[0] = "<" + value[0][1:-1] + ">"
                            hash_table[data.attrib[parent_condition]].update({value[0]: "object"})
                        else:
                            value = string_substitution_xml(parent_subject.subject_map.value, "{(.+?)}", row, "subject",
                                                            parent_subject.iterator, parent_parent_map, namespace)
                            if value != None:
                                if parent_subject.subject_map.term_type != None:
                                    if "BlankNode" in parent_subject.subject_map.term_type:
                                        if "/" in value:
                                            value = "_:" + encode_char(value.replace("/", "2F")).replace("%", "")
                                            if "." in value:
                                                value = value.replace(".", "2E")
                                            if blank_message:
                                                logger.warning(
                                                    "Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                                                blank_message = False
                                        else:
                                            value = "_:" + encode_char(value).replace("%", "")
                                            if "." in value:
                                                value = value.replace(".", "2E")
                                else:
                                    value = "<" + value + ">"
                                hash_table[data.attrib[parent_condition]].update({value: "object"})

                else:
                    if parent_subject.subject_map.subject_mapping_type == "reference":
                        value = string_substitution_xml(parent_subject.subject_map.value, ".+", row, "object",
                                                        parent_subject.iterator, parent_parent_map, namespace)
                        if value[0] != None:
                            if "http" in value[0]:
                                value[0] = "<" + value[0][1:-1] + ">"
                        hash_table.update({data.attrib[parent_condition]: {value[0]: "object"}})
                    else:
                        value = string_substitution_xml(parent_subject.subject_map.value, "{(.+?)}", row, "subject",
                                                        parent_subject.iterator, parent_parent_map, namespace)
                        if value != None:
                            if parent_subject.subject_map.term_type != None:
                                if "BlankNode" in parent_subject.subject_map.term_type:
                                    if "/" in value:
                                        value = "_:" + encode_char(value.replace("/", "2F")).replace("%", "")
                                        if "." in value:
                                            value = value.replace(".", "2E")
                                        if blank_message:
                                            logger.warning(
                                                "Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                                            blank_message = False
                                    else:
                                        value = "_:" + encode_char(value).replace("%", "")
                                        if "." in value:
                                            value = value.replace(".", "2E")
                            else:
                                value = "<" + value + ">"
                            hash_table.update({data.attrib[parent_condition]: {value: "object"}})
        else:
            if data.find(parent_condition).text in hash_table:
                if duplicate == "yes":
                    if parent_subject.subject_map.subject_mapping_type == "reference":
                        value = string_substitution_xml(parent_subject.subject_map.value, ".+", row, "object",
                                                        parent_subject.iterator, parent_parent_map, namespace)
                        if value[0] != None:
                            if "http" in value[0]:
                                value[0] = "<" + value[0][1:-1] + ">"
                        if value[0] not in hash_table[data.find(parent_condition).text]:
                            hash_table[data.find(parent_condition).text].update({value[0]: "object"})
                    else:
                        value = string_substitution_xml(parent_subject.subject_map.value, "{(.+?)}", row, "subject",
                                                        parent_subject.iterator, parent_parent_map, namespace)
                        if value != None:
                            if parent_subject.subject_map.term_type != None:
                                if "BlankNode" in parent_subject.subject_map.term_type:
                                    if "/" in value:
                                        value = "_:" + encode_char(value.replace("/", "2F")).replace("%", "")
                                        if "." in value:
                                            value = value.replace(".", "2E")
                                        if blank_message:
                                            logger.warning(
                                                "Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                                            blank_message = False
                                    else:
                                        value = "_:" + encode_char(value).replace("%", "")
                                        if "." in value:
                                            value = value.replace(".", "2E")
                            else:
                                value = "<" + value + ">"
                            if value not in hash_table[data.find(parent_condition).text]:
                                hash_table[data.find(parent_condition).text].update({value: "object"})
                else:
                    if parent_subject.subject_map.subject_mapping_type == "reference":
                        value = string_substitution_xml(parent_subject.subject_map.value, ".+", row, "object",
                                                        parent_subject.iterator, parent_parent_map, namespace)
                        if value[0] != None:
                            if "http" in value:
                                value[0] = "<" + value[0][1:-1] + ">"
                        hash_table[data.find(parent_condition).text].update({value[0]: "object"})
                    else:
                        value = string_substitution_xml(parent_subject.subject_map.value, "{(.+?)}", row, "subject",
                                                        parent_subject.iterator, parent_parent_map, namespace)
                        if value != None:
                            if parent_subject.subject_map.term_type != None:
                                if "BlankNode" in parent_subject.subject_map.term_type:
                                    if "/" in value:
                                        value = "_:" + encode_char(value.replace("/", "2F")).replace("%", "")
                                        if "." in value:
                                            value = value.replace(".", "2E")
                                        if blank_message:
                                            logger.warning(
                                                "Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                                            blank_message = False
                                    else:
                                        value = "_:" + encode_char(value).replace("%", "")
                                        if "." in value:
                                            value = value.replace(".", "2E")
                            else:
                                value = "<" + value + ">"
                            hash_table[data.find(parent_condition).text].update({value: "object"})

            else:
                if parent_subject.subject_map.subject_mapping_type == "reference":
                    value = string_substitution_xml(parent_subject.subject_map.value, ".+", row, "object",
                                                    parent_subject.iterator, parent_parent_map, namespace)
                    if value[0] != None:
                        if "http" in value[0]:
                            value[0] = "<" + value[0][1:-1] + ">"
                    hash_table.update({data.find(parent_condition).text: {value[0]: "object"}})
                else:
                    value = string_substitution_xml(parent_subject.subject_map.value, "{(.+?)}", row, "subject",
                                                    parent_subject.iterator, parent_parent_parent_map, namespace)
                    if value != None:
                        if parent_subject.subject_map.term_type != None:
                            if "BlankNode" in parent_subject.subject_map.term_type:
                                if "/" in value:
                                    value = "_:" + encode_char(value.replace("/", "2F")).replace("%", "")
                                    if "." in value:
                                        value = value.replace(".", "2E")
                                    if blank_message:
                                        logger.warning(
                                            "Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                                        blank_message = False
                                else:
                                    value = "_:" + encode_char(value).replace("%", "")
                                    if "." in value:
                                        value = value.replace(".", "2E")
                        else:
                            value = "<" + value + ">"
                        hash_table.update({data.find(parent_condition).text: {value: "object"}})
    join_table.update({parent_subject.triples_map_id + "_" + child_object.child[0]: hash_table})


def hash_maker_array(parent_data, parent_subject, child_object):
    hash_table = {}
    row_headers = [x[0] for x in parent_data.description]
    for row in parent_data:
        element = row[row_headers.index(child_object.parent[0])]
        if type(element) is int:
            element = str(element)
        if row[row_headers.index(child_object.parent[0])] in hash_table:
            if duplicate == "yes":
                if "<" + string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers,
                                                   "object", ignore) + ">" not in hash_table[
                    row[row_headers.index(child_object.parent[0])]]:
                    hash_table[element].update({"<" + string_substitution_array(parent_subject.subject_map.value,
                                                                                "{(.+?)}", row, row_headers, "object",
                                                                                ignore) + ">": "object"})
            else:
                hash_table[element].update({"<" + string_substitution_array(parent_subject.subject_map.value, "{(.+?)}",
                                                                            row, row_headers, "object",
                                                                            ignore) + ">": "object"})

        else:
            hash_table.update({element: {
                "<" + string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers, "object",
                                                ignore) + ">": "object"}})
    join_table.update({parent_subject.triples_map_id + "_" + child_object.child[0]: hash_table})


def hash_maker_array_list(parent_data, parent_subject, child_object, r_w):
    hash_table = {}
    global blank_message
    row_headers = [x[0] for x in parent_data.description]
    for row in parent_data:
        if child_list_value_array(child_object.parent, row, row_headers) in hash_table:
            if duplicate == "yes":
                if parent_subject.subject_map.subject_mapping_type == "reference":
                    value = string_substitution_array(parent_subject.subject_map.value, ".+", row, row_headers,
                                                      "object", ignore)
                    if value != None:
                        if "http" in value and "<" not in value:
                            value = "<" + value[1:-1] + ">"
                        elif "http" in value and "<" in value:
                            value = value[1:-1]
                    if value not in hash_table[child_list_value_array(child_object.parent, row, row_headers)]:
                        hash_table[child_list_value_array(child_object.parent, row, row_headers)].update(
                            {value + ">": "object"})

                else:
                    value = string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers,
                                                      "object", ignore)
                    if value != None:
                        if parent_subject.subject_map.term_type != None:
                            if "BlankNode" in parent_subject.subject_map.term_type:
                                if "/" in value:
                                    value = "_:" + encode_char(value.replace("/", "2F")).replace("%", "")
                                    if "." in value:
                                        value = value.replace(".", "2E")
                                    if blank_message:
                                        logger.warning(
                                            "Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                                        blank_message = False
                                else:
                                    value = "_:" + encode_char(value).replace("%", "")
                                    if "." in value:
                                        value = value.replace(".", "2E")
                        else:
                            value = "<" + value + ">"
                        if value not in hash_table[child_list_value_array(child_object.parent, row, row_headers)]:
                            hash_table[child_list_value_array(child_object.parent, row, row_headers)].update(
                                {value: "object"})
            else:
                if parent_subject.subject_map.subject_mapping_type == "reference":
                    value = string_substitution_array(parent_subject.subject_map.value, ".+", row, row_headers,
                                                      "object", ignore)
                    if value != None:
                        if "http" in value and "<" not in value:
                            value = "<" + value[1:-1] + ">"
                        elif "http" in value and "<" in value:
                            value = value[1:-1]
                    hash_table[child_list_value_array(child_object.parent, row, row_headers)].update({value: "object"})
                else:
                    value = string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers,
                                                      "object", ignore)
                    if value != None:
                        if parent_subject.subject_map.term_type != None:
                            if "BlankNode" in parent_subject.subject_map.term_type:
                                if "/" in value:
                                    value = "_:" + encode_char(value.replace("/", "2F")).replace("%", "")
                                    if blank_message:
                                        logger.warning(
                                            "Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                                        blank_message = False
                                else:
                                    value = "_:" + encode_char(value).replace("%", "")
                                if "." in value:
                                    value = value.replace(".", "2E")
                        else:
                            value = "<" + value + ">"
                        hash_table[child_list_value_array(child_object.parent, row, row_headers)].update(
                            {value: "object"})

        else:
            if parent_subject.subject_map.subject_mapping_type == "reference":
                value = string_substitution_array(parent_subject.subject_map.value, ".+", row, row_headers, "object",
                                                  ignore)
                if value != None:
                    if "http" in value and "<" not in value:
                        value = "<" + value[1:-1] + ">"
                    elif "http" in value and "<" in value:
                        value = value[1:-1]
                hash_table.update({child_list_value_array(child_object.parent, row, row_headers): {value: "object"}})
            else:
                value = string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers,
                                                  "object", ignore)
                if value != None:
                    if parent_subject.subject_map.term_type != None:
                        if "BlankNode" in parent_subject.subject_map.term_type:
                            if "/" in value:
                                value = "_:" + encode_char(value.replace("/", "2F")).replace("%", "")
                                if blank_message:
                                    logger.warning(
                                        "Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                                    blank_message = False
                            else:
                                value = "_:" + encode_char(value).replace("%", "")
                            if "." in value:
                                value = value.replace(".", "2E")
                    else:
                        value = "<" + value + ">"
                    hash_table.update(
                        {child_list_value_array(child_object.parent, row, row_headers): {value: "object"}})
    join_table.update({parent_subject.triples_map_id + "_" + child_list(child_object.child): hash_table})


def mappings_expansion(triples_map_list):
    global generated_subjects
    new_list = []
    if mapping_partitions == "yes":
        for triples_map in triples_map_list:
            generated_subjects[triples_map.triples_map_id] = {
                "number_predicates": len(triples_map.predicate_object_maps_list)}
            subject_attr = []
            if triples_map.subject_map.subject_mapping_type == "template":
                for attr in triples_map.subject_map.value.split("{"):
                    if "}" in attr:
                        subject_attr.append(attr.split("}")[0])
            elif triples_map.subject_map.subject_mapping_type == "reference":
                subject_attr.append(triples_map.subject_map.reference)
            generated_subjects[triples_map.triples_map_id]["subject_attr"] = subject_attr
            if len(triples_map.predicate_object_maps_list) > 1:
                i = 0
                for po in triples_map.predicate_object_maps_list:
                    if i == 0:
                        subject_map = triples_map.subject_map
                    else:
                        subject_map = tm.SubjectMap(triples_map.subject_map.value, triples_map.subject_map.condition,
                                                    triples_map.subject_map.subject_mapping_type,
                                                    triples_map.subject_map.parent,triples_map.child, [None],
                                                    triples_map.subject_map.term_type, triples_map.subject_map.graph,
                                                    triples_map.func_result)
                    if po.object_map.mapping_type == "parent triples map":
                        if po.object_map.child != None:
                            for triples_map_element in triples_map_list:
                                if po.object_map.value == triples_map_element.triples_map_id:
                                    if len(triples_map_element.predicate_object_maps_list) > 1:
                                        po.object_map.value = po.object_map.value + "_1"
                                    if triples_map.file_format == "JSONPath" or triples_map.file_format == "XPath":
                                        if triples_map.data_source == triples_map_element.data_source:
                                            if triples_map.iterator == triples_map_element.iterator:
                                                if po.object_map.child[0] == po.object_map.parent[0]:
                                                    if triples_map_element.subject_map.subject_mapping_type == "template":
                                                        object_map = tm.ObjectMap("template",
                                                                                  triples_map_element.subject_map.value,
                                                                                  "None", "None", "None",
                                                                                  triples_map_element.subject_map.term_type,
                                                                                  "None", "None")
                                                    else:
                                                        object_map = tm.ObjectMap("reference",
                                                                                  triples_map_element.subject_map.value,
                                                                                  "None", "None", "None",
                                                                                  triples_map_element.subject_map.term_type,
                                                                                  "None", "None")
                                                    predicate_object = tm.PredicateObjectMap(po.predicate_map,
                                                                                             object_map, po.graph)
                                                    new_list += [
                                                        tm.TriplesMap(triples_map.triples_map_id + "_" + str(i),
                                                                      triples_map.data_source, subject_map,
                                                                      [predicate_object],
                                                                      triples_map.reference_formulation,
                                                                      triples_map.iterator, triples_map.tablename,
                                                                      triples_map.query,
                                                                      triples_map.function,
                                                                      triples_map.func_map_list,
                                                                      triples_map.mappings_type)]
                                                else:
                                                    predicate_object = tm.PredicateObjectMap(po.predicate_map,
                                                                                             po.object_map, po.graph)
                                                    new_list += [
                                                        tm.TriplesMap(triples_map.triples_map_id + "_" + str(i),
                                                                      triples_map.data_source, subject_map,
                                                                      [predicate_object],
                                                                      triples_map.reference_formulation,
                                                                      triples_map.iterator, triples_map.tablename,
                                                                      triples_map.query,
                                                                      triples_map.function,
                                                                      triples_map.func_map_list,
                                                                      triples_map.mappings_type)]
                                            else:
                                                predicate_object = tm.PredicateObjectMap(po.predicate_map,
                                                                                         po.object_map, po.graph)
                                                new_list += [tm.TriplesMap(triples_map.triples_map_id + "_" + str(i),
                                                                           triples_map.data_source, subject_map,
                                                                           [predicate_object],
                                                                           triples_map.reference_formulation,
                                                                           triples_map.iterator, triples_map.tablename,
                                                                           triples_map.query,
                                                                           triples_map.function,
                                                                           triples_map.func_map_list,
                                                                      triples_map.mappings_type)]
                                        else:
                                            predicate_object = tm.PredicateObjectMap(po.predicate_map, po.object_map,
                                                                                     po.graph)
                                            new_list += [tm.TriplesMap(triples_map.triples_map_id + "_" + str(i),
                                                                       triples_map.data_source, subject_map,
                                                                       [predicate_object],
                                                                       triples_map.reference_formulation,
                                                                       triples_map.iterator, triples_map.tablename,
                                                                       triples_map.query,
                                                                       triples_map.function,
                                                                       triples_map.func_map_list,
                                                                      triples_map.mappings_type)]
                                    elif str(triples_map.file_format).lower() == "csv":
                                        if triples_map.data_source == triples_map_element.data_source:
                                            if po.object_map.child[0] == po.object_map.parent[0]:
                                                if triples_map_element.subject_map.subject_mapping_type == "template":
                                                    object_map = tm.ObjectMap("template",
                                                                              triples_map_element.subject_map.value,
                                                                              "None", "None", "None",
                                                                              triples_map_element.subject_map.term_type,
                                                                              "None", "None")
                                                else:
                                                    object_map = tm.ObjectMap("reference",
                                                                              triples_map_element.subject_map.value,
                                                                              "None", "None", "None",
                                                                              triples_map_element.subject_map.term_type,
                                                                              "None", "None")
                                                predicate_object = tm.PredicateObjectMap(po.predicate_map, object_map,
                                                                                         po.graph)
                                                new_list += [tm.TriplesMap(triples_map.triples_map_id + "_" + str(i),
                                                                           triples_map.data_source, subject_map,
                                                                           [predicate_object],
                                                                           triples_map.reference_formulation,
                                                                           triples_map.iterator, triples_map.tablename,
                                                                           triples_map.query,
                                                                           triples_map.function,
                                                                           triples_map.func_map_list,
                                                                      triples_map.mappings_type)]
                                            else:
                                                predicate_object = tm.PredicateObjectMap(po.predicate_map,
                                                                                         po.object_map, po.graph)
                                                new_list += [tm.TriplesMap(triples_map.triples_map_id + "_" + str(i),
                                                                           triples_map.data_source, subject_map,
                                                                           [predicate_object],
                                                                           triples_map.reference_formulation,
                                                                           triples_map.iterator, triples_map.tablename,
                                                                           triples_map.query,
                                                                           triples_map.function,
                                                                           triples_map.func_map_list,
                                                                      triples_map.mappings_type)]
                                        else:
                                            predicate_object = tm.PredicateObjectMap(po.predicate_map, po.object_map,
                                                                                     po.graph)
                                            new_list += [tm.TriplesMap(triples_map.triples_map_id + "_" + str(i),
                                                                       triples_map.data_source, subject_map,
                                                                       [predicate_object],
                                                                       triples_map.reference_formulation,
                                                                       triples_map.iterator, triples_map.tablename,
                                                                       triples_map.query,
                                                                       triples_map.function,
                                                                       triples_map.func_map_list,
                                                                      triples_map.mappings_type)]
                                    else:
                                        if triples_map.query == triples_map_element.query or triples_map.tablename == triples_map_element.tablename:
                                            if po.object_map.child[0] == po.object_map.parent[0]:
                                                if triples_map_element.subject_map.subject_mapping_type == "template":
                                                    object_map = tm.ObjectMap("template",
                                                                              triples_map_element.subject_map.value,
                                                                              "None", "None", "None",
                                                                              triples_map_element.subject_map.term_type,
                                                                              "None", "None")
                                                else:
                                                    object_map = tm.ObjectMap("reference",
                                                                              triples_map_element.subject_map.value,
                                                                              "None", "None", "None",
                                                                              triples_map_element.subject_map.term_type,
                                                                              "None", "None")
                                                predicate_object = tm.PredicateObjectMap(po.predicate_map, object_map,
                                                                                         po.graph)
                                                new_list += [tm.TriplesMap(triples_map.triples_map_id + "_" + str(i),
                                                                           triples_map.data_source, subject_map,
                                                                           [predicate_object],
                                                                           triples_map.reference_formulation,
                                                                           triples_map.iterator, triples_map.tablename,
                                                                           triples_map.query,
                                                                           triples_map.function,
                                                                           triples_map.func_map_list,
                                                                      triples_map.mappings_type,
                                                                      triples_map.mappings_type)]
                                            else:
                                                predicate_object = tm.PredicateObjectMap(po.predicate_map,
                                                                                         po.object_map, po.graph)
                                                new_list += [tm.TriplesMap(triples_map.triples_map_id + "_" + str(i),
                                                                           triples_map.data_source, subject_map,
                                                                           [predicate_object],
                                                                           triples_map.reference_formulation,
                                                                           triples_map.iterator, triples_map.tablename,
                                                                           triples_map.query,
                                                                           triples_map.function,
                                                                           triples_map.func_map_list,
                                                                      triples_map.mappings_type,
                                                                      triples_map.mappings_type)]
                                        else:
                                            predicate_object = tm.PredicateObjectMap(po.predicate_map, po.object_map,
                                                                                     po.graph)
                                            new_list += [tm.TriplesMap(triples_map.triples_map_id + "_" + str(i),
                                                                       triples_map.data_source, subject_map,
                                                                       [predicate_object],
                                                                       triples_map.reference_formulation,
                                                                       triples_map.iterator, triples_map.tablename,
                                                                       triples_map.query,
                                                                       triples_map.function,
                                                                       triples_map.func_map_list,
                                                                      triples_map.mappings_type,
                                                                      triples_map.mappings_type)]
                                    break
                        else:
                            for triples_map_element in triples_map_list:
                                if po.object_map.value == triples_map_element.triples_map_id:
                                    if str(triples_map.file_format).lower() == "csv" or triples_map.file_format == "JSONPath" or triples_map.file_format == "XPath":
                                        if triples_map.data_source == triples_map_element.data_source:
                                            if triples_map.file_format == "JSONPath" or triples_map.file_format == "XPath":
                                                if triples_map.iterator == triples_map_element.iterator:
                                                    if triples_map_element.subject_map.subject_mapping_type == "template":
                                                        object_map = tm.ObjectMap("template",
                                                                                  triples_map_element.subject_map.value,
                                                                                  "None", "None", "None",
                                                                                  triples_map_element.subject_map.term_type,
                                                                                  "None", "None")
                                                    else:
                                                        object_map = tm.ObjectMap("reference",
                                                                                  triples_map_element.subject_map.value,
                                                                                  "None", "None", "None",
                                                                                  triples_map_element.subject_map.term_type,
                                                                                  "None", "None")
                                                    predicate_object = tm.PredicateObjectMap(po.predicate_map,
                                                                                             object_map, po.graph)
                                                    new_list += [
                                                        tm.TriplesMap(triples_map.triples_map_id + "_" + str(i),
                                                                      triples_map.data_source, subject_map,
                                                                      [predicate_object],
                                                                      triples_map.reference_formulation,
                                                                      triples_map.iterator, triples_map.tablename,
                                                                      triples_map.query,
                                                                      triples_map.function,
                                                                      triples_map.func_map_list,
                                                                      triples_map.mappings_type)]
                                                else:
                                                    if len(triples_map_element.predicate_object_maps_list) > 1:
                                                        po.object_map.value = po.object_map.value + "_1"
                                                    predicate_object = tm.PredicateObjectMap(po.predicate_map,
                                                                                             po.object_map, po.graph)
                                                    new_list += [
                                                        tm.TriplesMap(triples_map.triples_map_id + "_" + str(i),
                                                                      triples_map.data_source, subject_map,
                                                                      [predicate_object],
                                                                      triples_map.reference_formulation,
                                                                      triples_map.iterator, triples_map.tablename,
                                                                      triples_map.query,
                                                                      triples_map.function,
                                                                      triples_map.func_map_list,
                                                                      triples_map.mappings_type)]
                                            elif str(triples_map.file_format).lower() == "csv":
                                                if triples_map_element.subject_map.subject_mapping_type == "template":
                                                    object_map = tm.ObjectMap("template",
                                                                              triples_map_element.subject_map.value,
                                                                              "None", "None", "None",
                                                                              triples_map_element.subject_map.term_type,
                                                                              "None", "None")
                                                else:
                                                    object_map = tm.ObjectMap("reference",
                                                                              triples_map_element.subject_map.value,
                                                                              "None", "None", "None",
                                                                              triples_map_element.subject_map.term_type,
                                                                              "None", "None")
                                                predicate_object = tm.PredicateObjectMap(po.predicate_map, object_map,
                                                                                         po.graph)
                                                new_list += [tm.TriplesMap(triples_map.triples_map_id + "_" + str(i),
                                                                           triples_map.data_source, subject_map,
                                                                           [predicate_object],
                                                                           triples_map.reference_formulation,
                                                                           triples_map.iterator, triples_map.tablename,
                                                                           triples_map.query,
                                                                           triples_map.function,
                                                                      triples_map.func_map_list,
                                                                      triples_map.mappings_type)]
                                        else:
                                            new_list += [tm.TriplesMap(triples_map.triples_map_id + "_" + str(i),
                                                                       triples_map.data_source, subject_map, [po],
                                                                       triples_map.reference_formulation,
                                                                       triples_map.iterator, triples_map.tablename,
                                                                       triples_map.query,
                                                                       triples_map.function,
                                                                      triples_map.func_map_list,
                                                                      triples_map.mappings_type)]
                                    else:
                                        if (
                                                triples_map.query != "None" and triples_map_element.query != "None" and triples_map.query == triples_map_element.query) or (
                                                triples_map.tablename == triples_map_element.tablename and triples_map.tablename != "None" and triples_map_element.tablename != "None"):
                                            if triples_map_element.subject_map.subject_mapping_type == "template":
                                                object_map = tm.ObjectMap("template",
																		  triples_map_element.subject_map.value, "None",
																		  "None", "None",
																		  triples_map_element.subject_map.term_type,
																		  "None", "None")
                                            else:
                                                object_map = tm.ObjectMap("reference",
																		  triples_map_element.subject_map.value, "None",
                                                                          "None", "None",
																		  triples_map_element.subject_map.term_type,
																		  "None", "None")
                                            predicate_object = tm.PredicateObjectMap(po.predicate_map, object_map,
																					 po.graph)
                                            new_list += [tm.TriplesMap(triples_map.triples_map_id + "_" + str(i),
                                                                       triples_map.data_source, subject_map,
                                                                       [predicate_object],
                                                                       triples_map.reference_formulation,
                                                                       triples_map.iterator, triples_map.tablename,
                                                                       triples_map.query,
                                                                       triples_map.function,
                                                                      triples_map.func_map_list,
                                                                      triples_map.mappings_type)]
                                        else:
                                            new_list += [tm.TriplesMap(triples_map.triples_map_id + "_" + str(i),
                                                                       triples_map.data_source, subject_map, [po],
                                                                       triples_map.reference_formulation,
                                                                       triples_map.iterator, triples_map.tablename,
                                                                       triples_map.query,
                                                                       triples_map.function,
                                                                      triples_map.func_map_list,
                                                                      triples_map.mappings_type)]
                                break
                    else:
                        new_list += [tm.TriplesMap(triples_map.triples_map_id + "_" + str(i), triples_map.data_source,
                                                   subject_map, [po], triples_map.reference_formulation,
                                                   triples_map.iterator, triples_map.tablename, triples_map.query,
                                                   triples_map.function,triples_map.func_map_list,
                                                                      triples_map.mappings_type)]
                    i += 1
            else:
                new_list += [triples_map]
    else:
        for triples_map in triples_map_list:
            pom_list = []
            for po in triples_map.predicate_object_maps_list:
                if po.object_map.mapping_type == "parent triples map":
                    for triples_map_element in triples_map_list:
                        if po.object_map.value == triples_map_element.triples_map_id:
                            if po.object_map.child != None:
                                if str(triples_map.file_format).lower() == "csv" or triples_map.file_format == "JSONPath" or triples_map.file_format == "XPath":
                                    if triples_map.data_source == triples_map_element.data_source:
                                        if po.object_map.child[0] == po.object_map.parent[0]:
                                            """if triples_map_element.subject_map.subject_mapping_type == "template":
                                                    object_map = tm.ObjectMap("template",
                                                                              triples_map_element.subject_map.value, "None",
                                                                              "None", "None",
                                                                              triples_map_element.subject_map.term_type,
                                                                              "None", "None")
                                                else:
                                                    object_map = tm.ObjectMap("reference",
                                                                              triples_map_element.subject_map.value, "None",
                                                                              "None", "None",
                                                                              triples_map_element.subject_map.term_type,
                                                                              "None", "None")
                                                pom_list.append(
                                                    tm.PredicateObjectMap(po.predicate_map, object_map, po.graph))"""
                                            pom_list.append(po)
                                        else:
                                            pom_list.append(po)
                                    else:
                                        pom_list.append(po)
                                elif (
                                        triples_map.query != "None" and triples_map_element.query != "None" and triples_map.query == triples_map_element.query) or (
                                        triples_map.tablename == triples_map_element.tablename and triples_map.tablename != "None" and triples_map_element.tablename != "None"):
                                    if po.object_map.child[0] == po.object_map.parent[0]:
                                        if triples_map_element.subject_map.subject_mapping_type == "template":
                                            object_map = tm.ObjectMap("template", triples_map_element.subject_map.value,
                                                                      "None", "None", "None",
                                                                      triples_map_element.subject_map.term_type, "None",
                                                                      "None")
                                        else:
                                            object_map = tm.ObjectMap("reference",
                                                                      triples_map_element.subject_map.value, "None",
                                                                      "None", "None",
                                                                      triples_map_element.subject_map.term_type, "None",
                                                                      "None")
                                        pom_list.append(tm.PredicateObjectMap(po.predicate_map, object_map, po.graph))
                                    else:
                                        pom_list.append(po)
                                else:
                                    pom_list.append(po)
                            else:
                                if triples_map_element.subject_map.subject_mapping_type == "template":
                                    object_map = tm.ObjectMap("template", triples_map_element.subject_map.value, "None",
                                                              "None", "None", triples_map_element.subject_map.term_type,
                                                              "None", "None")
                                else:
                                    object_map = tm.ObjectMap("reference", triples_map_element.subject_map.value,
                                                              "None", "None", "None",
                                                              triples_map_element.subject_map.term_type, "None", "None")
                                pom_list.append(tm.PredicateObjectMap(po.predicate_map, object_map, po.graph))
                else:
                    pom_list.append(po)
            new_list += [
                tm.TriplesMap(triples_map.triples_map_id, triples_map.data_source, triples_map.subject_map, pom_list,
                              triples_map.reference_formulation, triples_map.iterator, triples_map.tablename,
                              triples_map.query,triples_map.function,triples_map.func_map_list,triples_map.mappings_type)]

    return new_list


def mapping_parser(mapping_file):
    """
	(Private function, not accessible from outside this package)

	Takes a mapping file in Turtle (.ttl) or Notation3 (.n3) format and parses it into a list of
	TriplesMap objects (refer to TriplesMap.py file)

	Parameters
	----------
	mapping_file : string
		Path to the mapping file

	Returns
	-------
	A list of TriplesMap objects containing all the parsed rules from the original mapping file
	"""

    mapping_graph = rdflib.Graph()

    try:
        mapping_graph.parse(mapping_file, format='n3')
    except Exception as n3_mapping_parse_exception:
        logger.exception(n3_mapping_parse_exception)
        logger.exception('Could not parse {} as a mapping file. Aborting...'.format(mapping_file))
        sys.exit(1)

    if new_formulation == "yes":
        function_query = """
        prefix rr: <http://www.w3.org/ns/r2rml#> 
        prefix rml: <http://w3id.org/rml/> 
        prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#>
        prefix td: <https://www.w3.org/2019/wot/td#>
        prefix hctl: <https://www.w3.org/2019/wot/hypermedia#> 
        prefix dcat: <http://www.w3.org/ns/dcat#>
        prefix void: <http://rdfs.org/ns/void#>
        prefix sd: <http://www.w3.org/ns/sparql-service-description#>
        SELECT DISTINCT *
        WHERE {
        OPTIONAL {
                ?function_id rml:function ?function .
                OPTIONAL {
                    ?function_id rml:input ?input.
                    ?input rml:parameter ?param.
                    OPTIONAL {
                        ?input rml:inputValue ?input_value.
                    }
                    OPTIONAL {
                        ?input rml:inputValueMap ?input_map.
                        OPTIONAL {?input_map rml:reference ?param_reference.}
                        OPTIONAL {?input_map rml:template ?param_template.}
                        OPTIONAL {?input_map rml:functionExecution ?param_func.}
                    }
                }
            }
        }

        """
        mapping_query = """
        prefix rr: <http://www.w3.org/ns/r2rml#> 
        prefix rml: <http://w3id.org/rml/> 
        prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#>
        prefix td: <https://www.w3.org/2019/wot/td#>
        prefix hctl: <https://www.w3.org/2019/wot/hypermedia#> 
        prefix dcat: <http://www.w3.org/ns/dcat#>
        prefix void: <http://rdfs.org/ns/void#>
        prefix sd: <http://www.w3.org/ns/sparql-service-description#>
        SELECT DISTINCT *
        WHERE {
    # Subject -------------------------------------------------------------------------
            OPTIONAL{?triples_map_id a ?mappings_type}
            ?triples_map_id rml:logicalSource ?_source .
            OPTIONAL{
                ?_source rml:source ?source_attr .
                OPTIONAL {?source_attr rml:root ?root .}
                ?source_attr rml:path ?data_source
            }
            OPTIONAL{
                ?_source rml:source ?data_link .
                ?data_link dcat:downloadURL ?url_source .
            }
            OPTIONAL{
                ?_source rml:source ?data_link .
                ?data_link void:dataDump ?url_source .
            }
            OPTIONAL{
                ?_source rml:source ?data_link .
                ?data_link dcat:url ?url_source .
                ?data_link dcat:dialect ?dialect .
                ?dialect dcat:delimiter ?delimiter . 
            }
            OPTIONAL{
                ?_source rml:source ?data_link .
                ?data_link td:hasPropertyAffordance ?has_form .
                ?has_form td:hasForm ?form .
                ?form hctl:hasTarget ?url_source .
            }
            OPTIONAL{
                ?_source rml:source ?data_link .
                ?data_link sd:endpoint ?url_source . 
            }
            OPTIONAL {?_source rml:referenceFormulation ?ref_form .}
            OPTIONAL { ?_source rml:iterator ?iterator . }
            OPTIONAL { ?_source rr:tableName ?tablename .}
            OPTIONAL { ?_source rml:query ?query .}

            ?triples_map_id rml:subjectMap ?_subject_map .
            OPTIONAL {?_subject_map rml:template ?subject_template .}
            OPTIONAL {?_subject_map rml:reference ?subject_reference .}
            OPTIONAL {?_subject_map rml:constant ?subject_constant}
            OPTIONAL {?_subject_map rml:quotedTriplesMap ?subject_quoted .
                OPTIONAL {
                        ?_subject_map rml:joinCondition ?join_condition .
                        ?join_condition rml:child ?subject_child_value;
                                     rml:parent ?subject_parent_value.
                    }
                }
            OPTIONAL { ?_subject_map rml:class ?rdf_class . }
            OPTIONAL { ?_subject_map rml:termType ?termtype . }
            OPTIONAL { ?_subject_map rml:graph ?graph . }
            OPTIONAL { ?_subject_map rml:graphMap ?subject_graph_structure .
                       ?subject_graph_structure rml:constant ?graph . 
                       OPTIONAL {?subject_graph_structure rml:logicalTarget ?output .
                                 ?output rml:target ?dump.
                                 ?dump void:dataDump ?subject_graph_dump.}
                }
            OPTIONAL { ?_subject_map rml:graphMap ?subj_graph_structure .
                       ?subj_graph_structure rml:template ?graph . 
                       OPTIONAL {?subj_graph_structure rml:logicalTarget ?subj_output .
                                 ?subj_output rml:target ?subj_dump.
                                 ?subj_dump void:dataDump ?subject_graph_dump.}
               }
            OPTIONAL {?_subject_map rml:functionExecution ?subject_function .
                        OPTIONAL {
                            ?_subject_map rml:returnMap ?output_map .
                            ?output_map rml:constant ?subject_output .        
                            }
                    }
            OPTIONAL {?_subject_map rml:logicalTarget ?output.
                      ?output rml:target ?dump.
                      ?dump void:dataDump ?subject_dump.
                    }         

    # Predicate -----------------------------------------------------------------------
            OPTIONAL {
            ?triples_map_id rml:predicateObjectMap ?_predicate_object_map .

            OPTIONAL {
                ?_predicate_object_map rml:predicateMap ?_predicate_map .
                ?_predicate_map rml:constant ?predicate_constant .
            }
            OPTIONAL {
                ?_predicate_object_map rml:predicateMap ?_predicate_map .
                ?_predicate_map rml:template ?predicate_template .
            }
            OPTIONAL {
                ?_predicate_object_map rml:predicateMap ?_predicate_map .
                ?_predicate_map rml:reference ?predicate_reference .
            }
            OPTIONAL {
                ?_predicate_object_map rml:predicate ?predicate_constant_shortcut .
            }
            OPTIONAL {
                ?_predicate_object_map rml:predicateMap ?_predicate_map .
                ?_predicate_map rml:functionExecution ?predicate_function .
                OPTIONAL {
                    ?_predicate_map rml:returnMap ?output_map .
                    ?output_map rml:constant ?predicate_output .        
                    }
            }
            OPTIONAL {
                ?_predicate_map rml:logicalTarget ?pre_output .
                ?pre_output rml:target ?pre_dump.
                ?pre_dump void:dataDump ?predicate_dump.
            }

    # Object --------------------------------------------------------------------------

            OPTIONAL {
                ?_predicate_object_map rml:objectMap ?_object_map .
                ?_object_map rml:constant ?object_constant .
                OPTIONAL { ?_object_map rml:language ?language .}
                OPTIONAL {?_object_map rml:languageMap ?language_map.
                          OPTIONAL {?language_map rml:reference ?language_value.}
                          OPTIONAL {?language_map rml:constant ?language.}
                          OPTIONAL {?language_map rml:logicalTarget ?output .
                                 ?output rml:target ?dump.
                                 ?dump void:dataDump ?language_dump.}
                         }
                OPTIONAL {?_object_map rml:datatypeMap ?datatype_map.
                          OPTIONAL {?datatype_map rml:template ?datatype_value.}
                          OPTIONAL {?datatype_map rml:constant ?datatype.}
                          OPTIONAL {?datatype_map rml:logicalTarget ?output .
                                 ?output rml:target ?dump.
                                 ?dump void:dataDump ?datatype_dump.}
                         }
                OPTIONAL {?_object_map rml:termType ?term .}
                OPTIONAL {
                    ?_object_map rml:datatype ?object_datatype .
                }
            }
            OPTIONAL {
                ?_predicate_object_map rml:objectMap ?_object_map .
                ?_object_map rml:template ?object_template .
                OPTIONAL {?_object_map rml:termType ?term .}
                OPTIONAL {?_object_map rml:languageMap ?language_map.
                          ?language_map rml:reference ?language_value.}
                OPTIONAL {
                    ?_object_map rml:datatype ?object_datatype .
                }
            }
            OPTIONAL {
                ?_predicate_object_map rml:objectMap ?_object_map .
                ?_object_map rml:reference ?object_reference .
                OPTIONAL { ?_object_map rml:language ?language .}
                OPTIONAL {?_object_map rml:languageMap ?language_map.
                          OPTIONAL {?language_map rml:reference ?language_value.}
                          OPTIONAL {?language_map rml:constant ?language.}
                          OPTIONAL {?language_map rml:logicalTarget ?output .
                                 ?output rml:target ?dump.
                                 ?dump void:dataDump ?language_dump.}
                         }
                OPTIONAL {?_object_map rml:datatypeMap ?datatype_map.
                          OPTIONAL {?datatype_map rml:template ?datatype_value.}
                          OPTIONAL {?datatype_map rml:constant ?object_datatype.}
                          OPTIONAL {?datatype_map rml:logicalTarget ?output .
                                 ?output rml:target ?dump.
                                 ?dump void:dataDump ?datatype_dump.}
                         }
                OPTIONAL {?_object_map rml:termType ?term .}
                OPTIONAL {
                    ?_object_map rml:datatype ?object_datatype .
                }
            }
            OPTIONAL {
                ?_predicate_object_map rml:objectMap ?_object_map .
                ?_object_map rml:parentTriplesMap ?object_parent_triples_map .
                OPTIONAL {
                    ?_object_map rml:joinCondition ?join_condition .
                    ?join_condition rml:child ?child_value;
                                 rml:parent ?parent_value.
                    OPTIONAL{?parent_value rml:functionExecution ?executed_parent .
                            ?executed_parent rml:function ?parent_function .}
                    OPTIONAL{?child_value rml:functionExecution ?executed_child .
                            ?executed_child rml:function ?child_function .}
                    OPTIONAL {?_object_map rml:termType ?term .}
                }
            }
            OPTIONAL {
                    ?_predicate_object_map rml:objectMap ?_object_map .
                    ?_object_map rml:quotedTriplesMap ?object_quoted .
                    OPTIONAL {
                        ?_object_map rml:joinCondition ?join_condition .
                        ?join_condition rml:child ?child_value;
                                     rml:parent ?parent_value.
                    }
                }
            OPTIONAL {
                ?_predicate_object_map rml:object ?object_constant_shortcut .
            }
            OPTIONAL{
                OPTIONAL {
                    ?_object_map rml:datatype ?object_datatype .
                }
                ?_object_map rml:functionExecution ?function.
                OPTIONAL {
                    ?_object_map rml:returnMap ?output_map .
                    ?output_map rml:constant ?func_output .        
                    }
                OPTIONAL { ?_object_map rml:language ?language .}
                OPTIONAL {?_object_map rml:languageMap ?language_map.
                          OPTIONAL {?language_map rml:reference ?language_value.}
                          OPTIONAL {?language_map rml:constant ?language_value.}
                          OPTIONAL {?language_map rml:logicalTarget ?language_output .
                                 ?language_output rml:target ?language_dump.
                                 ?language_dump void:dataDump ?language_dump.}
                         }
                OPTIONAL {?_object_map rml:datatypeMap ?datatype_map.
                          OPTIONAL {?datatype_map rml:template ?datatype_value.}
                          OPTIONAL {?datatype_map rml:constant ?datatype_value.}
                          OPTIONAL {?datatype_map rml:logicalTarget ?output .
                                 ?output rml:target ?dump.
                                 ?dump void:dataDump ?datatype_dump.}
                         }
                OPTIONAL {?_object_map rml:termType ?term .}
                
            }
            OPTIONAL {?_predicate_object_map rml:graph ?predicate_object_graph .}
            OPTIONAL { ?_predicate_object_map  rml:graphMap ?_graph_structure .
                       OPTIONAL {?_graph_structure rml:template ?predicate_object_graph  .}
                       OPTIONAL {?_graph_structure rml:constant ?predicate_object_graph  .} 
                       OPTIONAL {?_graph_structure rml:logicalTarget ?po_graph_output .
                                 ?po_graph_output rml:target ?po_graph_dump.
                                 ?po_graph_dump void:dataDump ?object_graph_dump.}
                     }
            OPTIONAL { ?_object_map rml:logicalTarget ?obj_output.
                        ?obj_output rml:target ?obj_dump.
                        ?obj_dump void:dataDump ?object_dump.}  
            }
            OPTIONAL {
                ?_source rml:source ?db .
                ?db a d2rq:Database;
                d2rq:jdbcDSN ?jdbcDSN; 
                d2rq:jdbcDriver ?jdbcDriver; 
                d2rq:username ?user;
                d2rq:password ?password .
            }
        } """
    else:
        mapping_query = """
    		prefix rr: <http://www.w3.org/ns/r2rml#> 
    		prefix rml: <http://semweb.mmlab.be/ns/rml#> 
    		prefix ql: <http://semweb.mmlab.be/ns/ql#> 
    		prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#>
    		prefix td: <https://www.w3.org/2019/wot/td#>
    		prefix htv: <http://www.w3.org/2011/http#>
    		prefix hctl: <https://www.w3.org/2019/wot/hypermedia#>
            prefix fnml: <http://semweb.mmlab.be/ns/fnml#>  
    		SELECT DISTINCT *
    		WHERE {

    	# Subject -------------------------------------------------------------------------
    			OPTIONAL{?triples_map_id a ?mappings_type}
                ?triples_map_id rml:logicalSource ?_source .
    			OPTIONAL{?_source rml:source ?data_source .}
    			OPTIONAL{
    				?_source rml:source ?data_link .
    				?data_link td:hasForm ?form .
    				?form hctl:hasTarget ?url_source .
    			}
    			OPTIONAL {?_source rml:referenceFormulation ?ref_form .}
    			OPTIONAL { ?_source rml:iterator ?iterator . }
    			OPTIONAL { ?_source rr:tableName ?tablename .}
    			OPTIONAL { ?_source rml:query ?query .}

    			OPTIONAL {?triples_map_id rr:subjectMap ?_subject_map .}
                OPTIONAL {?triples_map_id rml:subjectMap ?_subject_map .}
    			OPTIONAL {?_subject_map rr:template ?subject_template .}
    			OPTIONAL {?_subject_map rml:reference ?subject_reference .}
    			OPTIONAL {?_subject_map rr:constant ?subject_constant}
                OPTIONAL {?_subject_map rml:quotedTriplesMap ?subject_quoted .
                    OPTIONAL {
                            ?_subject_map rr:joinCondition ?join_condition .
                            ?join_condition rr:child ?subject_child_value;
                                         rr:parent ?subject_parent_value.
                        }
                    }
    			OPTIONAL { ?_subject_map rr:class ?rdf_class . }
    			OPTIONAL { ?_subject_map rr:termType ?termtype . }
    			OPTIONAL { ?_subject_map rr:graph ?graph . }
    			OPTIONAL { ?_subject_map rr:graphMap ?_graph_structure .
    					   ?_graph_structure rr:constant ?graph . }
    			OPTIONAL { ?_subject_map rr:graphMap ?_graph_structure .
    					   ?_graph_structure rr:template ?graph . }	
                OPTIONAL {?_subject_map fnml:functionValue ?subject_function .}  	   

    	# Predicate -----------------------------------------------------------------------
    			OPTIONAL {
    			?triples_map_id rr:predicateObjectMap ?_predicate_object_map .
                OPTIONAL {?_predicate_object_map rr:predicateMap ?_predicate_map .}
                OPTIONAL {?_predicate_object_map rml:predicateMap ?_predicate_map .}
    			OPTIONAL {
    				?_predicate_map rr:constant ?predicate_constant .
    			}
    			OPTIONAL {
    				?_predicate_map rr:template ?predicate_template .
    			}
    			OPTIONAL {
    				?_predicate_map rml:reference ?predicate_reference .
    			}
    			OPTIONAL {
    				?_predicate_object_map rr:predicate ?predicate_constant_shortcut .
    			}
                OPTIONAL {
                    ?_predicate_map fnml:functionValue ?predicate_function .
                }


    	# Object --------------------------------------------------------------------------
                OPTIONAL {?_predicate_object_map rr:objectMap ?_object_map .}
                OPTIONAL {?_predicate_object_map rml:objectMap ?_object_map .}
    			OPTIONAL {
    				?_object_map rr:constant ?object_constant .
    				OPTIONAL {
    					?_object_map rr:datatype ?object_datatype .
    				}
                    OPTIONAL { ?_object_map rr:language ?language .}
    			}
    			OPTIONAL {
    				?_object_map rr:template ?object_template .
    				OPTIONAL {?_object_map rr:termType ?term .}
    				OPTIONAL {?_object_map rml:languageMap ?language_map.
    						  ?language_map rml:reference ?language_value.}
    				OPTIONAL {
    					?_object_map rr:datatype ?object_datatype .
    				}
    			}
    			OPTIONAL {
    				?_object_map rml:reference ?object_reference .
    				OPTIONAL { ?_object_map rr:language ?language .}
    				OPTIONAL {?_object_map rml:languageMap ?language_map.
    						  ?language_map rml:reference ?language_value.}
                    OPTIONAL {?_object_map rml:datatypeMap ?datatype_map.
                          ?datatype_map rml:template ?datatype_value.}
    				OPTIONAL {?_object_map rr:termType ?term .}
    				OPTIONAL {
    					?_object_map rr:datatype ?object_datatype .
    				}
    			}
    			OPTIONAL {
                    ?_object_map rr:parentTriplesMap ?object_parent_triples_map .
                    OPTIONAL {
                        ?_object_map rr:joinCondition ?join_condition .
                        ?join_condition rr:child ?child_value;
                                     rr:parent ?parent_value.
                        OPTIONAL{?parent_value fnml:functionValue ?parent_function.}
                        OPTIONAL{?child_value fnml:functionValue ?child_function.}
                        OPTIONAL {?_object_map rr:termType ?term .}
                    }
                    OPTIONAL {
                        ?_object_map rr:joinCondition ?join_condition .
                        ?join_condition rr:child ?child_value;
                                     rr:parent ?parent_value.
                    }
                }
    			OPTIONAL {
    				?_predicate_object_map rr:object ?object_constant_shortcut .
    			}
                OPTIONAL {
                    ?_predicate_object_map rml:object ?object_constant_shortcut .
                }
                OPTIONAL {
                    ?_object_map rml:quotedTriplesMap ?object_quoted .
                    OPTIONAL {
                        ?_object_map rr:joinCondition ?join_condition .
                        ?join_condition rr:child ?child_value;
                                     rr:parent ?parent_value.
                    }
                }
                OPTIONAL {?_object_map fnml:functionValue ?function .}
    			OPTIONAL {?_predicate_object_map rr:graph ?predicate_object_graph .}
    			OPTIONAL { ?_predicate_object_map  rr:graphMap ?_graph_structure .
    					   ?_graph_structure rr:constant ?predicate_object_graph  . }
    			OPTIONAL { ?_predicate_object_map  rr:graphMap ?_graph_structure .
    					   ?_graph_structure rr:template ?predicate_object_graph  . }	
    			}
    			OPTIONAL {
                    ?_source rml:source ?db .
    				?db a d2rq:Database;
      				d2rq:jdbcDSN ?jdbcDSN; 
      				d2rq:jdbcDriver ?jdbcDriver; 
    			    d2rq:username ?user;
    			    d2rq:password ?password .
    			}
    		} """

    triples_map_list = []
    func_map_list = []
    if new_formulation == "yes":
        mapping_query_results = mapping_graph.query(function_query)
        for result_triples_map in mapping_query_results:
            if result_triples_map.function_id != None:
                func_map_exists = False
                for func_map in func_map_list:
                    func_map_exists = func_map_exists or (
                                str(func_map.func_map_id) == str(result_triples_map.function_id))
                if not func_map_exists:
                    parameters = {}
                    if result_triples_map.param != None:
                        if str(result_triples_map.param) not in parameters:
                            if result_triples_map.input_value != None:
                                parameters[str(result_triples_map.param)] = {
                                        "value":str(result_triples_map.input_value), 
                                        "type":"constant"}
                            elif result_triples_map.param_reference != None:
                                parameters[str(result_triples_map.param)] = {
                                        "value":str(result_triples_map.param_reference), 
                                        "type":"reference"}
                            elif result_triples_map.param_template != None:
                                parameters[str(result_triples_map.param)] = {
                                        "value":str(result_triples_map.param_template), 
                                        "type":"template"}
                            elif result_triples_map.param_func != None:
                                parameters[str(result_triples_map.param)] = {
                                        "value":str(result_triples_map.param_func), 
                                        "type":"function"}
                    func_map = tm.FunctionMap(str(result_triples_map.function_id),str(result_triples_map.function),parameters)
                    func_map_list.append(func_map)
                else:
                    for func_map in func_map_list:
                        if str(func_map.func_map_id) == str(result_triples_map.function_id):
                            if result_triples_map.param != None:
                                if str(result_triples_map.param) not in func_map.parameters:
                                    if result_triples_map.input_value != None:
                                        func_map.parameters[str(result_triples_map.param)] = {
                                                "value":str(result_triples_map.input_value), 
                                                "type":"constant"}
                                    elif result_triples_map.param_reference != None:
                                        func_map.parameters[str(result_triples_map.param)] = {
                                                "value":str(result_triples_map.param_reference), 
                                                "type":"reference"}
                                    elif result_triples_map.param_template != None:
                                        func_map.parameters[str(result_triples_map.param)] = {
                                                "value":str(result_triples_map.param_template), 
                                                "type":"template"}
                                    elif result_triples_map.param_func != None:
                                        func_map.parameters[str(result_triples_map.param)] = {
                                                "value":str(result_triples_map.param_func), 
                                                "type":"function"}

    mapping_query_results = mapping_graph.query(mapping_query)
    for result_triples_map in mapping_query_results:
        triples_map_exists = False
        for triples_map in triples_map_list:
            triples_map_exists = triples_map_exists or (
                        str(triples_map.triples_map_id) == str(result_triples_map.triples_map_id))
        if not triples_map_exists:
            if result_triples_map.subject_template != None:
                if result_triples_map.rdf_class is None:
                    reference, condition = string_separetion(str(result_triples_map.subject_template))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_template), condition, "template","None","None",
                                                [result_triples_map.rdf_class], result_triples_map.termtype,
                                                [result_triples_map.graph],"None")
                else:
                    reference, condition = string_separetion(str(result_triples_map.subject_template))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_template), condition, "template","None","None",
                                                [str(result_triples_map.rdf_class)], result_triples_map.termtype,
                                                [result_triples_map.graph],"None")
            elif result_triples_map.subject_reference != None:
                if result_triples_map.rdf_class is None:
                    reference, condition = string_separetion(str(result_triples_map.subject_reference))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_reference), condition, "reference","None","None",
                                                [result_triples_map.rdf_class], result_triples_map.termtype,
                                                [result_triples_map.graph],"None")
                else:
                    reference, condition = string_separetion(str(result_triples_map.subject_reference))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_reference), condition, "reference","None","None",
                                                [str(result_triples_map.rdf_class)], result_triples_map.termtype,
                                                [result_triples_map.graph],"None")
            elif result_triples_map.subject_constant != None:
                if result_triples_map.rdf_class is None:
                    reference, condition = string_separetion(str(result_triples_map.subject_constant))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_constant), condition, "constant","None","None",
                                                [result_triples_map.rdf_class], result_triples_map.termtype,
                                                [result_triples_map.graph],"None")
                else:
                    reference, condition = string_separetion(str(result_triples_map.subject_constant))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_constant), condition, "constant","None","None",
                                                [str(result_triples_map.rdf_class)], result_triples_map.termtype,
                                                [result_triples_map.graph],"None")
            elif result_triples_map.subject_function != None:
                func_output = "None"
                if result_triples_map.subject_output != None:
                    if "#" in result_triples_map.subject_output:
                        func_output = result_triples_map.subject_output.split("#")[1]
                    else:
                        func_output = result_triples_map.subject_output.split("/")[len(result_triples_map.subject_output.split("/"))-1]
                if result_triples_map.rdf_class is None:
                    reference, condition = string_separetion(str(result_triples_map.subject_constant))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_function), condition, "function","None","None", 
                                                [str(result_triples_map.rdf_class)], result_triples_map.termtype, 
                                                [result_triples_map.graph],func_output)
                else:
                    reference, condition = string_separetion(str(result_triples_map.subject_constant))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_function), condition, "function","None","None",
                                                [str(result_triples_map.rdf_class)], result_triples_map.termtype, 
                                                [result_triples_map.graph],func_output)
            elif result_triples_map.subject_quoted != None:
                if result_triples_map.rdf_class is None:
                    reference, condition = string_separetion(str(result_triples_map.subject_quoted))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_quoted), condition, "quoted triples map", 
                                                result_triples_map.subject_parent_value, result_triples_map.subject_child_value, 
                                                [result_triples_map.rdf_class], result_triples_map.termtype, 
                                                [result_triples_map.graph],"None")
                else:
                    reference, condition = string_separetion(str(result_triples_map.subject_quoted))
                    subject_map = tm.SubjectMap(str(result_triples_map.subject_quoted), condition, "quoted triples map", 
                                                result_triples_map.subject_parent_value, result_triples_map.subject_child_value, 
                                                [str(result_triples_map.rdf_class)], result_triples_map.termtype, 
                                                [result_triples_map.graph],"None")

            if new_formulation == "yes":
                output_file = ""
                if result_triples_map.subject_dump != None:
                    output_file = result_triples_map.subject_dump[7:] if result_triples_map.subject_dump[:7] == "file://" else result_triples_map.subject_dump  
                elif result_triples_map.subject_graph_dump != None:
                    output_file = result_triples_map.subject_graph_dump[7:] if result_triples_map.subject_graph_dump[:7] == "file://" else result_triples_map.subject_graph_dump
                if output_file != "":
                    if str(result_triples_map.triples_map_id) not in logical_dump:
                        logical_dump[str(result_triples_map.triples_map_id)] = {output_file:"subject"}
                    else:
                        if output_file not in logical_dump[str(result_triples_map.triples_map_id)]:
                            logical_dump[str(result_triples_map.triples_map_id)][output_file] = "subject"

            mapping_query_prepared = prepareQuery(mapping_query)

            mapping_query_prepared_results = mapping_graph.query(mapping_query_prepared, initBindings={
                'triples_map_id': result_triples_map.triples_map_id})

            join_predicate = {}
            predicate_object_maps_list = []
            predicate_object_graph = {}

            function = False
            for result_predicate_object_map in mapping_query_prepared_results:
                join = True
                if result_predicate_object_map.predicate_constant_shortcut != None:
                    predicate_map = tm.PredicateMap("constant shortcut",
                                                    str(result_predicate_object_map.predicate_constant_shortcut), "", "None")
                    predicate_object_graph[
                        str(result_predicate_object_map.predicate_constant_shortcut)] = result_triples_map.predicate_object_graph
                elif result_predicate_object_map.predicate_constant != None:
                    predicate_map = tm.PredicateMap("constant", str(result_predicate_object_map.predicate_constant), "", "None")
                    predicate_object_graph[
                        str(result_predicate_object_map.predicate_constant)] = result_triples_map.predicate_object_graph
                elif result_predicate_object_map.predicate_template != None:
                    template, condition = string_separetion(str(result_predicate_object_map.predicate_template))
                    predicate_map = tm.PredicateMap("template", template, condition, "None")
                elif result_predicate_object_map.predicate_reference != None:
                    reference, condition = string_separetion(str(result_predicate_object_map.predicate_reference))
                    predicate_map = tm.PredicateMap("reference", reference, condition, "None")
                elif result_predicate_object_map.predicate_function != None:
                    func_output = "None"
                    if result_predicate_object_map.predicate_output != None:
                        if "#" in result_predicate_object_map.predicate_output:
                            func_output = result_predicate_object_map.predicate_output.split("#")[1]
                        else:
                            func_output = result_predicate_object_map.predicate_output.split("/")[len(result_predicate_object_map.predicate_output.split("/"))-1]
                    predicate_map = tm.PredicateMap("function", str(result_predicate_object_map.predicate_function),"",func_output)
                else:
                    predicate_map = tm.PredicateMap("None", "None", "None", "None")

                if new_formulation == "yes":
                    if result_predicate_object_map.predicate_dump != None:
                        output_file = result_predicate_object_map.predicate_dump[7:] if result_predicate_object_map.predicate_dump[:7] == "file://" else result_predicate_object_map.predicate_dump 
                        if str(result_triples_map.triples_map_id) not in logical_dump:
                            logical_dump[str(result_triples_map.triples_map_id)] = {output_file:[predicate_map.value]}
                        else:
                            if output_file not in logical_dump[str(result_triples_map.triples_map_id)]:
                                logical_dump[str(result_triples_map.triples_map_id)][output_file] = [predicate_map.value]
                            else:
                                if predicate_map.value not in logical_dump[str(result_triples_map.triples_map_id)][output_file]:
                                    logical_dump[str(result_triples_map.triples_map_id)][output_file].append(predicate_map.value)
                
                if "execute" in predicate_map.value:
                    function = True

                if result_predicate_object_map.object_constant != None:
                    object_map = tm.ObjectMap("constant", str(result_predicate_object_map.object_constant),
                                              str(result_predicate_object_map.object_datatype), "None", "None",
                                              result_predicate_object_map.term, result_predicate_object_map.language,
                                              result_predicate_object_map.language_value,
                                              result_predicate_object_map.datatype_value, "None")
                elif result_predicate_object_map.object_template != None:
                    object_map = tm.ObjectMap("template", str(result_predicate_object_map.object_template),
                                              str(result_predicate_object_map.object_datatype), "None", "None",
                                              result_predicate_object_map.term, result_predicate_object_map.language,
                                              result_predicate_object_map.language_value,
                                              result_predicate_object_map.datatype_value, "None")
                elif result_predicate_object_map.object_reference != None:
                    object_map = tm.ObjectMap("reference", str(result_predicate_object_map.object_reference),
                                              str(result_predicate_object_map.object_datatype), "None", "None",
                                              result_predicate_object_map.term, result_predicate_object_map.language,
                                              result_predicate_object_map.language_value,
                                              result_predicate_object_map.datatype_value, "None")
                elif result_predicate_object_map.object_parent_triples_map != None:
                    if predicate_map.value + " " + str(result_predicate_object_map.object_parent_triples_map) not in join_predicate:
                        if (result_predicate_object_map.child_function is not None) and (result_predicate_object_map.parent_function is not None):
                            join_predicate[
                                predicate_map.value + " " + str(result_predicate_object_map.object_parent_triples_map)] = {
                                "predicate":predicate_map, 
                                "childs":[str(result_predicate_object_map.child_value)], 
                                "parents":[str(result_predicate_object_map.parent_function)], 
                                "triples_map":str(result_predicate_object_map.object_parent_triples_map)}
                        elif (result_predicate_object_map.child_function is not None) and (result_predicate_object_map.parent_function is None):
                            join_predicate[
                                predicate_map.value + " " + str(result_predicate_object_map.object_parent_triples_map)] = {
                                "predicate":predicate_map, 
                                "childs":[str(result_predicate_object_map.child_function)], 
                                "parents":[str(result_predicate_object_map.parent_value)], 
                                "triples_map":str(result_predicate_object_map.object_parent_triples_map)}
                        elif (result_predicate_object_map.child_function is None) and (result_predicate_object_map.parent_function is not None):
                            join_predicate[
                                predicate_map.value + " " + str(result_predicate_object_map.object_parent_triples_map)] = {
                                "predicate":predicate_map, 
                                "childs":[str(result_predicate_object_map.child_function)], 
                                "parents":[str(result_predicate_object_map.parent_function)], 
                                "triples_map":str(result_predicate_object_map.object_parent_triples_map)}
                        else:
                            join_predicate[
                                predicate_map.value + " " + str(result_predicate_object_map.object_parent_triples_map)] = {
                                "predicate":predicate_map, 
                                "childs":[str(result_predicate_object_map.child_value)], 
                                "parents":[str(result_predicate_object_map.parent_value)], 
                                "triples_map":str(result_predicate_object_map.object_parent_triples_map)}
                    else:
                        if (result_predicate_object_map.child_function is None) and (result_predicate_object_map.parent_function is not None):
                            join_predicate[
                                predicate_map.value + " " + str(result_predicate_object_map.object_parent_triples_map)][
                                "childs"].append(str(result_predicate_object_map.child_function))
                            join_predicate[
                                predicate_map.value + " " + str(result_predicate_object_map.object_parent_triples_map)][
                                "parents"].append(str(result_predicate_object_map.parent_value))
                        elif (result_predicate_object_map.child_function is not None) and (result_predicate_object_map.parent_function is None):
                            join_predicate[
                                predicate_map.value + " " + str(result_predicate_object_map.object_parent_triples_map)][
                                "childs"].append(str(result_predicate_object_map.child_function))
                            join_predicate[
                                predicate_map.value + " " + str(result_predicate_object_map.object_parent_triples_map)][
                                "parents"].append(str(result_predicate_object_map.parent_value))
                        elif (result_predicate_object_map.child_function is not None) and (result_predicate_object_map.parent_function is not None):
                            join_predicate[
                                predicate_map.value + " " + str(result_predicate_object_map.object_parent_triples_map)][
                                "childs"].append(str(result_predicate_object_map.child_function))
                            join_predicate[
                                predicate_map.value + " " + str(result_predicate_object_map.object_parent_triples_map)][
                                "parents"].append(str(result_predicate_object_map.parent_function))
                        else:
                            join_predicate[
                                predicate_map.value + " " + str(result_predicate_object_map.object_parent_triples_map)][
                                "childs"].append(str(result_predicate_object_map.child_value))
                            join_predicate[
                                predicate_map.value + " " + str(result_predicate_object_map.object_parent_triples_map)][
                                "parents"].append(str(result_predicate_object_map.parent_value))
                    join = False
                elif result_predicate_object_map.function is not None:
                    func_output = "None"
                    if result_predicate_object_map.func_output != None:
                        if "#" in result_predicate_object_map.func_output:
                            func_output = result_predicate_object_map.func_output.split("#")[1]
                        else:
                            func_output = result_predicate_object_map.func_output.split("/")[len(result_predicate_object_map.func_output.split("/"))-1]
                    object_map = tm.ObjectMap("reference function", str(result_predicate_object_map.function),
                                            str(result_predicate_object_map.object_datatype), "None", "None", 
                                            result_predicate_object_map.term, result_predicate_object_map.language,
                                            result_predicate_object_map.language_value,
                                            result_predicate_object_map.datatype_value, func_output)
                elif result_predicate_object_map.object_quoted != None:
                    object_map = tm.ObjectMap("quoted triples map", str(result_predicate_object_map.object_quoted), 
                                            str(result_predicate_object_map.object_datatype), 
                                            [str(result_predicate_object_map.child_value)], [str(result_predicate_object_map.parent_value)], 
                                            result_predicate_object_map.term, result_predicate_object_map.language,
                                            result_predicate_object_map.language_value,
                                            result_predicate_object_map.datatype_value, "None")
                elif result_predicate_object_map.object_constant_shortcut != None:
                    object_map = tm.ObjectMap("constant shortcut",
                                              str(result_predicate_object_map.object_constant_shortcut), "None", "None",
                                              "None", result_predicate_object_map.term,
                                              result_predicate_object_map.language,
                                              result_predicate_object_map.language_value,
                                              result_predicate_object_map.datatype_value, "None")
                else:
                    object_map = tm.ObjectMap("None", "None", "None", "None", "None", "None", "None", "None", "None", "None")

                if new_formulation == "yes":
                    output_file = ""
                    if result_predicate_object_map.object_dump != None:
                        output_file = result_predicate_object_map.object_dump[7:] if result_predicate_object_map.object_dump[:7] == "file://" else result_predicate_object_map.object_dump  
                    elif result_predicate_object_map.language_dump != None:
                        output_file = result_predicate_object_map.language_dump[7:] if result_predicate_object_map.language_dump[:7] == "file://" else result_predicate_object_map.language_dump  
                    elif result_predicate_object_map.datatype_dump != None:
                        output_file = result_predicate_object_map.datatype_dump[7:] if result_predicate_object_map.datatype_dump[:7] == "file://" else result_predicate_object_map.datatype_dump
                    if output_file != "":
                        if str(result_triples_map.triples_map_id) not in logical_dump:
                            logical_dump[str(result_triples_map.triples_map_id)] = {output_file:[object_map.value]}
                        else:
                            if output_file not in logical_dump[str(result_triples_map.triples_map_id)]:
                                if result_predicate_object_map.language_dump != None:
                                    if result_predicate_object_map.language != None:
                                        logical_dump[str(result_triples_map.triples_map_id)][output_file] = [object_map.value + "_" + result_predicate_object_map.language]
                                    elif result_predicate_object_map.language_value != None:
                                        logical_dump[str(result_triples_map.triples_map_id)][output_file] = [object_map.value + "_" + result_predicate_object_map.language_value]
                                elif result_predicate_object_map.datatype_dump != None:
                                    if result_predicate_object_map.object_datatype != None:
                                        logical_dump[str(result_triples_map.triples_map_id)][output_file] = [str(object_map.value + "_" + result_predicate_object_map.object_datatype)]
                                    elif result_predicate_object_map.datatype_value != None:
                                        logical_dump[str(result_triples_map.triples_map_id)][output_file] = [str(object_map.value + "_" + result_predicate_object_map.datatype_value)]
                                else:
                                    logical_dump[str(result_triples_map.triples_map_id)][output_file] = [object_map.value]
                            else:
                                if result_predicate_object_map.language_dump != None:
                                    if result_predicate_object_map.language != None:
                                        if result_predicate_object_map.language_value not in logical_dump[str(result_triples_map.triples_map_id)][output_file]:
                                            logical_dump[str(result_triples_map.triples_map_id)][output_file].append(object_map.value + "_" + result_predicate_object_map.language)
                                    elif result_predicate_object_map.language_value != None:
                                        if result_predicate_object_map.language_value not in logical_dump[str(result_triples_map.triples_map_id)][output_file]:
                                            logical_dump[str(result_triples_map.triples_map_id)][output_file].append(object_map.value + "_" + result_predicate_object_map.language_value)
                                elif result_predicate_object_map.datatype_dump != None:
                                    if result_predicate_object_map.object_datatype != None:
                                        if str(object_map.value + "_" + result_predicate_object_map.object_datatype) not in logical_dump[str(result_triples_map.triples_map_id)][output_file]:
                                            logical_dump[str(result_triples_map.triples_map_id)][output_file].append(str(object_map.value + "_" + result_predicate_object_map.object_datatype))
                                    elif result_predicate_object_map.datatype_value != None:
                                        if str(object_map.value + "_" + result_predicate_object_map.datatype_value) not in logical_dump[str(result_triples_map.triples_map_id)][output_file]:
                                            logical_dump[str(result_triples_map.triples_map_id)][output_file].append(str(object_map.value + "_" + result_predicate_object_map.datatype_value))
                                else:
                                    if object_map.value not in logical_dump[str(result_triples_map.triples_map_id)][output_file]:
                                        logical_dump[str(result_triples_map.triples_map_id)][output_file].append(object_map.value)
                    if result_predicate_object_map.object_graph_dump != None:
                        output_file = result_predicate_object_map.object_graph_dump[7:] if result_predicate_object_map.object_graph_dump[:7] == "file://" else result_predicate_object_map.object_graph_dump
                    if output_file != "":
                        if str(result_triples_map.triples_map_id) not in logical_dump:
                            logical_dump[str(result_triples_map.triples_map_id)] = {output_file:[object_map.value]}
                        else:
                            if output_file not in logical_dump[str(result_triples_map.triples_map_id)]:
                                logical_dump[str(result_triples_map.triples_map_id)][output_file] = [object_map.value]
                            else:
                                if object_map.value not in logical_dump[str(result_triples_map.triples_map_id)][output_file]:
                                    logical_dump[str(result_triples_map.triples_map_id)][output_file].append(object_map.value)
                                
                if join:
                    predicate_object_maps_list += [
                        tm.PredicateObjectMap(predicate_map, object_map, predicate_object_graph)]
                join = True
            if join_predicate:
                for jp in join_predicate.keys():
                    object_map = tm.ObjectMap("parent triples map", join_predicate[jp]["triples_map"],
                                              str(result_predicate_object_map.object_datatype),
                                              join_predicate[jp]["childs"], join_predicate[jp]["parents"],
                                              result_predicate_object_map.term, result_predicate_object_map.language,
                                              result_predicate_object_map.language_value,
                                              result_predicate_object_map.datatype_value, "None")
                    predicate_object_maps_list += [
                        tm.PredicateObjectMap(join_predicate[jp]["predicate"], object_map, predicate_object_graph)]
            if result_triples_map.url_source is not None:
                if result_triples_map.delimiter is not None:
                    url_source = str(result_triples_map.url_source)[7:] if str(result_triples_map.url_source)[:7] == "file://" else str(result_triples_map.url_source)
                    delimiter[url_source] = str(result_triples_map.delimiter)
                if ".xml" in str(result_triples_map.url_source) and str(result_triples_map.ref_form) != "http://w3id.org/rml/XPath":
                    current_triples_map = tm.TriplesMap(str(result_triples_map.triples_map_id),
                                                        str(result_triples_map.url_source), subject_map,
                                                        predicate_object_maps_list,
                                                        ref_form="http://w3id.org/rml/XPath",
                                                        iterator=str(result_triples_map.iterator),
                                                        tablename=str(result_triples_map.tablename),
                                                        query=str(result_triples_map.query),
                                                        function=function,func_map_list=func_map_list, 
                                                        mappings_type=str(result_triples_map.mappings_type))
                else:
                    current_triples_map = tm.TriplesMap(str(result_triples_map.triples_map_id),
                                                        str(result_triples_map.url_source), subject_map,
                                                        predicate_object_maps_list,
                                                        ref_form=str(result_triples_map.ref_form),
                                                        iterator=str(result_triples_map.iterator),
                                                        tablename=str(result_triples_map.tablename),
                                                        query=str(result_triples_map.query),
                                                        function=function,func_map_list=func_map_list, 
                                                        mappings_type=str(result_triples_map.mappings_type))
            else:
                current_triples_map = tm.TriplesMap(str(result_triples_map.triples_map_id),
                                                    str(result_triples_map.data_source), subject_map,
                                                    predicate_object_maps_list,
                                                    ref_form=str(result_triples_map.ref_form),
                                                    iterator=str(result_triples_map.iterator),
                                                    tablename=str(result_triples_map.tablename),
                                                    query=str(result_triples_map.query),
                                                    function=function,func_map_list=func_map_list, 
                                                    mappings_type=str(result_triples_map.mappings_type))

            triples_map_list += [current_triples_map]

        else:
            for triples_map in triples_map_list:
                if str(triples_map.triples_map_id) == str(result_triples_map.triples_map_id):
                    if str(result_triples_map.rdf_class) not in triples_map.subject_map.rdf_class:
                        triples_map.subject_map.rdf_class.append(str(result_triples_map.rdf_class))
                    if result_triples_map.graph not in triples_map.subject_map.graph:
                        triples_map.graph.append(result_triples_map.graph)

                    if new_formulation == "yes":
                        output_file = ""
                        if result_triples_map.subject_dump != None:
                            output_file = result_triples_map.subject_dump[7:] if result_triples_map.subject_dump[:7] == "file://" else result_triples_map.subject_dump  
                        elif result_triples_map.subject_graph_dump != None:
                            output_file = result_triples_map.subject_graph_dump[7:] if result_triples_map.subject_graph_dump[:7] == "file://" else result_triples_map.subject_graph_dump   
                        if output_file != "":
                            if str(result_triples_map.triples_map_id) not in logical_dump:
                                logical_dump[str(result_triples_map.triples_map_id)] = {output_file:"subject"}
                            else:
                                if output_file not in logical_dump[str(result_triples_map.triples_map_id)]:
                                    logical_dump[str(result_triples_map.triples_map_id)][output_file] = "subject"

                    if result_triples_map.predicate_constant_shortcut != None:
                        for po in triples_map.predicate_object_maps_list:
                            if po.predicate_map.value == str(result_triples_map.predicate_constant_shortcut):
                                if str(result_triples_map.predicate_constant_shortcut) in po.graph:
                                    po.graph[str(result_triples_map.predicate_constant_shortcut)] = result_triples_map.predicate_object_graph

                    if new_formulation == "yes":
                        output_file = ""
                        if result_triples_map.predicate_dump != None:
                            if result_triples_map.predicate_constant != None:
                                value = result_triples_map.predicate_constant
                            elif result_triples_map.predicate_template != None:
                                value = result_triples_map.predicate_template
                            elif result_triples_map.predicate_reference != None:
                                value = result_triples_map.predicate_reference
                            output_file = result_triples_map.predicate_dump[7:] if result_triples_map.predicate_dump[:7] == "file://" else result_triples_map.predicate_dump 
                            
                            if str(result_triples_map.triples_map_id) not in logical_dump:
                                logical_dump[str(result_triples_map.triples_map_id)] = {output_file:value}
                            else:
                                if output_file not in logical_dump[str(result_triples_map.triples_map_id)]:
                                    logical_dump[str(result_triples_map.triples_map_id)][output_file] = value

                        output_file = ""
                        if result_triples_map.object_dump != None:
                            output_file = result_triples_map.object_dump[7:] if result_triples_map.object_dump[:7] == "file://" else result_triples_map.object_dump  
                        elif result_triples_map.object_graph_dump != None:
                            output_file = result_triples_map.object_graph_dump[7:] if result_triples_map.object_graph_dump[:7] == "file://" else result_triples_map.object_graph_dump
                        elif result_triples_map.language_dump != None:
                            output_file = result_triples_map.language_dump[7:] if result_triples_map.language_dump[:7] == "file://" else result_triples_map.language_dump  
                        elif result_triples_map.datatype_dump != None:
                            output_file = result_triples_map.datatype_dump[7:] if result_triples_map.datatype_dump[:7] == "file://" else result_triples_map.datatype_dump    
                        if output_file != "":
                            if result_triples_map.object_constant != None:
                                value = result_triples_map.object_constant
                            elif result_triples_map.object_reference != None:
                                value = result_triples_map.object_reference
                            elif result_triples_map.object_template != None:
                                value = result_triples_map.object_template
                            elif result_triples_map.object_parent_triples_map != None:
                                value = result_triples_map.object_parent_triples_map   
                            if str(result_triples_map.triples_map_id) not in logical_dump:
                                logical_dump[str(result_triples_map.triples_map_id)] = {output_file:value}
                            else:
                                if output_file not in logical_dump[str(result_triples_map.triples_map_id)]:
                                    logical_dump[str(result_triples_map.triples_map_id)][output_file] = value

    return mappings_expansion(triples_map_list)


def semantify_xml(triples_map, triples_map_list, output_file_descriptor):
    print("TM: " + triples_map.triples_map_name)
    logger.info("TM: " + triples_map.triples_map_name)
    i = 0
    triples_map_triples = {}
    generated_triples = {}
    object_list = []
    global blank_message
    global host, port, user, password, datab
    if "http" in triples_map.data_source:
        response = requests.get(triples_map.data_source, stream=True)
        root = ET.fromstring(response.content)
    else:
        tree = ET.parse(triples_map.data_source)
        root = tree.getroot()
    if "[" not in triples_map.iterator:
        level = triples_map.iterator.split("/")[len(triples_map.iterator.split("/")) - 1]
        if level == "":
            i = 1
            while i < len(triples_map.iterator.split("/")) - 1:
                level = triples_map.iterator.split("/")[len(triples_map.iterator.split("/")) - i]
                if level != "":
                    break
                i += 1
    else:
        temp = triples_map.iterator.split("[")[0]
        level = temp.split("/")[len(temp.split("/")) - 1]
        if level == "":
            i = 1
            while i < len(temp.split("/")) - 1:
                level = temp.split("/")[len(temp.split("/")) - i]
                if level != "":
                    break
                i += 1
    parent_map = {c: p for p in root.iter() for c in p}
    if "http" in triples_map.data_source:
        namespace = {}
        for elem in root.iter():
            namespace_uri = elem.tag.split('}')[0][1:]
            if namespace_uri and ':' in elem.tag:
                prefix = elem.tag.split(':')[0]
                namespace[prefix] = namespace_uri
    else:
        namespace = dict([node for _, node in ET.iterparse(str(triples_map.data_source), events=['start-ns'])])
    if namespace:
        for name in namespace:
            ET.register_namespace(name, namespace[name])
    if "/" in triples_map.iterator:
        parent_level = 2
        while len(list(root.iterfind(level, namespace))) == 0:
            if triples_map.iterator != level:
                level = triples_map.iterator.split("/")[len(triples_map.iterator.split("/")) - parent_level] + "/" + level
                parent_level += 1
            else:
                break
    else:
        level = "."
    if mapping_partitions == "yes":
        if triples_map.predicate_object_maps_list[0].predicate_map.mapping_type == "constant" or \
                triples_map.predicate_object_maps_list[0].predicate_map.mapping_type == "constant shortcut":
            predicate = "<" + triples_map.predicate_object_maps_list[0].predicate_map.value + ">"
            constant_predicate = False
        else:
            predicate = None
            constant_predicate = True
    else:
        predicate = None
        constant_predicate = True
    for child in root.iterfind(level, namespace):
        create_subject = True
        global generated_subjects

        if mapping_partitions == "yes":
            if "_" in triples_map.triples_map_id:
                componets = triples_map.triples_map_id.split("_")[:-1]
                triples_map_id = ""
                for name in componets:
                    triples_map_id += name + "_"
                triples_map_id = triples_map_id[:-1]
            else:
                triples_map_id = triples_map.triples_map_id

            subject_attr = extract_subject_values(child, generated_subjects[triples_map_id]["subject_attr"], "XML",
                                                  parent_map)

            if subject_attr == None:
                subject = None
                create_subject = False
            else:
                if triples_map_id in generated_subjects:
                    if subject_attr in generated_subjects[triples_map_id]:
                        subject = generated_subjects[triples_map_id][subject_attr]
                        create_subject = False

        if create_subject:
            subject_value = string_substitution_xml(triples_map.subject_map.value, "{(.+?)}", child, "subject",
                                                    triples_map.iterator, parent_map, namespace)
            if triples_map.subject_map.subject_mapping_type == "template":
                if triples_map.subject_map.term_type is None:
                    if triples_map.subject_map.condition == "":

                        try:
                            subject = "<" + subject_value + ">"
                        except:
                            subject = None

                    else:
                        #	field, condition = condition_separetor(triples_map.subject_map.condition)
                        #	if row[field] == condition:
                        try:
                            subject = "<" + subject_value + ">"
                        except:
                            subject = None
                else:
                    if "IRI" in triples_map.subject_map.term_type:
                        if triples_map.subject_map.condition == "":

                            try:
                                subject = "<" + base + subject_value + ">"
                            except:
                                subject = None

                        else:
                            #	field, condition = condition_separetor(triples_map.subject_map.condition)
                            #	if row[field] == condition:
                            try:
                                if "http" not in subject_value:
                                    if base != "":
                                        subject = "<" + base + subject_value + ">"
                                    else:
                                        subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                                else:
                                    if is_valid_url_syntax(subject_value):
                                        subject = "<" + subject_value + ">"
                                    else:
                                        if base != "":
                                            subject = "<" + base + subject_value + ">"
                                        else:
                                            subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                            except:
                                subject = None

                    elif "BlankNode" in triples_map.subject_map.term_type:
                        if triples_map.subject_map.condition == "":

                            try:
                                if "/" in subject_value:
                                    subject = "_:" + encode_char(subject_value.replace("/", "2F")).replace("%", "")
                                    if blank_message:
                                        logger.warning(
                                            "Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                                        blank_message = False
                                else:
                                    subject = "_:" + encode_char(subject_value).replace("%", "")
                                if "." in subject:
                                    subject = subject.replace(".", "2E")
                            except:
                                subject = None

                        else:
                            #	field, condition = condition_separetor(triples_map.subject_map.condition)
                            #	if row[field] == condition:
                            try:
                                subject = "_:" + subject_value
                            except:
                                subject = None

                    elif "Literal" in triples_map.subject_map.term_type:
                        subject = None

                    else:
                        if triples_map.subject_map.condition == "":

                            try:
                                subject = "<" + subject_value + ">"
                            except:
                                subject = None

                        else:
                            #	field, condition = condition_separetor(triples_map.subject_map.condition)
                            #	if row[field] == condition:
                            try:
                                subject = "<" + subject_value + ">"
                            except:
                                subject = None

            elif "reference" in triples_map.subject_map.subject_mapping_type:
                if triples_map.subject_map.condition == "":
                    subject_value = string_substitution_xml(triples_map.subject_map.value, ".+", child, "subject",
                                                            triples_map.iterator, parent_map, namespace)
                    subject_value = subject_value[0][1:-1]
                    try:
                        if " " not in subject_value:
                            if "http" not in subject_value:
                                if base != "":
                                    subject = "<" + base + subject_value + ">"
                                else:
                                    subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                            else:
                                if is_valid_url_syntax(subject_value):
                                    subject = "<" + subject_value + ">"
                                else:
                                    if base != "":
                                        subject = "<" + base + subject_value + ">"
                                    else:
                                        subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                        else:
                            logger.error("<http://example.com/base/" + subject_value + "> is an invalid URL")
                            subject = None
                    except:
                        subject = None
                    if triples_map.subject_map.term_type == "IRI":
                        if " " not in subject_value:
                            subject = "<" + encode_char(subject_value) + ">"
                        else:
                            subject = None

                else:
                    #	field, condition = condition_separetor(triples_map.subject_map.condition)
                    #	if row[field] == condition:
                    try:
                        if "http" not in subject_value:
                            if base != "":
                                subject = "<" + base + subject_value + ">"
                            else:
                                subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                        else:
                            if is_valid_url_syntax(subject_value):
                                subject = "<" + subject_value + ">"
                            else:
                                if base != "":
                                    subject = "<" + base + subject_value + ">"
                                else:
                                    subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                    except:
                        subject = None

            elif "constant" in triples_map.subject_map.subject_mapping_type:
                subject = "<" + triples_map.subject_map.value + ">"

            else:
                if triples_map.subject_map.condition == "":

                    try:
                        subject = "\"" + triples_map.subject_map.value + "\""
                    except:
                        subject = None

                else:
                    #	field, condition = condition_separetor(triples_map.subject_map.condition)
                    #	if row[field] == condition:
                    try:
                        subject = "\"" + triples_map.subject_map.value + "\""
                    except:
                        subject = None

            if mapping_partitions == "yes":
                if triples_map_id in generated_subjects:
                    if subject_attr in generated_subjects[triples_map_id]:
                        pass
                    else:
                        generated_subjects[triples_map_id][subject_attr] = subject
                else:
                    generated_subjects[triples_map_id] = {subject_attr: subject}

        if triples_map.subject_map.rdf_class != [None] and subject != None:
            predicate = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
            for rdf_class in triples_map.subject_map.rdf_class:
                if rdf_class != None and ("str" == type(rdf_class).__name__ or "URIRef" == type(rdf_class).__name__):
                    obj = "<{}>".format(rdf_class)
                    dictionary_table_update(subject)
                    dictionary_table_update(obj)
                    dictionary_table_update(predicate + "_" + obj)
                    rdf_type = subject + " " + predicate + " " + obj + ".\n"
                    for graph in triples_map.subject_map.graph:
                        if graph != None and "defaultGraph" not in graph:
                            if "{" in graph:
                                rdf_type = rdf_type[:-2] + " <" + string_substitution_xml(graph, "{(.+?)}", child,
																						  "subject",
																						  triples_map.iterator,
																						  parent_map,
																						  namespace) + ">.\n"
                                dictionary_table_update(
                                    "<" + string_substitution_xml(graph, "{(.+?)}", child, "subject",
                                                                  triples_map.iterator, parent_map,
                                                                  namespace) + ">")
                            else:
                                rdf_type = rdf_type[:-2] + " <" + graph + ">.\n"
                                dictionary_table_update("<" + graph + ">")
                        if duplicate == "yes":
                            if dic_table[predicate + "_" + obj] not in g_triples:
                                output_file_descriptor.write(rdf_type)
                                g_triples.update({dic_table[predicate + "_" + obj]: {
                                    dic_table[subject] + "_" + dic_table[obj]: ""}})
                                i += 1
                            elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                dic_table[predicate + "_" + obj]]:
                                output_file_descriptor.write(rdf_type)
                                g_triples[dic_table[predicate + "_" + obj]].update(
                                    {dic_table[subject] + "_" + dic_table[obj]: ""})
                                i += 1
                        else:
                            output_file_descriptor.write(rdf_type)
                            i += 1
        for predicate_object_map in triples_map.predicate_object_maps_list:
            if constant_predicate:
                if predicate_object_map.predicate_map.mapping_type == "constant" or predicate_object_map.predicate_map.mapping_type == "constant shortcut":
                    predicate = "<" + predicate_object_map.predicate_map.value + ">"
                elif predicate_object_map.predicate_map.mapping_type == "template":
                    if predicate_object_map.predicate_map.condition != "":
                        # field, condition = condition_separetor(predicate_object_map.predicate_map.condition)
                        # if row[field] == condition:
                        try:
                            predicate = "<" + string_substitution_xml(predicate_object_map.predicate_map.value,
                                                                      "{(.+?)}", child, "predicate",
                                                                      triples_map.iterator, parent_map,
                                                                      namespace) + ">"
                        except:
                            predicate = None
                    # else:
                    #	predicate = None
                    else:
                        try:
                            predicate = "<" + string_substitution_xml(predicate_object_map.predicate_map.value,
                                                                      "{(.+?)}", child, "predicate",
                                                                      triples_map.iterator, parent_map,
                                                                      namespace) + ">"
                        except:
                            predicate = None
                elif predicate_object_map.predicate_map.mapping_type == "reference":
                    if predicate_object_map.predicate_map.condition != "":
                        # field, condition = condition_separetor(predicate_object_map.predicate_map.condition)
                        # if row[field] == condition:
                        predicate = string_substitution_xml(predicate_object_map.predicate_map.value, ".+", child,
                                                            "predicate", triples_map.iterator, parent_map,
                                                            namespace)
                    # else:
                    #	predicate = None
                    else:
                        predicate = string_substitution_xml(predicate_object_map.predicate_map.value, ".+", child,
                                                            "predicate", triples_map.iterator, parent_map,
                                                            namespace)
                    predicate = "<" + predicate[1:-1] + ">"
                else:
                    predicate = None

            if predicate_object_map.object_map.mapping_type == "constant" or predicate_object_map.object_map.mapping_type == "constant shortcut":
                if "/" in predicate_object_map.object_map.value:
                    object = "<" + predicate_object_map.object_map.value + ">"
                else:
                    object = "\"" + predicate_object_map.object_map.value + "\""
                if predicate_object_map.object_map.datatype != None:
                    object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
                elif predicate_object_map.object_map.datatype_map != None:
                    datatype_value = string_substitution_xml(predicate_object_map.object_map.datatype_map, "{(.+?)}", child,
                                                        "object", triples_map.iterator, parent_map, namespace)
                    if "http" in datatype_value:
                       object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(datatype_value)
                    else:
                       object = "\"" + object[1:-1] + "\"" + "^^<{}>".format("http://example.com/base/" + datatype_value)
                elif predicate_object_map.object_map.language != None:
                    if "spanish" == predicate_object_map.object_map.language or "es" == predicate_object_map.object_map.language:
                        object += "@es"
                    elif "english" == predicate_object_map.object_map.language or "en" == predicate_object_map.object_map.language:
                        object += "@en"
                    elif len(predicate_object_map.object_map.language) == 2:
                        object += "@" + predicate_object_map.object_map.language
                    else:
                        object = None
                elif predicate_object_map.object_map.language_map != None:
                    lang = string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object",
                                               ignore, triples_map.iterator)
                    if lang != None:
                        object += "@" + string_substitution(predicate_object_map.object_map.language_map, ".+", row,
                                                            "object", ignore, triples_map.iterator)[1:-1]
            elif predicate_object_map.object_map.mapping_type == "template":
                object = string_substitution_xml(predicate_object_map.object_map.value, "{(.+?)}", child, "object",
                                                 triples_map.iterator, parent_map, namespace)
                if isinstance(object, list):
                    for i in range(len(object)):
                        if predicate_object_map.object_map.term is None:
                            object[i] = "<" + object[i] + ">"
                        elif "IRI" in predicate_object_map.object_map.term:
                            object[i] = "<" + object[i] + ">"
                        else:
                            object[i] = "\"" + object[i] + "\""
                            if predicate_object_map.object_map.datatype != None:
                                object[i] = "\"" + object[i][1:-1] + "\"" + "^^<{}>".format(
                                    predicate_object_map.object_map.datatype)
                            elif predicate_object_map.object_map.datatype_map != None:
                                datatype_value = string_substitution_xml(predicate_object_map.object_map.datatype_map, "{(.+?)}", child,
                                                                    "object", triples_map.iterator, parent_map, namespace)
                                if "http" in datatype_value:
                                   object[i] = "\"" + object[i][1:-1] + "\"" + "^^<{}>".format(datatype_value)
                                else:
                                   object[i] = "\"" + object[i][1:-1] + "\"" + "^^<{}>".format("http://example.com/base/" + datatype_value)
                            elif predicate_object_map.object_map.language != None:
                                if "spanish" == predicate_object_map.object_map.language or "es" == predicate_object_map.object_map.language:
                                    object[i] += "@es"
                                elif "english" == predicate_object_map.object_map.language or "en" == predicate_object_map.object_map.language:
                                    object[i] += "@en"
                                elif len(predicate_object_map.object_map.language) == 2:
                                    object[i] += "@" + predicate_object_map.object_map.language
                                else:
                                    object[i] = None
                            elif predicate_object_map.object_map.language_map != None:
                                lang = string_substitution_xml(predicate_object_map.object_map.language_map, ".+",
                                                               child, "object", triples_map.iterator, parent_map,
                                                               namespace)
                                if lang != None:
                                    object[i] += "@" + string_substitution_xml(
                                        predicate_object_map.object_map.language_map, ".+", child, "object",
                                        triples_map.iterator, parent_map, namespace)[1:-1]
                else:
                    if predicate_object_map.object_map.term is None:
                        object = "<" + object + ">"
                    elif "IRI" in predicate_object_map.object_map.term:
                        object = "<" + object + ">"
                    else:
                        object = "\"" + object + "\""
                        if predicate_object_map.object_map.datatype != None:
                            object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(
                                predicate_object_map.object_map.datatype)
                        elif predicate_object_map.object_map.datatype_map != None:
                            datatype_value = string_substitution_xml(predicate_object_map.object_map.datatype_map, "{(.+?)}", child,
                                                                "object", triples_map.iterator, parent_map, namespace)
                            if "http" in datatype_value:
                               object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(datatype_value)
                            else:
                               object = "\"" + object[1:-1] + "\"" + "^^<{}>".format("http://example.com/base/" + datatype_value)
                        elif predicate_object_map.object_map.language != None:
                            if "spanish" == predicate_object_map.object_map.language or "es" == predicate_object_map.object_map.language:
                                object += "@es"
                            elif "english" == predicate_object_map.object_map.language or "en" == predicate_object_map.object_map.language:
                                object += "@en"
                            elif len(predicate_object_map.object_map.language) == 2:
                                object += "@" + predicate_object_map.object_map.language
                            else:
                                object = None
                        elif predicate_object_map.object_map.language_map != None:
                            lang = string_substitution_xml(predicate_object_map.object_map.language_map, ".+",
                                                           child, "object", triples_map.iterator, parent_map,
                                                           namespace)
                            if lang != None:
                                object += "@" + string_substitution_xml(
                                    predicate_object_map.object_map.language_map, ".+", child, "object",
                                    triples_map.iterator, parent_map, namespace)[1:-1]
            elif predicate_object_map.object_map.mapping_type == "reference":
                object = string_substitution_xml(predicate_object_map.object_map.value, ".+", child, "object",
                                                 triples_map.iterator, parent_map, namespace)
                if object != None:
                    if isinstance(object, list):
                        for i in range(len(object)):
                            if "\\" in object[i][1:-1]:
                                object[i] = "\"" + object[i][1:-1].replace("\\", "\\\\") + "\""
                            if "'" in object[i][1:-1]:
                                object[i] = "\"" + object[i][1:-1].replace("'", "\\\\'") + "\""
                            if "\"" in object[i][1:-1]:
                                object[i] = "\"" + object[i][1:-1].replace("\"", "\\\"") + "\""
                            if "\n" in object[i]:
                                object[i] = object[i].replace("\n", "\\n")
                            if predicate_object_map.object_map.datatype != None:
                                object[i] += "^^<{}>".format(predicate_object_map.object_map.datatype)
                            elif predicate_object_map.object_map.datatype_map != None:
                                datatype_value = string_substitution_xml(predicate_object_map.object_map.datatype_map, "{(.+?)}", child,
                                                                    "object", triples_map.iterator, parent_map, namespace)
                                if "http" in datatype_value:
                                   object[i] = "\"" + object[i][1:-1] + "\"" + "^^<{}>".format(datatype_value)
                                else:
                                   object[i] = "\"" + object[i][1:-1] + "\"" + "^^<{}>".format("http://example.com/base/" + datatype_value)
                            elif predicate_object_map.object_map.language != None:
                                if "spanish" == predicate_object_map.object_map.language or "es" == predicate_object_map.object_map.language:
                                    object[i] += "@es"
                                elif "english" == predicate_object_map.object_map.language or "en" == predicate_object_map.object_map.language:
                                    object[i] += "@en"
                                elif len(predicate_object_map.object_map.language) == 2:
                                    object[i] += "@" + predicate_object_map.object_map.language
                                else:
                                    object[i] = None
                            elif predicate_object_map.object_map.language_map != None:
                                lang = string_substitution_xml(predicate_object_map.object_map.language_map, ".+",
                                                               child, "object", triples_map.iterator, parent_map,
                                                               namespace)
                                if lang != None:
                                    object[i] += "@" + string_substitution_xml(
                                        predicate_object_map.object_map.language_map, ".+", child, "object",
                                        triples_map.iterator, parent_map, namespace)[1:-1]
                            elif predicate_object_map.object_map.term != None:
                                if "IRI" in predicate_object_map.object_map.term:
                                    if " " not in object:
                                        object[i] = "\"" + object[i][1:-1].replace("\\\\'", "'") + "\""
                                        object[i] = "<" + encode_char(object[i][1:-1]) + ">"
                                    else:
                                        object[i] = None
                    else:
                        if "\\" in object[1:-1]:
                            object = "\"" + object[1:-1].replace("\\", "\\\\") + "\""
                        if "'" in object[1:-1]:
                            object = "\"" + object[1:-1].replace("'", "\\\\'") + "\""
                        if "\"" in object[1:-1]:
                            object = "\"" + object[1:-1].replace("\"", "\\\"") + "\""
                        if "\n" in object:
                            object = object.replace("\n", "\\n")
                        if predicate_object_map.object_map.datatype != None:
                            object += "^^<{}>".format(predicate_object_map.object_map.datatype)
                        elif predicate_object_map.object_map.datatype_map != None:
                            datatype_value = string_substitution_xml(predicate_object_map.object_map.datatype_map, "{(.+?)}", child,
                                                                "object", triples_map.iterator, parent_map, namespace)
                            if "http" in datatype_value:
                               object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(datatype_value)
                            else:
                               object = "\"" + object[1:-1] + "\"" + "^^<{}>".format("http://example.com/base/" + datatype_value)
                        elif predicate_object_map.object_map.language != None:
                            if "spanish" == predicate_object_map.object_map.language or "es" == predicate_object_map.object_map.language:
                                object += "@es"
                            elif "english" == predicate_object_map.object_map.language or "en" == predicate_object_map.object_map.language:
                                object += "@en"
                            elif len(predicate_object_map.object_map.language) == 2:
                                object += "@" + predicate_object_map.object_map.language
                            else:
                                object = None
                        elif predicate_object_map.object_map.language_map != None:
                            lang = string_substitution_xml(predicate_object_map.object_map.language_map, ".+",
                                                           child, "object", triples_map.iterator, parent_map,
                                                           namespace)
                            if lang != None:
                                object += "@" + string_substitution_xml(
                                    predicate_object_map.object_map.language_map, ".+", child, "object",
                                    triples_map.iterator, parent_map, namespace)[1:-1]
                        elif predicate_object_map.object_map.term != None:
                            if "IRI" in predicate_object_map.object_map.term:
                                if " " not in object:
                                    object = "\"" + object[1:-1].replace("\\\\'", "'") + "\""
                                    object = "<" + encode_char(object[1:-1]) + ">"
                                else:
                                    object = None
            elif predicate_object_map.object_map.mapping_type == "parent triples map":
                if subject != None:
                    for triples_map_element in triples_map_list:
                        if triples_map_element.triples_map_id == predicate_object_map.object_map.value:
                            if triples_map_element.data_source != triples_map.data_source:
                                if triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[
                                    0] not in join_table:
                                    if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
                                        if "http" in triples_map_element.data_source:
                                            if triples_map_element.file_format == "JSONPath":
                                                response = urlopen(triples_map_element.data_source)
                                                data = json.loads(response.read())
                                                if isinstance(data, list):
                                                    hash_maker(data, triples_map_element,
                                                               predicate_object_map.object_map,"", triples_map_list)
                                                elif len(data) < 2:
                                                    hash_maker(data[list(data.keys())[0]], triples_map_element,
                                                               predicate_object_map.object_map,"", triples_map_list)
                                        else:
                                            with open(str(triples_map_element.data_source),
                                                      "r") as input_file_descriptor:
                                                if str(triples_map_element.file_format).lower() == "csv":
                                                    data = csv.DictReader(input_file_descriptor, delimiter=",")
                                                    hash_maker(data, triples_map_element,
                                                               predicate_object_map.object_map,"", triples_map_list)
                                                else:
                                                    data = json.load(input_file_descriptor)
                                                    if isinstance(data, list):
                                                        hash_maker(data, triples_map_element,
                                                                   predicate_object_map.object_map,"", triples_map_list)
                                                    elif len(data) < 2:
                                                        hash_maker(data[list(data.keys())[0]], triples_map_element,
                                                                   predicate_object_map.object_map,"", triples_map_list)

                                    elif triples_map_element.file_format == "XPath":
                                        with open(str(triples_map_element.data_source),
                                                  "r") as input_file_descriptor:
                                            child_tree = ET.parse(input_file_descriptor)
                                            child_root = child_tree.getroot()
                                            hash_maker_xml(child_root, triples_map_element,
                                                           predicate_object_map.object_map, parent_map, namespace)
                                    else:
                                        database, query_list = translate_sql(triples_map_element)
                                        db = connector.connect(host=host, port=int(port), user=user,
                                                               password=password)
                                        cursor = db.cursor(buffered=True)
                                        cursor.execute("use " + datab)
                                        for query in query_list:
                                            cursor.execute(query)
                                        hash_maker_array(cursor, triples_map_element,
                                                         predicate_object_map.object_map)

                                if "@" in predicate_object_map.object_map.child[0]:
                                    child_condition = predicate_object_map.object_map.child[0].split("@")[len(predicate_object_map.object_map.child[0].split("@"))-1]
                                    if child_condition in child.attrib:
                                        if child.attrib[child_condition] != None:
                                            if child.attrib[child_condition] in join_table[
                                                triples_map_element.triples_map_id + "_" +
                                                predicate_object_map.object_map.child[0]]:
                                                object_list = join_table[triples_map_element.triples_map_id + "_" +
                                                                         predicate_object_map.object_map.child[0]][
                                                    child.attrib[child_condition]]
                                            else:
                                                object_list = []
                                    else:
                                        object_list = []
                                else:
                                    if child.find(predicate_object_map.object_map.child[0]) != None:
                                        if child.find(predicate_object_map.object_map.child[0]).text in join_table[
                                            triples_map_element.triples_map_id + "_" +
                                            predicate_object_map.object_map.child[0]]:
                                            object_list = join_table[triples_map_element.triples_map_id + "_" +
                                                                     predicate_object_map.object_map.child[0]][
                                                child.find(predicate_object_map.object_map.child[0]).text]
                                        else:
                                            object_list = []
                                object = None
                            else:
                                if predicate_object_map.object_map.parent != None:
                                    if triples_map_element.triples_map_id + "_" + \
                                            predicate_object_map.object_map.child[0] not in join_table:
                                        with open(str(triples_map_element.data_source),
                                                  "r") as input_file_descriptor:
                                            child_tree = ET.parse(input_file_descriptor)
                                            child_root = child_tree.getroot()
                                            hash_maker_xml(child_root, triples_map_element,
                                                           predicate_object_map.object_map, parent_map, namespace)

                                    if "@" in predicate_object_map.object_map.child[0]:
                                        child_condition = predicate_object_map.object_map.child[0].split("@")[len(predicate_object_map.object_map.child[0].split("@"))-1]
                                        if child_condition in child.attrib:
                                            if child.attrib[child_condition] != None:
                                                if child.attrib[child_condition] in join_table[
                                                    triples_map_element.triples_map_id + "_" +
                                                    predicate_object_map.object_map.child[0]]:
                                                    object_list = join_table[triples_map_element.triples_map_id + "_" +
                                                                             predicate_object_map.object_map.child[0]][
                                                        child.attrib[child_condition]]
                                                else:
                                                    object_list = []
                                        else:
                                            object_list = []
                                    else:
                                        if child.find(predicate_object_map.object_map.child[0]) != None:
                                            if child.find(predicate_object_map.object_map.child[0]).text in join_table[
                                                triples_map_element.triples_map_id + "_" +
                                                predicate_object_map.object_map.child[0]]:
                                                object_list = join_table[triples_map_element.triples_map_id + "_" +
                                                                         predicate_object_map.object_map.child[0]][
                                                    child.find(predicate_object_map.object_map.child[0]).text]
                                            else:
                                                object_list = []
                                    object = None
                                else:
                                    try:
                                        object = "<" + string_substitution_xml(
                                            triples_map_element.subject_map.value, "{(.+?)}", child, "subject",
                                            triples_map.iterator, parent_map, namespace) + ">"
                                    except TypeError:
                                        object = None
                            break
                        else:
                            continue
                else:
                    object = None
            else:
                object = None

            if is_current_output_valid(triples_map.triples_map_id,predicate_object_map,current_logical_dump,logical_dump):
                if predicate in general_predicates:
                    dictionary_table_update(predicate + "_" + predicate_object_map.object_map.value)
                else:
                    dictionary_table_update(predicate)
                if predicate != None and (object != None or object) and subject != None:
                    for graph in triples_map.subject_map.graph:
                        dictionary_table_update(subject)
                        if isinstance(object, list):
                            for obj in object:
                                dictionary_table_update(obj)
                                triple = subject + " " + predicate + " " + obj + ".\n"
                                if graph != None and "defaultGraph" not in graph:
                                    if "{" in graph:
                                        triple = triple[:-2] + " <" + string_substitution_xml(graph, "{(.+?)}", child,
                                                                                              "subject",
                                                                                              triples_map.iterator,
                                                                                              parent_map,
                                                                                              namespace) + ">.\n"
                                        dictionary_table_update(
                                            "<" + string_substitution_xml(graph, "{(.+?)}", child, "subject",
                                                                          triples_map.iterator, parent_map,
                                                                          namespace) + ">")
                                    else:
                                        triple = triple[:-2] + " <" + graph + ">.\n"
                                        dictionary_table_update("<" + graph + ">")
                                if duplicate == "yes":
                                    if predicate in general_predicates:
                                        if dic_table[
                                            predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                            output_file_descriptor.write(triple)
                                            g_triples.update({dic_table[
                                                                  predicate + "_" + predicate_object_map.object_map.value]: {
                                                dic_table[subject] + "_" + dic_table[obj]: ""}})
                                            i += 1
                                        elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                            dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                            output_file_descriptor.write(triple)
                                            g_triples[dic_table[
                                                predicate + "_" + predicate_object_map.object_map.value]].update(
                                                {dic_table[subject] + "_" + dic_table[obj]: ""})
                                            i += 1
                                    else:
                                        if dic_table[predicate] not in g_triples:
                                            output_file_descriptor.write(triple)
                                            g_triples.update(
                                                {dic_table[predicate]: {dic_table[subject] + "_" + dic_table[obj]: ""}})
                                            i += 1
                                        elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                            dic_table[predicate]]:
                                            output_file_descriptor.write(triple)
                                            g_triples[dic_table[predicate]].update(
                                                {dic_table[subject] + "_" + dic_table[obj]: ""})
                                            i += 1
                                else:
                                    output_file_descriptor.write(triple)
                                    i += 1
                        else:
                            dictionary_table_update(object)
                            triple = subject + " " + predicate + " " + object + ".\n"
                            if graph != None and "defaultGraph" not in graph:
                                if "{" in graph:
                                    triple = triple[:-2] + " <" + string_substitution_xml(graph, "{(.+?)}", child,
                                                                                          "subject",
                                                                                          triples_map.iterator,
                                                                                          parent_map,
                                                                                          namespace) + ">.\n"
                                    dictionary_table_update(
                                        "<" + string_substitution_xml(graph, "{(.+?)}", child, "subject",
                                                                      triples_map.iterator, parent_map,
                                                                      namespace) + ">")
                                else:
                                    triple = triple[:-2] + " <" + graph + ">.\n"
                                    dictionary_table_update("<" + graph + ">")
                            if duplicate == "yes":
                                if predicate in general_predicates:
                                    if dic_table[
                                        predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                        output_file_descriptor.write(triple)
                                        g_triples.update({dic_table[
                                                              predicate + "_" + predicate_object_map.object_map.value]: {
                                            dic_table[subject] + "_" + dic_table[object]: ""}})
                                        i += 1
                                    elif dic_table[subject] + "_" + dic_table[object] not in g_triples[
                                        dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                        output_file_descriptor.write(triple)
                                        g_triples[
                                            dic_table[predicate + "_" + predicate_object_map.object_map.value]].update(
                                            {dic_table[subject] + "_" + dic_table[object]: ""})
                                        i += 1
                                else:
                                    if dic_table[predicate] not in g_triples:
                                        output_file_descriptor.write(triple)
                                        g_triples.update(
                                            {dic_table[predicate]: {dic_table[subject] + "_" + dic_table[object]: ""}})
                                        i += 1
                                    elif dic_table[subject] + "_" + dic_table[object] not in g_triples[
                                        dic_table[predicate]]:
                                        output_file_descriptor.write(triple)
                                        g_triples[dic_table[predicate]].update(
                                            {dic_table[subject] + "_" + dic_table[object]: ""})
                                        i += 1
                            else:
                                output_file_descriptor.write(triple)
                                i += 1
                    if predicate[1:-1] in predicate_object_map.graph:
                        if isinstance(object, list):
                            for obj in object:
                                triple = subject + " " + predicate + " " + obj + ".\n"
                                if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in \
                                        predicate_object_map.graph[predicate[1:-1]]:
                                    if "{" in predicate_object_map.graph[predicate[1:-1]]:
                                        triple = triple[:-2] + " <" + string_substitution_xml(
                                            predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", child, "subject",
                                            triples_map.iterator, parent_map, namespace) + ">.\n"
                                        dictionary_table_update(
                                            "<" + string_substitution_xml(predicate_object_map.graph[predicate[1:-1]],
                                                                          "{(.+?)}", child, "subject",
                                                                          triples_map.iterator, parent_map,
                                                                          namespace) + ">")
                                    else:
                                        triple = triple[:-2] + " <" + predicate_object_map.graph[
                                            predicate[1:-1]] + ">.\n"
                                        dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
                                    if duplicate == "yes":
                                        if predicate in general_predicates:
                                            if dic_table[
                                                predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                                output_file_descriptor.write(triple)
                                                g_triples.update({dic_table[
                                                                      predicate + "_" + predicate_object_map.object_map.value]: {
                                                    dic_table[subject] + "_" + dic_table[obj]: ""}})
                                                i += 1
                                            elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                                predicate + "_" + predicate_object_map.object_map.value]:
                                                output_file_descriptor.write(triple)
                                                g_triples[dic_table[
                                                    predicate + "_" + predicate_object_map.object_map.value]].update(
                                                    {dic_table[subject] + "_" + dic_table[obj]: ""})
                                                i += 1
                                        else:
                                            if dic_table[predicate] not in g_triples:
                                                output_file_descriptor.write(triple)
                                                g_triples.update({dic_table[predicate]: {
                                                    dic_table[subject] + "_" + dic_table[obj]: ""}})
                                                i += 1
                                            elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                                dic_table[predicate]]:
                                                output_file_descriptor.write(triple)
                                                g_triples[dic_table[predicate]].update(
                                                    {dic_table[subject] + "_" + dic_table[obj]: ""})
                                                i += 1
                                    else:
                                        output_file_descriptor.write(triple)
                        else:
                            triple = subject + " " + predicate + " " + object + ".\n"
                            if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in \
    								predicate_object_map.graph[predicate[1:-1]]:
                                if "{" in predicate_object_map.graph[predicate[1:-1]]:
                                    triple = triple[:-2] + " <" + string_substitution_xml(
                                        predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", child, "subject",
                                        triples_map.iterator, parent_map, namespace) + ">.\n"
                                else:
                                    triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
                                if duplicate == "yes":
                                    if predicate in general_predicates:
                                        if dic_table[
    										predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                            output_file_descriptor.write(triple)
                                            g_triples.update({dic_table[
                                                                  predicate + "_" + predicate_object_map.object_map.value]: {
                                                dic_table[subject] + "_" + dic_table[object]: ""}})
                                            i += 1
                                        elif dic_table[subject] + "_" + dic_table[object] not in g_triples[
                                            predicate + "_" + predicate_object_map.object_map.value]:
                                            output_file_descriptor.write(triple)
                                            g_triples[dic_table[
                                                predicate + "_" + predicate_object_map.object_map.value]].update(
                                                {dic_table[subject] + "_" + dic_table[object]: ""})
                                            i += 1
                                    else:
                                        if dic_table[predicate] not in g_triples:
                                            output_file_descriptor.write(triple)
                                            g_triples.update({dic_table[predicate]: {
                                                dic_table[subject] + "_" + dic_table[object]: ""}})
                                            i += 1
                                        elif dic_table[subject] + "_" + dic_table[object] not in g_triples[
                                            dic_table[predicate]]:
                                            output_file_descriptor.write(triple)
                                            g_triples[dic_table[predicate]].update(
                                                {dic_table[subject] + "_" + dic_table[object]: ""})
                                            i += 1
                                else:
                                    output_file_descriptor.write(triple)
                elif predicate != None and subject != None and object_list:
                    dictionary_table_update(subject)
                    for obj in object_list:
                        dictionary_table_update(obj)
                        for graph in triples_map.subject_map.graph:
                            if predicate_object_map.object_map.term != None:
                                if "IRI" in predicate_object_map.object_map.term:
                                    triple = subject + " " + predicate + " <" + obj[1:-1] + ">.\n"
                                else:
                                    triple = subject + " " + predicate + " " + obj + ".\n"
                            else:
                                triple = subject + " " + predicate + " " + obj + ".\n"

                            if graph != None and "defaultGraph" not in graph:
                                if "{" in graph:
                                    triple = triple[:-2] + " <" + string_substitution_xml(graph, "{(.+?)}", child,
                                                                                          "subject",
                                                                                          triples_map.iterator,
                                                                                          parent_map,
                                                                                          namespace) + ">.\n"
                                    dictionary_table_update(
                                        "<" + string_substitution_xml(graph, "{(.+?)}", child, "subject",
                                                                      triples_map.iterator, parent_map,
                                                                      namespace) + ">")
                                else:
                                    triple = triple[:-2] + " <" + graph + ">.\n"
                                    dictionary_table_update("<" + graph + ">")

                            if duplicate == "yes":
                                if predicate in general_predicates:
                                    if dic_table[
                                        predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                        output_file_descriptor.write(triple)
                                        g_triples.update({dic_table[
                                                              predicate + "_" + predicate_object_map.object_map.value]: {
                                            dic_table[subject] + "_" + dic_table[obj]: ""}})
                                        i += 1
                                    elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                        dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                        output_file_descriptor.write(triple)
                                        g_triples[
                                            dic_table[predicate + "_" + predicate_object_map.object_map.value]].update(
                                            {dic_table[subject] + "_" + dic_table[obj]: ""})
                                        i += 1
                                else:
                                    if dic_table[predicate] not in g_triples:
                                        output_file_descriptor.write(triple)
                                        g_triples.update(
                                            {dic_table[predicate]: {dic_table[subject] + "_" + dic_table[obj]: ""}})
                                        i += 1
                                    elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                        dic_table[predicate]]:
                                        output_file_descriptor.write(triple)
                                        g_triples[dic_table[predicate]].update(
                                            {dic_table[subject] + "_" + dic_table[obj]: ""})
                                        i += 1
                            else:
                                output_file_descriptor.write(triple)
                                i += 1
                        if predicate[1:-1] in predicate_object_map.graph:
                            triple = subject + " " + predicate + " " + obj + ".\n"
                            if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in \
                                    predicate_object_map.graph[predicate[1:-1]]:
                                if "{" in predicate_object_map.graph[predicate[1:-1]]:
                                    triple = triple[:-2] + " <" + string_substitution_xml(
                                        predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", child, "subject",
                                        triples_map.iterator, parent_map, namespace) + ">.\n"
                                    dictionary_table_update(
                                        "<" + string_substitution_xml(predicate_object_map.graph[predicate[1:-1]],
                                                                      "{(.+?)}", child, "subject", triples_map.iterator,
                                                                      parent_map, namespace) + ">")
                                else:
                                    triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
                                    dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
                                if duplicate == "yes":
                                    if predicate in general_predicates:
                                        if dic_table[
                                            predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                            output_file_descriptor.write(triple)
                                            g_triples.update({dic_table[
                                                                  predicate + "_" + predicate_object_map.object_map.value]: {
                                                dic_table[subject] + "_" + dic_table[obj]: ""}})
                                            i += 1
                                        elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                            dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                            output_file_descriptor.write(triple)
                                            g_triples[dic_table[
                                                predicate + "_" + predicate_object_map.object_map.value]].update(
                                                {dic_table[subject] + "_" + dic_table[obj]: ""})
                                            i += 1
                                    else:
                                        if dic_table[predicate] not in g_triples:
                                            output_file_descriptor.write(triple)
                                            g_triples.update(
                                                {dic_table[predicate]: {dic_table[subject] + "_" + dic_table[obj]: ""}})
                                            i += 1
                                        elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                            dic_table[predicate]]:
                                            output_file_descriptor.write(triple)
                                            g_triples[dic_table[predicate]].update(
                                                {dic_table[subject] + "_" + dic_table[obj]: ""})
                                            i += 1
                                else:
                                    output_file_descriptor.write(triple)
                                    i += 1
                    object_list = []
                else:
                    continue
    return i


def semantify_json(triples_map, triples_map_list, delimiter, output_file_descriptor, data, iterator):
    logger.info("TM: " + triples_map.triples_map_name)
    global current_logical_dump
    triples_map_triples = {}
    generated_triples = {}
    object_list = []
    subject_list = []
    global blank_message
    global host, port, user, password, datab
    i = 0
    if iterator == "$[*]":
        iterator = "$.[*]"
    if iterator != "None" and iterator != "$.[*]" and iterator != "":
        new_iterator = ""
        temp_keys = iterator.split(".")
        row = data
        executed = True
        for tp in temp_keys:
            new_iterator += tp + "."
            if "$" != tp and "" != tp:
                if "[*][*]" in tp:
                    if tp.split("[*][*]")[0] in row:
                        row = row[tp.split("[*][*]")[0]]
                    else:
                        row = []
                elif "[*]" in tp:
                    if tp.split("[*]")[0] in row:
                        row = row[tp.split("[*]")[0]]
                    else:
                        row = []
                elif "*" == tp:
                    pass
                else:
                    if tp in row:
                        row = row[tp]
                    else:
                        row = []
            elif "" == tp and isinstance(row, dict):
                if len(row.keys()) == 1:
                    while list(row.keys())[0] not in temp_keys:
                        new_iterator += "."
                        row = row[list(row.keys())[0]]
                        if isinstance(row, list):
                            for sub_row in row:
                                i += semantify_json(triples_map, triples_map_list, delimiter, output_file_descriptor,
                                                    sub_row, iterator.replace(new_iterator[:-1], ""))
                            executed = False
                            break
                        if isinstance(row, str):
                            row = []
                            break
                if "*" == new_iterator[-2]:
                    for sub_row in row:
                        i += semantify_json(triples_map, triples_map_list, delimiter, output_file_descriptor,
                                            row[sub_row], iterator.replace(new_iterator[:-1], ""))
                    executed = False
                    break
                if "[*][*]" in new_iterator:
                    for sub_row in row:
                        for sub_sub_row in row[sub_row]:
                            i += semantify_json(triples_map, triples_map_list, delimiter, output_file_descriptor,
                                                sub_sub_row, iterator.replace(new_iterator[:-1], ""))
                    executed = False
                    break
                if isinstance(row, list):
                    for sub_row in row:
                        i += semantify_json(triples_map, triples_map_list, delimiter, output_file_descriptor, sub_row,
                                            iterator.replace(new_iterator[:-1], ""))
                    executed = False
                    break
        if executed:
            if isinstance(row, list):
                for sub_row in row:
                    i += semantify_json(triples_map, triples_map_list, delimiter, output_file_descriptor, sub_row,
                                        iterator.replace(new_iterator[:-1], ""))
            else:
                i += semantify_json(triples_map, triples_map_list, delimiter, output_file_descriptor, row,
                                    iterator.replace(new_iterator[:-1], ""))
    elif iterator == "$.[*]":
        for row in data:
            i += semantify_json(triples_map, triples_map_list, delimiter, output_file_descriptor, row, "")
    else:
        create_subject = True
        global generated_subjects

        if mapping_partitions == "yes":
            if "_" in triples_map.triples_map_id:
                componets = triples_map.triples_map_id.split("_")[:-1]
                triples_map_id = ""
                for name in componets:
                    triples_map_id += name + "_"
                triples_map_id = triples_map_id[:-1]
            else:
                triples_map_id = triples_map.triples_map_id

            subject_attr = extract_subject_values(data, generated_subjects[triples_map_id]["subject_attr"], "JSONPath")

            if subject_attr == None:
                subject = None
                create_subject = False
            else:
                if triples_map_id in generated_subjects:
                    if subject_attr in generated_subjects[triples_map_id]:
                        subject = generated_subjects[triples_map_id][subject_attr]
                        create_subject = False

        if create_subject:
            subject_value = string_substitution_json(triples_map.subject_map.value, "{(.+?)}", data, "subject", ignore,
                                                     iterator)
            if triples_map.subject_map.subject_mapping_type == "template":
                if triples_map.subject_map.term_type is None:
                    if isinstance(subject_value,list):
                        subject_list = []
                        for subject_val in subject_value:
                            subject_list.append("<" + subject_val + ">")
                        subject = None
                    else:
                        if triples_map.subject_map.condition == "":

                            try:
                                subject = "<" + subject_value + ">"
                            except:
                                subject = None

                        else:
                            #	field, condition = condition_separetor(triples_map.subject_map.condition)
                            #	if row[field] == condition:
                            try:
                                subject = "<" + subject_value + ">"
                            except:
                                subject = None
                else:
                    if "IRI" in triples_map.subject_map.term_type:
                        if triples_map.subject_map.condition == "":

                            try:
                                if "http" not in subject_value:
                                    if base != "":
                                        subject = "<" + base + subject_value + ">"
                                    else:
                                        subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                                else:
                                    if is_valid_url_syntax(subject_value):
                                        subject = "<" + subject_value + ">"
                                    else:
                                        if base != "":
                                            subject = "<" + base + subject_value + ">"
                                        else:
                                            subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                            except:
                                subject = None

                        else:
                            #	field, condition = condition_separetor(triples_map.subject_map.condition)
                            #	if row[field] == condition:
                            try:
                                if "http" not in subject_value:
                                    if base != "":
                                        subject = "<" + base + subject_value + ">"
                                    else:
                                        subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                                else:
                                    if is_valid_url_syntax(subject_value):
                                        subject = "<" + subject_value + ">"
                                    else:
                                        if base != "":
                                            subject = "<" + base + subject_value + ">"
                                        else:
                                            subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                            except:
                                subject = None

                    elif "BlankNode" in triples_map.subject_map.term_type:
                        if triples_map.subject_map.condition == "":

                            try:
                                if "/" in subject_value:
                                    subject = "_:" + encode_char(subject_value.replace("/", "2F")).replace("%", "")
                                    if blank_message:
                                        logger.warning(
                                            "Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                                        blank_message = False
                                else:
                                    subject = "_:" + encode_char(subject_value).replace("%", "")
                                if "." in subject:
                                    subject = subject.replace(".", "2E")

                            except:
                                subject = None
                        else:
                            #	field, condition = condition_separetor(triples_map.subject_map.condition)
                            #	if row[field] == condition:
                            try:
                                subject = "_:" + subject_value
                            except:
                                subject = None
                    elif "Literal" in triples_map.subject_map.term_type:
                        subject = None
                    else:
                        if triples_map.subject_map.condition == "":

                            try:
                                subject = "<" + subject_value + ">"

                            except:
                                subject = None
                        else:
                            #	field, condition = condition_separetor(triples_map.subject_map.condition)
                            #	if row[field] == condition:
                            try:
                                subject = "<" + subject_value + ">"

                            except:
                                subject = None

            elif "reference" in triples_map.subject_map.subject_mapping_type:
                if triples_map.subject_map.condition == "":
                    subject_value = string_substitution_json(triples_map.subject_map.value, ".+", data, "subject",
                                                             ignore, iterator)
                    subject_value = subject_value[1:-1]
                    try:
                        if " " not in subject_value:
                            if "http" not in subject_value:
                                if base != "":
                                    subject = "<" + base + subject_value + ">"
                                else:
                                    subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                            else:
                                if is_valid_url_syntax(subject_value):
                                    subject = "<" + subject_value + ">"
                                else:
                                    if base != "":
                                        subject = "<" + base + subject_value + ">"
                                    else:
                                        subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                        else:
                            logger.error("<http://example.com/base/" + subject_value + "> is an invalid URL")
                            subject = None
                    except:
                        subject = None
                    if triples_map.subject_map.term_type == "IRI":
                        if " " not in subject_value:
                            subject = "<" + encode_char(subject_value) + ">"
                        else:
                            subject = None
                else:
                    #	field, condition = condition_separetor(triples_map.subject_map.condition)
                    #	if row[field] == condition:
                    try:
                        if "http" not in subject_value:
                            if base != "":
                                subject = "<" + base + subject_value + ">"
                            else:
                                subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                        else:
                            if is_valid_url_syntax(subject_value):
                                subject = "<" + subject_value + ">"
                            else:
                                if base != "":
                                    subject = "<" + base + subject_value + ">"
                                else:
                                    subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                    except:
                        subject = None

            elif "constant" in triples_map.subject_map.subject_mapping_type:
                subject = "<" + triples_map.subject_map.value + ">"
            elif "Literal" in triples_map.subject_map.term_type:
                subject = None
            else:
                if triples_map.subject_map.condition == "":

                    try:
                        subject = "\"" + triples_map.subject_map.value + "\""
                    except:
                        subject = None

                else:
                    #	field, condition = condition_separetor(triples_map.subject_map.condition)
                    #	if row[field] == condition:
                    try:
                        subject = "\"" + triples_map.subject_map.value + "\""
                    except:
                        subject = None

            if mapping_partitions == "yes":
                if triples_map_id in generated_subjects:
                    if subject_attr in generated_subjects[triples_map_id]:
                        pass
                    else:
                        generated_subjects[triples_map_id][subject_attr] = subject
                else:
                    generated_subjects[triples_map_id] = {subject_attr: subject}

        if triples_map.subject_map.rdf_class != [None] and subject != None:
            predicate = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
            for rdf_class in triples_map.subject_map.rdf_class:
                if rdf_class != None and ("str" == type(rdf_class).__name__ or "URIRef" == type(rdf_class).__name__):
                    for graph in triples_map.subject_map.graph:
                        obj = "<{}>".format(rdf_class)
                        dictionary_table_update(subject)
                        dictionary_table_update(obj)
                        dictionary_table_update(predicate + "_" + obj)
                        rdf_type = subject + " " + predicate + " " + obj + ".\n"
                        if graph != None and "defaultGraph" not in graph:
                            if "{" in graph:
                                rdf_type = rdf_type[:-2] + " <" + string_substitution_json(graph, "{(.+?)}", data,
                                                                                           "subject", ignore,
                                                                                           iterator) + ">.\n"
                                dictionary_table_update(
                                    "<" + string_substitution_json(graph, "{(.+?)}", data, "subject", ignore,
                                                                   iterator) + ">")
                            else:
                                rdf_type = rdf_type[:-2] + " <" + graph + ">.\n"
                                dictionary_table_update("<" + graph + ">")
                        if duplicate == "yes":
                            if dic_table[predicate + "_" + obj] not in g_triples:
                                output_file_descriptor.write(rdf_type)
                                g_triples.update(
                                    {dic_table[predicate + "_" + obj]: {dic_table[subject] + "_" + dic_table[obj]: ""}})
                                i += 1
                            elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                dic_table[predicate + "_" + obj]]:
                                output_file_descriptor.write(rdf_type)
                                g_triples[dic_table[predicate + "_" + obj]].update(
                                    {dic_table[subject] + "_" + dic_table[obj]: ""})
                                i += 1
                        else:
                            output_file_descriptor.write(rdf_type)
                            i += 1

        for predicate_object_map in triples_map.predicate_object_maps_list:
            if predicate_object_map.predicate_map.mapping_type == "constant" or predicate_object_map.predicate_map.mapping_type == "constant shortcut":
                predicate = "<" + predicate_object_map.predicate_map.value + ">"
            elif predicate_object_map.predicate_map.mapping_type == "template":
                if predicate_object_map.predicate_map.condition != "":
                    # field, condition = condition_separetor(predicate_object_map.predicate_map.condition)
                    # if row[field] == condition:
                    try:
                        predicate = "<" + string_substitution_json(predicate_object_map.predicate_map.value, "{(.+?)}",
                                                                   data, "predicate", ignore, iterator) + ">"
                    except:
                        predicate = None
                # else:
                #	predicate = None
                else:
                    try:
                        predicate = "<" + string_substitution_json(predicate_object_map.predicate_map.value, "{(.+?)}",
                                                                   data, "predicate", ignore, iterator) + ">"
                    except:
                        predicate = None
            elif predicate_object_map.predicate_map.mapping_type == "reference":
                if predicate_object_map.predicate_map.condition != "":
                    # field, condition = condition_separetor(predicate_object_map.predicate_map.condition)
                    # if row[field] == condition:
                    predicate = string_substitution_json(predicate_object_map.predicate_map.value, ".+", data,
                                                         "predicate", ignore, iterator)
                # else:
                #	predicate = None
                else:
                    predicate = string_substitution_json(predicate_object_map.predicate_map.value, ".+", data,
                                                         "predicate", ignore, iterator)
                predicate = "<" + predicate[1:-1] + ">"
            else:
                predicate = None

            if predicate_object_map.object_map.mapping_type == "constant" or predicate_object_map.object_map.mapping_type == "constant shortcut":
                if "/" in predicate_object_map.object_map.value:
                    object = "<" + predicate_object_map.object_map.value + ">"
                else:
                    object = "\"" + predicate_object_map.object_map.value + "\""
                if predicate_object_map.object_map.datatype != None:
                    object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
                elif predicate_object_map.object_map.datatype_map != None:
                    datatype_value = string_substitution_json(predicate_object_map.object_map.datatype_map, "{(.+?)}", data,
                                                        "object", ignore, iterator)
                    if "http" in datatype_value:
                       object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(datatype_value)
                    else:
                       object = "\"" + object[1:-1] + "\"" + "^^<{}>".format("http://example.com/base/" + datatype_value)
                elif predicate_object_map.object_map.language != None:
                    if "spanish" == predicate_object_map.object_map.language or "es" == predicate_object_map.object_map.language:
                        object += "@es"
                    elif "english" == predicate_object_map.object_map.language or "en" == predicate_object_map.object_map.language:
                        object += "@en"
                    elif len(predicate_object_map.object_map.language) == 2:
                        object += "@" + predicate_object_map.object_map.language
                    else:
                        object = None
                elif predicate_object_map.object_map.language_map != None:
                    lang = string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object",
                                               ignore, triples_map.iterator)
                    if lang != None:
                        object += "@" + string_substitution(predicate_object_map.object_map.language_map, ".+", row,
                                                            "object", ignore, triples_map.iterator)[1:-1]
            elif predicate_object_map.object_map.mapping_type == "template":
                try:
                    object = string_substitution_json(predicate_object_map.object_map.value, "{(.+?)}", data, "object",
                                                      ignore, iterator)
                    if isinstance(object, list):
                        object_list = object
                        object = None
                        i = 0
                        while i < len(object_list):
                            if predicate_object_map.object_map.term is None:
                                object_list[i] = "<" + object_list[i] + ">"
                            elif "IRI" in predicate_object_map.object_map.term:
                                object_list[i] = "<" + object_list[i] + ">"
                            elif "BlankNode" in predicate_object_map.object_map.term:
                                object_list[i] = "_:" + object_list[i]
                                if "/" in object:
                                    object_list[i] = object_list[i].replace("/", "2F")
                                    print("Incorrect format for Blank Nodes. \"/\" will be replace with \"-\".")
                                if "." in object_list[i]:
                                    object_list[i] = object_list[i].replace(".", "2E")
                                object_list[i] = encode_char(object_list[i])
                            else:
                                if predicate_object_map.object_map.datatype != None:
                                    object[i] = "\"" + object[i][1:-1] + "\"" + "^^<{}>".format(
                                        predicate_object_map.object_map.datatype)
                                elif predicate_object_map.object_map.datatype_map != None:
                                    datatype_value = string_substitution_json(predicate_object_map.object_map.datatype_map, "{(.+?)}", data,
                                                                        "object", ignore, iterator)
                                    if "http" in datatype_value:
                                       object[i] = "\"" + object[i][1:-1] + "\"" + "^^<{}>".format(datatype_value)
                                    else:
                                       object[i] = "\"" + object[i][1:-1] + "\"" + "^^<{}>".format("http://example.com/base/" + datatype_value)
                                elif predicate_object_map.object_map.language != None:
                                    if "spanish" == predicate_object_map.object_map.language or "es" == predicate_object_map.object_map.language:
                                        object[i] += "@es"
                                    elif "english" == predicate_object_map.object_map.language or "en" == predicate_object_map.object_map.language:
                                        object[i] += "@en"
                                    elif len(predicate_object_map.object_map.language) == 2:
                                        object[i] += "@" + predicate_object_map.object_map.language
                                    else:
                                        object[i] = None
                                elif predicate_object_map.object_map.language_map != None:
                                    lang = string_substitution_json(predicate_object_map.object_map.language_map, ".+",
                                                                    data, "object", ignore, iterator)
                                    if lang != None:
                                        object[i] += "@" + string_substitution_json(
                                            predicate_object_map.object_map.language_map, ".+", data, "object", ignore,
                                            iterator)[1:-1]
                            i += 1
                    else:
                        if predicate_object_map.object_map.term is None:
                            object = "<" + object + ">"
                        elif "IRI" in predicate_object_map.object_map.term:
                            object = "<" + object + ">"
                        elif "BlankNode" in predicate_object_map.object_map.term:
                            object = "_:" + object
                            if "/" in object:
                                object = object.replace("/", "2F")
                                print("Incorrect format for Blank Nodes. \"/\" will be replace with \"-\".")
                            if "." in object:
                                object = object.replace(".", "2E")
                            object = encode_char(object)
                        else:
                            object = "\"" + string_substitution_json(predicate_object_map.object_map.value, "{(.+?)}",
                                                                     data, "object", ignore, iterator) + "\""
                            if predicate_object_map.object_map.datatype != None:
                                object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(
                                    predicate_object_map.object_map.datatype)
                            elif predicate_object_map.object_map.datatype_map != None:
                                datatype_value = string_substitution_json(predicate_object_map.object_map.datatype_map, "{(.+?)}", data,
                                                                    "object", ignore, iterator)
                                if "http" in datatype_value:
                                   object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(datatype_value)
                                else:
                                   object = "\"" + object[1:-1] + "\"" + "^^<{}>".format("http://example.com/base/" + datatype_value)
                            elif predicate_object_map.object_map.language != None:
                                if "spanish" == predicate_object_map.object_map.language or "es" == predicate_object_map.object_map.language:
                                    object += "@es"
                                elif "english" == predicate_object_map.object_map.language or "en" == predicate_object_map.object_map.language:
                                    object += "@en"
                                elif len(predicate_object_map.object_map.language) == 2:
                                    object += "@" + predicate_object_map.object_map.language
                                else:
                                    object = None
                            elif predicate_object_map.object_map.language_map != None:
                                lang = string_substitution_json(predicate_object_map.object_map.language_map, ".+",
                                                                data, "object", ignore, iterator)
                                if lang != None:
                                    object += "@" + string_substitution_json(
                                        predicate_object_map.object_map.language_map, ".+", data, "object", ignore,
                                        iterator)[1:-1]
                except TypeError:
                    object = None
            elif predicate_object_map.object_map.mapping_type == "reference":
                object = string_substitution_json(predicate_object_map.object_map.value, ".+", data, "object", ignore,
                                                  iterator)
                if isinstance(object, list):
                    object_list = object
                    object = None
                    if object_list:
                        i = 0
                        while i < len(object_list):
                            if "\\" in object_list[i][1:-1]:
                                object = "\"" + object[i][1:-1].replace("\\", "\\\\") + "\""
                            if "'" in object_list[i][1:-1]:
                                object_list[i] = "\"" + object_list[i][1:-1].replace("'", "\\\\'") + "\""
                            if "\"" in object_list[i][1:-1]:
                                object_list[i] = "\"" + object_list[i][1:-1].replace("\"", "\\\"") + "\""
                            if "\n" in object_list[i]:
                                object_list[i] = object_list[i].replace("\n", "\\n")
                            if predicate_object_map.object_map.datatype != None:
                                object_list[i] = "\"" + object_list[i][1:-1] + "\"" + "^^<{}>".format(
                                    predicate_object_map.object_map.datatype)
                            elif predicate_object_map.object_map.datatype_map != None:
                                datatype_value = string_substitution_json(predicate_object_map.object_map.datatype_map, "{(.+?)}", data,
                                                                    "object", ignore, iterator)
                                if "http" in datatype_value:
                                   object[i] = "\"" + object[i][1:-1] + "\"" + "^^<{}>".format(datatype_value)
                                else:
                                   object[i] = "\"" + object[i][1:-1] + "\"" + "^^<{}>".format("http://example.com/base/" + datatype_value)
                            elif predicate_object_map.object_map.language != None:
                                if "spanish" == predicate_object_map.object_map.language or "es" == predicate_object_map.object_map.language:
                                    object_list[i] += "@es"
                                elif "english" == predicate_object_map.object_map.language or "en" == predicate_object_map.object_map.language:
                                    object_list[i] += "@en"
                                elif len(predicate_object_map.object_map.language) == 2:
                                    object_list[i] += "@" + predicate_object_map.object_map.language
                                else:
                                    object_list[i] = None
                            elif predicate_object_map.object_map.language_map != None:
                                object_list[i] += "@" + string_substitution_json(
                                    predicate_object_map.object_map.language_map, ".+", data, "object", ignore,
                                    iterator)[1:-1]
                            elif predicate_object_map.object_map.term != None:
                                if "IRI" in predicate_object_map.object_map.term:
                                    if " " not in object_list[i]:
                                        object_list[i] = "\"" + object_list[i][1:-1].replace("\\\\'", "'") + "\""
                                        object_list[i] = "<" + encode_char(object_list[i][1:-1]) + ">"
                                    else:
                                        object_list[i] = None
                            i += 1
                        if None in object_list:
                            temp = []
                            for obj in object_list:
                                temp.append(obj)
                            object_list = temp

                else:
                    if object != None:
                        if "\\" in object[1:-1]:
                            object = "\"" + object[1:-1].replace("\\", "\\\\") + "\""
                        if "'" in object[1:-1]:
                            object = "\"" + object[1:-1].replace("'", "\\\\'") + "\""
                        if "\"" in object[1:-1]:
                            object = "\"" + object[1:-1].replace("\"", "\\\"") + "\""
                        if "\n" in object:
                            object = object.replace("\n", "\\n")
                        if predicate_object_map.object_map.datatype != None:
                            object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(
                                predicate_object_map.object_map.datatype)
                        elif predicate_object_map.object_map.datatype_map != None:
                            datatype_value = string_substitution_json(predicate_object_map.object_map.datatype_map, "{(.+?)}", data,
                                                                "object", ignore, iterator)
                            if "http" in datatype_value:
                               object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(datatype_value)
                            else:
                               object = "\"" + object[1:-1] + "\"" + "^^<{}>".format("http://example.com/base/" + datatype_value)
                        elif predicate_object_map.object_map.language != None:
                            if "spanish" == predicate_object_map.object_map.language or "es" == predicate_object_map.object_map.language:
                                object += "@es"
                            elif "english" == predicate_object_map.object_map.language or "en" == predicate_object_map.object_map.language:
                                object += "@en"
                            elif len(predicate_object_map.object_map.language) == 2:
                                object += "@" + predicate_object_map.object_map.language
                            else:
                                object = None
                        elif predicate_object_map.object_map.language_map != None:
                            lang = string_substitution_json(predicate_object_map.object_map.language_map, ".+", data,
                                                            "object", ignore, iterator)
                            if lang != None:
                                object += "@" + string_substitution_json(predicate_object_map.object_map.language_map,
                                                                         ".+", data, "object", ignore, iterator)[1:-1]
                        elif predicate_object_map.object_map.term != None:
                            if "IRI" in predicate_object_map.object_map.term:
                                if " " not in object:
                                    object = "\"" + object[1:-1].replace("\\\\'", "'") + "\""
                                    object = "<" + encode_char(object[1:-1]) + ">"
                                else:
                                    object = None
            elif predicate_object_map.object_map.mapping_type == "parent triples map":
                if subject != None:
                    for triples_map_element in triples_map_list:
                        if triples_map_element.triples_map_id == predicate_object_map.object_map.value:
                            if triples_map_element.data_source != triples_map.data_source:
                                if triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[
                                    0] not in join_table:
                                    if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
                                        if "http" in triples_map_element.data_source:
                                            if triples_map_element.file_format == "JSONPath":
                                                response = urlopen(triples_map_element.data_source)
                                                data = json.loads(response.read())
                                                if isinstance(data, list):
                                                    hash_maker(data, triples_map_element,
                                                               predicate_object_map.object_map,"", triples_map_list)
                                                elif len(data) < 2:
                                                    hash_maker(data[list(data.keys())[0]], triples_map_element,
                                                               predicate_object_map.object_map,"", triples_map_list)
                                        else:
                                            with open(str(triples_map_element.data_source),
                                                      "r") as input_file_descriptor:
                                                if str(triples_map_element.file_format).lower() == "csv":
                                                    data_element = csv.DictReader(input_file_descriptor,
                                                                                  delimiter=delimiter)
                                                    hash_maker(data_element, triples_map_element,
                                                               predicate_object_map.object_map,"", triples_map_list)
                                                else:
                                                    data_element = json.load(input_file_descriptor)
                                                    if triples_map_element.iterator != "None" and triples_map_element.iterator != "$.[*]" and triples_map_element.iterator != "[*]":
                                                        join_iterator(data_element, triples_map_element.iterator,
                                                                      triples_map_element,
                                                                      predicate_object_map.object_map,
                                                                       triples_map_list)
                                                    else:
                                                        hash_maker(data_element[list(data_element.keys())[0]],
                                                                   triples_map_element, predicate_object_map.object_map,"", triples_map_list)

                                    elif triples_map_element.file_format == "XPath":
                                        with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
                                            child_tree = ET.parse(input_file_descriptor)
                                            child_root = child_tree.getroot()
                                            parent_map = {c: p for p in child_tree.iter() for c in p}
                                            namespace = dict([node for _, node in
                                                              ET.iterparse(str(triples_map_element.data_source),
                                                                           events=['start-ns'])])
                                            hash_maker_xml(child_root, triples_map_element,
                                                           predicate_object_map.object_map, parent_map, namespace)
                                    else:
                                        database, query_list = translate_sql(triples_map_element)
                                        db = connector.connect(host=host, port=int(port), user=user, password=password)
                                        cursor = db.cursor(buffered=True)
                                        cursor.execute("use " + datab)
                                        for query in query_list:
                                            cursor.execute(query)
                                        hash_maker_array(cursor, triples_map_element, predicate_object_map.object_map)
                                if sublist(predicate_object_map.object_map.child, data.keys()):
                                    if child_list_value(predicate_object_map.object_map.child, data) in join_table[
                                        triples_map_element.triples_map_id + "_" + child_list(
                                                predicate_object_map.object_map.child)]:
                                        object_list = join_table[triples_map_element.triples_map_id + "_" + child_list(
                                            predicate_object_map.object_map.child)][
                                            child_list_value(predicate_object_map.object_map.child, data)]
                                    else:
                                        object_list = []
                                object = None
                            else:
                                if predicate_object_map.object_map.parent != None:
                                    if triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[
                                        0] not in join_table:
                                        with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
                                            if str(triples_map_element.file_format).lower() == "csv":
                                                data = csv.DictReader(input_file_descriptor, delimiter=delimiter)
                                                hash_maker(data, triples_map_element, predicate_object_map.object_map,"", triples_map_list)
                                            else:
                                                parent_data = json.load(input_file_descriptor)
                                                if triples_map_element.iterator != "None":
                                                    join_iterator(parent_data, triples_map_element.iterator,
                                                                  triples_map_element, predicate_object_map.object_map, triples_map_list)
                                                else:
                                                    hash_maker(parent_data[list(parent_data.keys())[0]],
                                                               triples_map_element, predicate_object_map.object_map,"", triples_map_list)
                                    if "." in predicate_object_map.object_map.child[0]:
                                        temp_keys = predicate_object_map.object_map.child[0].split(".")
                                        temp_data = data
                                        for temp in temp_keys:
                                            if temp in temp_data:
                                                temp_data = temp_data[temp]
                                            else:
                                                temp_data = ""
                                                break
                                        if temp_data in join_table[triples_map_element.triples_map_id + "_" +
                                                                   predicate_object_map.object_map.child[
                                                                       0]] and temp_data != "":
                                            object_list = join_table[triples_map_element.triples_map_id + "_" +
                                                                     predicate_object_map.object_map.child[0]][
                                                temp_data]
                                        else:
                                            object_list = []
                                    else:
                                        if predicate_object_map.object_map.child[0] in data.keys():
                                            if data[predicate_object_map.object_map.child[0]] in join_table[
                                                triples_map_element.triples_map_id + "_" +
                                                predicate_object_map.object_map.child[0]]:
                                                object_list = join_table[triples_map_element.triples_map_id + "_" +
                                                                         predicate_object_map.object_map.child[0]][
                                                    data[predicate_object_map.object_map.child[0]]]
                                            else:
                                                object_list = []
                                        else:
                                            if "." in predicate_object_map.object_map.child[0]:
                                                iterators = predicate_object_map.object_map.child[0].split(".")
                                                if "[*]" in iterators[0]:
                                                    data = data[iterators[0].split("[*]")[0]]
                                                    for row in data:
                                                        if str(row[iterators[1]]) in join_table[
                                                            triples_map_element.triples_map_id + "_" +
                                                            predicate_object_map.object_map.child[0]]:
                                                            object_list = join_table[
                                                                triples_map_element.triples_map_id + "_" +
                                                                predicate_object_map.object_map.child[0]][
                                                                str(row[iterators[1]])]
                                                            if predicate != None and subject != None and object_list:
                                                                for obj in object_list:
                                                                    for graph in triples_map.subject_map.graph:
                                                                        if predicate_object_map.object_map.term != None:
                                                                            if "IRI" in predicate_object_map.object_map.term:
                                                                                triple = subject + " " + predicate + " <" + obj[
                                                                                                                            1:-1] + ">.\n"
                                                                            else:
                                                                                triple = subject + " " + predicate + " " + obj + ".\n"
                                                                        else:
                                                                            triple = subject + " " + predicate + " " + obj + ".\n"
                                                                        if graph != None and "defaultGraph" not in graph:
                                                                            if "{" in graph:
                                                                                triple = triple[
                                                                                         :-2] + " <" + string_substitution_json(
                                                                                    graph, "{(.+?)}", data, "subject",
                                                                                    ignore, iterator) + ">.\n"
                                                                            else:
                                                                                triple = triple[
                                                                                         :-2] + " <" + graph + ">.\n"
                                                                        if duplicate == "yes":
                                                                            if (triple not in generated_triples) and (
                                                                                    triple not in g_triples):
                                                                                output_file_descriptor.write(triple)
                                                                                generated_triples.update(
                                                                                    {triple: number_triple})
                                                                                g_triples.update(
                                                                                    {triple: number_triple})
                                                                                i += 1
                                                                        else:
                                                                            output_file_descriptor.write(triple)
                                                                            i += 1
                                                                    if predicate[1:-1] in predicate_object_map.graph:
                                                                        triple = subject + " " + predicate + " " + obj + ".\n"
                                                                        if predicate_object_map.graph[predicate[
                                                                                                      1:-1]] != None and "defaultGraph" not in \
                                                                                predicate_object_map.graph[
                                                                                    predicate[1:-1]]:
                                                                            if "{" in predicate_object_map.graph[
                                                                                predicate[1:-1]]:
                                                                                triple = triple[
                                                                                         :-2] + " <" + string_substitution_json(
                                                                                    predicate_object_map.graph[
                                                                                        predicate[1:-1]], "{(.+?)}",
                                                                                    data, "subject", ignore,
                                                                                    iterator) + ">.\n"
                                                                            else:
                                                                                triple = triple[:-2] + " <" + \
                                                                                         predicate_object_map.graph[
                                                                                             predicate[1:-1]] + ">.\n"
                                                                            if duplicate == "yes":
                                                                                if predicate not in g_triples:
                                                                                    output_file_descriptor.write(triple)
                                                                                    generated_triples.update(
                                                                                        {triple: number_triple})
                                                                                    g_triples.update({predicate: {
                                                                                        subject + "_" + object: triple}})
                                                                                    i += 1
                                                                                elif subject + "_" + object not in \
                                                                                        g_triples[predicate]:
                                                                                    output_file_descriptor.write(triple)
                                                                                    generated_triples.update(
                                                                                        {triple: number_triple})
                                                                                    g_triples[predicate].update({
                                                                                                                    subject + "_" + object: triple})
                                                                                    i += 1
                                                                                elif triple not in g_triples[predicate][
                                                                                    subject + "_" + obj]:
                                                                                    output_file_descriptor.write(triple)
                                                                                    i += 1
                                                                            else:
                                                                                output_file_descriptor.write(triple)
                                                                                i += 1
                                                        object_list = []
                                                elif "[" in iterators[0] and "]" in iterators[0]:
                                                    data = data[iterators[0].split("[")[0]]
                                                    index = int(iterators[0].split("[")[1].split("]")[0])
                                                    if index < len(data):
                                                        if str(data[index][iterators[1]]) in join_table[
                                                            triples_map_element.triples_map_id + "_" +
                                                            predicate_object_map.object_map.child[0]]:
                                                            object_list = join_table[
                                                                triples_map_element.triples_map_id + "_" +
                                                                predicate_object_map.object_map.child[0]][
                                                                str(data[int(index)][iterators[1]])]
                                                        else:
                                                            object_list = []
                                                    else:
                                                        logger.error("Requesting an element outside list range.")
                                                        object_list = []

                                    object = None
                                else:
                                    if triples_map_element.iterator != triples_map.iterator:
                                        parent_iterator = triples_map_element.iterator
                                        child_keys = triples_map.iterator.split(".")
                                        for child in child_keys:
                                            if child in parent_iterator:
                                                parent_iterator = parent_iterator.replace(child, "")[1:]
                                            else:
                                                break
                                    else:
                                        parent_iterator = ""
                                    try:
                                        object = "<" + string_substitution_json(triples_map_element.subject_map.value,
                                                                                "{(.+?)}", data, "object", ignore,
                                                                                parent_iterator) + ">"
                                    except TypeError:
                                        object = None
                            break
                        else:
                            continue
                else:
                    object = None
            else:
                object = None
            if is_current_output_valid(triples_map.triples_map_id,predicate_object_map,current_logical_dump,logical_dump):
                if predicate in general_predicates:
                    dictionary_table_update(predicate + "_" + predicate_object_map.object_map.value)
                else:
                    dictionary_table_update(predicate)
                if predicate != None and object != None and subject != None:
                    dictionary_table_update(subject)
                    dictionary_table_update(object)
                    for graph in triples_map.subject_map.graph:
                        triple = subject + " " + predicate + " " + object + ".\n"
                        if graph != None and "defaultGraph" not in graph:
                            if "{" in graph:
                                triple = triple[:-2] + " <" + string_substitution_json(graph, "{(.+?)}", data, "subject",
                                                                                       ignore, iterator) + ">.\n"
                                dictionary_table_update(
                                    "<" + string_substitution_json(graph, "{(.+?)}", data, "subject", ignore,
                                                                   iterator) + ">")
                            else:
                                triple = triple[:-2] + " <" + graph + ">.\n"
                                dictionary_table_update("<" + graph + ">")
                        if predicate_object_map.graph[predicate[1:-1]] == None or graph != None:
                            if duplicate == "yes":
                                if predicate in general_predicates:
                                    if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                        output_file_descriptor.write(triple)
                                        g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value]: {
                                            dic_table[subject] + "_" + dic_table[object]: ""}})
                                        i += 1
                                    elif dic_table[subject] + "_" + dic_table[object] not in g_triples[
                                        dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                        output_file_descriptor.write(triple)
                                        g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update(
                                            {dic_table[subject] + "_" + dic_table[object]: ""})
                                        i += 1
                                else:
                                    if dic_table[predicate] not in g_triples:
                                        output_file_descriptor.write(triple)
                                        g_triples.update(
                                            {dic_table[predicate]: {dic_table[subject] + "_" + dic_table[object]: ""}})
                                        i += 1
                                    elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
                                        output_file_descriptor.write(triple)
                                        g_triples[dic_table[predicate]].update(
                                            {dic_table[subject] + "_" + dic_table[object]: ""})
                                        i += 1
                            else:
                                output_file_descriptor.write(triple)
                                i += 1
                    if predicate[1:-1] in predicate_object_map.graph:
                        triple = subject + " " + predicate + " " + object + ".\n"
                        if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in \
                                predicate_object_map.graph[predicate[1:-1]]:
                            if "{" in predicate_object_map.graph[predicate[1:-1]]:
                                triple = triple[:-2] + " <" + string_substitution_json(
                                    predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", data, "subject", ignore,
                                    iterator) + ">.\n"
                                dictionary_table_update(
                                    "<" + string_substitution_json(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}",
                                                                   data, "subject", ignore, iterator) + ">")
                            else:
                                triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
                                dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
                            if duplicate == "yes":
                                if predicate in general_predicates:
                                    if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                        output_file_descriptor.write(triple)
                                        g_triples.update({dic_table[
                                                              predicate + "_" + predicate_object_map.object_map.value]: {
                                            dic_table[subject] + "_" + dic_table[object]: ""}})
                                        i += 1
                                    elif dic_table[subject] + "_" + dic_table[object] not in g_triples[
                                        predicate + "_" + predicate_object_map.object_map.value]:
                                        output_file_descriptor.write(triple)
                                        g_triples[
                                            dic_table[predicate + "_" + predicate_object_map.object_map.value]].update(
                                            {dic_table[subject] + "_" + dic_table[object]: ""})
                                        i += 1
                                else:
                                    if dic_table[predicate] not in g_triples:
                                        output_file_descriptor.write(triple)
                                        g_triples.update(
                                            {dic_table[predicate]: {dic_table[subject] + "_" + dic_table[object]: ""}})
                                        i += 1
                                    elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
                                        output_file_descriptor.write(triple)
                                        g_triples[dic_table[predicate]].update(
                                            {dic_table[subject] + "_" + dic_table[object]: ""})
                                        i += 1
                            else:
                                output_file_descriptor.write(triple)
                                i += 1
                elif predicate != None and subject != None and object_list:
                    dictionary_table_update(subject)
                    for obj in object_list:
                        dictionary_table_update(obj)
                        for graph in triples_map.subject_map.graph:
                            if predicate_object_map.object_map.term != None:
                                if "IRI" in predicate_object_map.object_map.term:
                                    triple = subject + " " + predicate + " <" + obj[1:-1] + ">.\n"
                                else:
                                    triple = subject + " " + predicate + " " + obj + ".\n"
                            else:
                                triple = subject + " " + predicate + " " + obj + ".\n"
                            if graph != None and "defaultGraph" not in graph:
                                if "{" in graph:
                                    triple = triple[:-2] + " <" + string_substitution_json(graph, "{(.+?)}", data,
                                                                                           "subject", ignore,
                                                                                           iterator) + ">.\n"
                                    dictionary_table_update(
                                        "<" + string_substitution_json(graph, "{(.+?)}", data, "subject", ignore,
                                                                       iterator) + ">")
                                else:
                                    triple = triple[:-2] + " <" + graph + ">.\n"
                                    dictionary_table_update("<" + graph + ">")
                            if predicate_object_map.graph[predicate[1:-1]] == None or graph != None:
                                if duplicate == "yes":
                                    if predicate in general_predicates:
                                        if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                            output_file_descriptor.write(triple)
                                            g_triples.update({dic_table[
                                                                  predicate + "_" + predicate_object_map.object_map.value]: {
                                                dic_table[subject] + "_" + dic_table[obj]: ""}})
                                            i += 1
                                        elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                            dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                            output_file_descriptor.write(triple)
                                            g_triples[
                                                dic_table[predicate + "_" + predicate_object_map.object_map.value]].update(
                                                {dic_table[subject] + "_" + dic_table[obj]: ""})
                                            i += 1
                                    else:
                                        if dic_table[predicate] not in g_triples:
                                            output_file_descriptor.write(triple)
                                            g_triples.update(
                                                {dic_table[predicate]: {dic_table[subject] + "_" + dic_table[obj]: ""}})
                                            i += 1
                                        elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
                                            output_file_descriptor.write(triple)
                                            g_triples[dic_table[predicate]].update(
                                                {dic_table[subject] + "_" + dic_table[obj]: ""})
                                            i += 1
                                else:
                                    output_file_descriptor.write(triple)
                                    i += 1

                        if predicate[1:-1] in predicate_object_map.graph:
                            triple = subject + " " + predicate + " " + obj + ".\n"
                            if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in \
                                    predicate_object_map.graph[predicate[1:-1]]:
                                if "{" in predicate_object_map.graph[predicate[1:-1]]:
                                    triple = triple[:-2] + " <" + string_substitution_json(
                                        predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", data, "subject", ignore,
                                        iterator) + ">.\n"
                                    dictionary_table_update(
                                        "<" + string_substitution_json(predicate_object_map.graph[predicate[1:-1]],
                                                                       "{(.+?)}", data, "subject", ignore, iterator) + ">")
                                else:
                                    triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
                                    dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
                                if duplicate == "yes":
                                    if predicate in general_predicates:
                                        if dic_table[
                                            predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                            output_file_descriptor.write(triple)
                                            g_triples.update({dic_table[
                                                                  predicate + "_" + predicate_object_map.object_map.value]: {
                                                dic_table[subject] + "_" + dic_table[obj]: ""}})
                                            i += 1
                                        elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                            dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                            output_file_descriptor.write(triple)
                                            g_triples[
                                                dic_table[predicate + "_" + predicate_object_map.object_map.value]].update(
                                                {dic_table[subject] + "_" + dic_table[obj]: ""})
                                            i += 1
                                    else:
                                        if dic_table[predicate] not in g_triples:
                                            output_file_descriptor.write(triple)
                                            g_triples.update(
                                                {dic_table[predicate]: {dic_table[subject] + "_" + dic_table[obj]: ""}})
                                            i += 1
                                        elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                            dic_table[predicate]]:
                                            output_file_descriptor.write(triple)
                                            g_triples[dic_table[predicate]].update(
                                                {dic_table[subject] + "_" + dic_table[obj]: ""})
                                            i += 1
                                else:
                                    output_file_descriptor.write(triple)
                                    i += 1
                    object_list = []
                elif predicate != None and subject_list:
                    for subj in subject_list:
                        dictionary_table_update(subj)
                        type_predicate = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
                        for rdf_class in triples_map.subject_map.rdf_class:
                            if rdf_class != None and ("str" == type(rdf_class).__name__ or "URIRef" == type(rdf_class).__name__):
                                for graph in triples_map.subject_map.graph:
                                    obj = "<{}>".format(rdf_class)
                                    dictionary_table_update(obj)
                                    dictionary_table_update(type_predicate + "_" + obj)
                                    rdf_type = subj + " " + type_predicate + " " + obj + ".\n"
                                    if graph != None and "defaultGraph" not in graph:
                                        if "{" in graph:
                                            rdf_type = rdf_type[:-2] + " <" + string_substitution_json(graph, "{(.+?)}", data,
                                                                                                       "subject", ignore,
                                                                                                       iterator) + ">.\n"
                                            dictionary_table_update(
                                                "<" + string_substitution_json(graph, "{(.+?)}", data, "subject", ignore,
                                                                               iterator) + ">")
                                        else:
                                            rdf_type = rdf_type[:-2] + " <" + graph + ">.\n"
                                            dictionary_table_update("<" + graph + ">")
                                    if duplicate == "yes":
                                        if dic_table[type_predicate + "_" + obj] not in g_triples:
                                            output_file_descriptor.write(rdf_type)
                                            g_triples.update(
                                                {dic_table[type_predicate + "_" + obj]: {dic_table[subj] + "_" + dic_table[obj]: ""}})
                                            i += 1
                                        elif dic_table[subj] + "_" + dic_table[obj] not in g_triples[
                                            dic_table[type_predicate + "_" + obj]]:
                                            output_file_descriptor.write(rdf_type)
                                            g_triples[dic_table[type_predicate + "_" + obj]].update(
                                                {dic_table[subj] + "_" + dic_table[obj]: ""})
                                            i += 1
                                    else:
                                        output_file_descriptor.write(rdf_type)
                                        i += 1
                        if object != None:
                            dictionary_table_update(object)
                            triple = subj + " " + predicate + " " + object + ".\n"
                            for graph in triples_map.subject_map.graph:
                                if graph != None and "defaultGraph" not in graph:
                                    if "{" in graph:
                                        triple = triple[:-2] + " <" + string_substitution_json(graph, "{(.+?)}", data, "subject",
                                                                                               ignore, iterator) + ">.\n"
                                        dictionary_table_update(
                                            "<" + string_substitution_json(graph, "{(.+?)}", data, "subject", ignore,
                                                                           iterator) + ">")
                                    else:
                                        triple = triple[:-2] + " <" + graph + ">.\n"
                                        dictionary_table_update("<" + graph + ">")
                                if predicate[1:-1] not in predicate_object_map.graph or graph != None or triples_map.subject_map.graph == [None]:
                                    if duplicate == "yes":
                                        if predicate in general_predicates:
                                            if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                                output_file_descriptor.write(triple)
                                                g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value]: {
                                                    dic_table[subj] + "_" + dic_table[object]: ""}})
                                                i += 1
                                            elif dic_table[subj] + "_" + dic_table[object] not in g_triples[
                                                dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                                output_file_descriptor.write(triple)
                                                g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update(
                                                    {dic_table[subj] + "_" + dic_table[object]: ""})
                                                i += 1
                                        else:
                                            if dic_table[predicate] not in g_triples:
                                                output_file_descriptor.write(triple)
                                                g_triples.update(
                                                    {dic_table[predicate]: {dic_table[subj] + "_" + dic_table[object]: ""}})
                                                i += 1
                                            elif dic_table[subj] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
                                                output_file_descriptor.write(triple)
                                                g_triples[dic_table[predicate]].update(
                                                    {dic_table[subj] + "_" + dic_table[object]: ""})
                                                i += 1
                                    else:
                                        output_file_descriptor.write(triple)
                                        i += 1
                        elif object_list:
                            for obj in object_list:
                                dictionary_table_update(obj)
                                for graph in triples_map.subject_map.graph:
                                    if predicate_object_map.object_map.term != None:
                                        if "IRI" in predicate_object_map.object_map.term:
                                            triple = subj + " " + predicate + " <" + obj[1:-1] + ">.\n"
                                        else:
                                            triple = subj + " " + predicate + " " + obj + ".\n"
                                    else:
                                        triple = subj + " " + predicate + " " + obj + ".\n"
                                    if graph != None and "defaultGraph" not in graph:
                                        if "{" in graph:
                                            triple = triple[:-2] + " <" + string_substitution_json(graph, "{(.+?)}", data,
                                                                                                   "subject", ignore,
                                                                                                   iterator) + ">.\n"
                                            dictionary_table_update(
                                                "<" + string_substitution_json(graph, "{(.+?)}", data, "subject", ignore,
                                                                               iterator) + ">")
                                        else:
                                            triple = triple[:-2] + " <" + graph + ">.\n"
                                            dictionary_table_update("<" + graph + ">")
                                    if predicate[1:-1] not in predicate_object_map.graph or graph != None or triples_map.subject_map.graph == [None]:
                                        if duplicate == "yes":
                                            if predicate in general_predicates:
                                                if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                                    output_file_descriptor.write(triple)
                                                    g_triples.update({dic_table[
                                                                          predicate + "_" + predicate_object_map.object_map.value]: {
                                                        dic_table[subj] + "_" + dic_table[obj]: ""}})
                                                    i += 1
                                                elif dic_table[subj] + "_" + dic_table[obj] not in g_triples[
                                                    dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                                    output_file_descriptor.write(triple)
                                                    g_triples[
                                                        dic_table[predicate + "_" + predicate_object_map.object_map.value]].update(
                                                        {dic_table[subj] + "_" + dic_table[obj]: ""})
                                                    i += 1
                                            else:
                                                if dic_table[predicate] not in g_triples:
                                                    output_file_descriptor.write(triple)
                                                    g_triples.update(
                                                        {dic_table[predicate]: {dic_table[subj] + "_" + dic_table[obj]: ""}})
                                                    i += 1
                                                elif dic_table[subj] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
                                                    output_file_descriptor.write(triple)
                                                    g_triples[dic_table[predicate]].update(
                                                        {dic_table[subj] + "_" + dic_table[obj]: ""})
                                                    i += 1
                                        else:
                                            output_file_descriptor.write(triple)
                                            i += 1
                        else:
                            continue
                else:
                    continue
    return i


def semantify_file(triples_map, triples_map_list, delimiter, output_file_descriptor, data, no_inner_cycle):
    """
		(Private function, not accessible from outside this package)

		Takes a triples-map rule and applies it to each one of the rows of its CSV data
		source

		Parameters
		----------
		triples_map : TriplesMap object
			Mapping rule consisting of a logical source, a subject-map and several predicateObjectMaps
			(refer to the TriplesMap.py file in the triplesmap folder)
		triples_map_list : list of TriplesMap objects
			List of triples-maps parsed from current mapping being used for the semantification of a
			dataset (mainly used to perform rr:joinCondition mappings)
		delimiter : string
			Delimiter value for the CSV or TSV file ("\s" and "\t" respectively)
		output_file_descriptor : file object
			Descriptor to the output file (refer to the Python 3 documentation)

		Returns
		-------
		An .nt file per each dataset mentioned in the configuration file semantified.
		If the duplicates are asked to be removed in main memory, also returns a -min.nt
		file with the triples sorted and with the duplicates removed.
		"""

    object_list = []
    subject_list = []
    triples_string = ""
    end_turtle = ""
    i = 0
    no_update = True
    global blank_message
    global generated_subjects
    global user, password, port, host, datab
    logger.info("TM: " + triples_map.triples_map_name)

    if mapping_partitions == "yes":
        if triples_map.predicate_object_maps_list[0].predicate_map.mapping_type == "constant" or \
                triples_map.predicate_object_maps_list[0].predicate_map.mapping_type == "constant shortcut":
            if output_format.lower() == "n-triples":
                predicate = "<" + triples_map.predicate_object_maps_list[0].predicate_map.value + ">"
            else:
                predicate = determine_prefix(triples_map.predicate_object_maps_list[0].predicate_map.value)
            constant_predicate = False
        else:
            predicate = None
            constant_predicate = True
    else:
        predicate = None
        constant_predicate = True

    for row in data:
        generated = 0
        duplicate_type = False
        create_subject = True
        global generated_subjects
        if mapping_partitions == "yes":
            if "_" in triples_map.triples_map_id:
                componets = triples_map.triples_map_id.split("_")[:-1]
                triples_map_id = ""
                for name in componets:
                    triples_map_id += name + "_"
                triples_map_id = triples_map_id[:-1]
            else:
                triples_map_id = triples_map.triples_map_id

            if triples_map_id in generated_subjects:
                subject_attr = ""
                for attr in generated_subjects[triples_map_id]["subject_attr"]:
                    subject_attr += str(row[attr]) + "_"
                subject_attr = subject_attr[:-1]

                if subject_attr in generated_subjects[triples_map_id]:
                    subject = generated_subjects[triples_map_id][subject_attr]
                    create_subject = False

        if create_subject:
            if triples_map.subject_map.subject_mapping_type == "template":
                subject_value = string_substitution(triples_map.subject_map.value, "{(.+?)}", row, "subject", ignore,
                                                    triples_map.iterator)
                if triples_map.subject_map.term_type is None:
                    if triples_map.subject_map.condition == "":

                        try:
                            subject = "<" + subject_value + ">"
                        except:
                            subject = None

                    else:
                        #	field, condition = condition_separetor(triples_map.subject_map.condition)
                        #	if row[field] == condition:
                        try:
                            subject = "<" + subject_value + ">"
                        except:
                            subject = None
                else:
                    if "IRI" in triples_map.subject_map.term_type:
                        subject_value = string_substitution(triples_map.subject_map.value, "{(.+?)}", row, "subject",
                                                            ignore, triples_map.iterator)
                        if triples_map.subject_map.condition == "":

                            try:
                                if "http" not in subject_value:
                                    if base != "":
                                        subject = "<" + base + subject_value + ">"
                                    else:
                                        subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                                else:
                                    if is_valid_url_syntax(subject_value):
                                        subject = "<" + subject_value + ">"
                                    else:
                                        if base != "":
                                            subject = "<" + base + subject_value + ">"
                                        else:
                                            subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                            except:
                                subject = None

                        else:
                            #	field, condition = condition_separetor(triples_map.subject_map.condition)
                            #	if row[field] == condition:
                            try:
                                if "http" not in subject_value:
                                    if base != "":
                                        subject = "<" + base + subject_value + ">"
                                    else:
                                        subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                                else:
                                    if is_valid_url_syntax(subject_value):
                                        subject = "<" + subject_value + ">"
                                    else:
                                        if base != "":
                                            subject = "<" + base + subject_value + ">"
                                        else:
                                            subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                            except:
                                subject = None

                    elif "BlankNode" in triples_map.subject_map.term_type:
                        if triples_map.subject_map.condition == "":
                            try:
                                if "/" in subject_value:
                                    subject = "_:" + encode_char(subject_value.replace("/", "2F")).replace("%", "")
                                    if "." in subject:
                                        subject = subject.replace(".", "2E")
                                    if blank_message:
                                        logger.warning(
                                            "Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                                        blank_message = False
                                else:
                                    subject = "_:" + encode_char(subject_value).replace("%", "")
                                    if "." in subject:
                                        subject = subject.replace(".", "2E")
                            except:
                                subject = None

                        else:
                            #	field, condition = condition_separetor(triples_map.subject_map.condition)
                            #	if row[field] == condition:
                            try:
                                subject = "_:" + subject_value
                            except:
                                subject = None
                    elif "Literal" in triples_map.subject_map.term_type:
                        subject = None
                    else:
                        if triples_map.subject_map.condition == "":

                            try:
                                subject = "<" + subject_value + ">"
                            except:
                                subject = None

                        else:
                            #	field, condition = condition_separetor(triples_map.subject_map.condition)
                            #	if row[field] == condition:
                            try:
                                subject = "<" + subject_value + ">"
                            except:
                                subject = None
            elif "reference" in triples_map.subject_map.subject_mapping_type:
                subject_value = string_substitution(triples_map.subject_map.value, ".+", row, "subject", ignore,
                                                    triples_map.iterator)
                if subject_value != None:
                    subject_value = subject_value[1:-1]
                    if triples_map.subject_map.condition == "":
                        if " " not in subject_value:
                            if "BlankNode" in triples_map.subject_map.term_type:
                                subject = "_:" + subject_value
                            else:
                                if "http" not in subject_value:
                                    if base != "":
                                        subject = "<" + base + subject_value + ">"
                                    else:
                                        subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                                else:
                                    if is_valid_url_syntax(subject_value):
                                        subject = "<" + subject_value + ">"
                                    else:
                                        if base != "":
                                            subject = "<" + base + subject_value + ">"
                                        else:
                                            subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                        else:
                            subject = None

                else:
                    subject = None

            elif "constant" in triples_map.subject_map.subject_mapping_type:
                subject = "<" + triples_map.subject_map.value + ">"

            elif "function" in triples_map.subject_map.subject_mapping_type:
                subject = None
                if new_formulation == "no":
                    temp_dics = []
                    for triples_map_element in triples_map_list:
                        if triples_map_element.triples_map_id == triples_map.subject_map.value:
                            dic = create_dictionary(triples_map_element)
                            current_func = {"output_name":"OUTPUT", 
                                            "inputs":dic["inputs"], 
                                            "function":dic["executes"],
                                            "func_par":dic}
                            for inputs in dic["inputs"]:
                                temp_dic = {}
                                if "reference function" in inputs:
                                    temp_dic = {"inputs":dic["inputs"], 
                                                    "function":dic["executes"],
                                                    "func_par":dic,
                                                    "id":triples_map_element.triples_map_id}
                                    if inner_function_exists(temp_dic, temp_dics):
                                        temp_dics.append(temp_dic)
                            if temp_dics:
                                func = inner_function(row,current_func,triples_map_list)
                                subject = "<" + encode_char(func) + ">"
                            else:
                                func = execute_function(row,current_func)
                                subject = "<" + encode_char(func) + ">"
                else:
                    func = None
                    for func_map in triples_map.func_map_list:
                        if func_map.func_map_id == triples_map.subject_map.value:
                            current_func = {"inputs":func_map.parameters, 
                                            "function":func_map.name}
                            inner_func = False
                            for param in func_map.parameters:
                                if "function" in func_map.parameters[param]["type"]:
                                    inner_func = True
                            if inner_func:
                                func = new_inner_function(row,triples_map.subject_map.value,triples_map)
                            else:
                                func = execute_function(row,None,current_func)
                    if triples_map.subject_map.func_result != None and func != None:
                        func = func[triples_map.subject_map.func_result]
                    if func != None:
                        if "http://" in func or "https://" in func:
                            subject = "<" + func + ">"
                        else:
                            subject = "<" + encode_char(func) + ">"
                    else:
                        subject = None
            elif "quoted triples map" in triples_map.subject_map.subject_mapping_type:
                for triples_map_element in triples_map_list:
                    if triples_map_element.triples_map_id == triples_map.subject_map.value:
                        if triples_map_element.data_source != triples_map.data_source:
                            if triples_map.subject_map.parent != None:
                                if ("quoted_" + triples_map_element.triples_map_id + "_" + triples_map.subject_map.child) not in join_table:
                                    if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
                                        with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
                                            if str(triples_map_element.file_format).lower() == "csv":
                                                data = csv.DictReader(input_file_descriptor, delimiter=',')
                                                hash_maker(data, triples_map_element, triples_map.subject_map, "quoted", triples_map_list)
                                            else:
                                                pass
                                if row[triples_map.subject_map.child] in join_table["quoted_" + triples_map_element.triples_map_id + "_" + triples_map.subject_map.child]:
                                    subject_list = join_table["quoted_" + triples_map_element.triples_map_id + "_" + triples_map.subject_map.child][row[triples_map.subject_map.child]]
                        else:
                            subject_list = inner_semantify_file(triples_map_element, triples_map_list, delimiter, row, base)
                        subject = None
            else:
                if triples_map.subject_map.condition == "":

                    try:
                        subject = "\"" + triples_map.subject_map.value + "\""
                    except:
                        subject = None

                else:
                    #	field, condition = condition_separetor(triples_map.subject_map.condition)
                    #	if row[field] == condition:
                    try:
                        subject = "\"" + triples_map.subject_map.value + "\""
                    except:
                        subject = None

            if mapping_partitions == "yes":
                if triples_map_id in generated_subjects:
                    if subject_attr in generated_subjects[triples_map_id]:
                        pass
                    else:
                        generated_subjects[triples_map_id][subject_attr] = subject
                else:
                    generated_subjects[triples_map_id] = {subject_attr: subject}

        if triples_map.subject_map.rdf_class != [None] and subject != None:
            predicate = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
            for rdf_class in triples_map.subject_map.rdf_class:
                if rdf_class != None and rdf_class != "None" and ("str" == type(rdf_class).__name__ or "URIRef" == type(rdf_class).__name__):
                    obj = "<{}>".format(rdf_class)
                    rdf_type = subject + " " + predicate + " " + obj + ".\n"
                    for graph in triples_map.subject_map.graph:
                        if graph != None and "defaultGraph" not in graph:
                            if "{" in graph:
                                rdf_type = rdf_type[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject",
                                                                                      ignore,
                                                                                      triples_map.iterator) + ">.\n"
                                dictionary_table_update(
                                    "<" + string_substitution(graph, "{(.+?)}", row, "subject", ignore,
                                                              triples_map.iterator) + ">")
                            else:
                                rdf_type = rdf_type[:-2] + " <" + graph + ">.\n"
                                dictionary_table_update("<" + graph + ">")
                    if no_inner_cycle:
                        if duplicate == "yes":
                            dictionary_table_update(subject)
                            dictionary_table_update(obj)
                            dictionary_table_update(predicate + "_" + obj)
                            if dic_table[predicate + "_" + obj] not in g_triples:
                                if output_format.lower() == "n-triples":
                                    output_file_descriptor.write(rdf_type)
                                else:
                                    output_file_descriptor.write(subject + " a " + determine_prefix(obj))
                                g_triples.update(
                                    {dic_table[predicate + "_" + obj]: {dic_table[subject] + "_" + dic_table[obj]: ""}})
                                i += 1
                            elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                dic_table[predicate + "_" + obj]]:
                                if output_format.lower() == "n-triples":
                                    output_file_descriptor.write(rdf_type)
                                else:
                                    output_file_descriptor.write(subject + " a " + determine_prefix(obj))
                                g_triples[dic_table[predicate + "_" + obj]].update(
                                    {dic_table[subject] + "_" + dic_table[obj]: ""})
                                i += 1
                            else:
                                duplicate_type = True
                        else:
                            if output_format.lower() == "n-triples":
                                output_file_descriptor.write(rdf_type)
                            else:
                                output_file_descriptor.write(subject + " a " + determine_prefix(obj))
                            i += 1

        if output_format.lower() == "turtle" and len(triples_map.predicate_object_maps_list) == 0:
            output_file_descriptor.write(".\n")

        for predicate_object_map in triples_map.predicate_object_maps_list:
            if constant_predicate:
                if predicate_object_map.predicate_map.mapping_type == "constant" or predicate_object_map.predicate_map.mapping_type == "constant shortcut":
                    if output_format.lower() == "n-triples":
                        predicate = "<" + predicate_object_map.predicate_map.value + ">"
                    else:
                        predicate = determine_prefix(predicate_object_map.predicate_map.value)
                elif predicate_object_map.predicate_map.mapping_type == "template":
                    if predicate_object_map.predicate_map.condition != "":
                        # field, condition = condition_separetor(predicate_object_map.predicate_map.condition)
                        # if row[field] == condition:
                        try:
                            predicate = "<" + string_substitution(predicate_object_map.predicate_map.value, "{(.+?)}",
                                                                  row, "predicate", ignore, triples_map.iterator) + ">"
                        except:
                            predicate = None
                    # else:
                    #	predicate = None
                    else:
                        try:
                            predicate = "<" + string_substitution(predicate_object_map.predicate_map.value, "{(.+?)}",
                                                                  row, "predicate", ignore, triples_map.iterator) + ">"
                        except:
                            predicate = None
                elif predicate_object_map.predicate_map.mapping_type == "reference":
                    if predicate_object_map.predicate_map.condition != "":
                        # field, condition = condition_separetor(predicate_object_map.predicate_map.condition)
                        # if row[field] == condition:
                        predicate = string_substitution(predicate_object_map.predicate_map.value, ".+", row,
                                                        "predicate", ignore, triples_map.iterator)
                    # else:
                    #	predicate = None
                    else:
                        predicate = string_substitution(predicate_object_map.predicate_map.value, ".+", row,
                                                        "predicate", ignore, triples_map.iterator)
                    predicate = "<" + predicate[1:-1] + ">"
                elif predicate_object_map.predicate_map.mapping_type == "function":
                    if new_formulation == "yes":
                        func = None
                        for func_map in triples_map.func_map_list:
                            if func_map.func_map_id == predicate_object_map.predicate_map.value:
                                current_func = {"inputs":func_map.parameters, 
                                                "function":func_map.name}
                                inner_func = False
                                for param in func_map.parameters:
                                    if "function" in func_map.parameters[param]["type"]:
                                        inner_func = True
                                if inner_func:
                                    func = new_inner_function(row,predicate_object_map.predicate_map.value,triples_map)
                                else:
                                    func = execute_function(row,None,current_func)
                        if predicate_object_map.predicate_map.func_result != None and func != None:
                            func = func[predicate_object_map.predicate_map.func_result]
                        if None != func:
                            predicate = "<" + func + ">"
                        else:
                            predicate = None
                    else:
                        predicate = None
                else:
                    predicate = None

            if predicate_object_map.object_map.mapping_type == "constant" or predicate_object_map.object_map.mapping_type == "constant shortcut":
                if "/" in predicate_object_map.object_map.value:
                    object = "<" + predicate_object_map.object_map.value + ">"
                else:
                    object = "\"" + predicate_object_map.object_map.value + "\""
                if predicate_object_map.object_map.datatype != None:
                    if output_format.lower() == "n-triples":
                        object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
                    else:
                        object = "\"" + object[1:-1] + "\"" + "^^{}".format(
                            determine_prefix(predicate_object_map.object_map.datatype))
                elif predicate_object_map.object_map.datatype_map != None:
                    datatype_value = string_substitution(predicate_object_map.object_map.datatype_map, "{(.+?)}", row,
                                                           "object", ignore, triples_map.iterator)
                    if "http" in datatype_value:
                       object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(datatype_value)
                    else:
                       object = "\"" + object[1:-1] + "\"" + "^^<{}>".format("http://example.com/base/" + datatype_value)
                elif predicate_object_map.object_map.language != None:
                    if "spanish" == predicate_object_map.object_map.language or "es" == predicate_object_map.object_map.language:
                        object += "@es"
                    elif "english" == predicate_object_map.object_map.language or "en" == predicate_object_map.object_map.language:
                        object += "@en"
                    elif len(predicate_object_map.object_map.language) == 2:
                        object += "@" + predicate_object_map.object_map.language
                    else:
                        object = None
                elif predicate_object_map.object_map.language_map != None:
                    lang = string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object",
                                               ignore, triples_map.iterator)
                    if lang != None:
                        object += "@" + string_substitution(predicate_object_map.object_map.language_map, ".+", row,
                                                            "object", ignore, triples_map.iterator)[1:-1]

            elif predicate_object_map.object_map.mapping_type == "template":
                try:
                    if predicate_object_map.object_map.term is None:
                        object = "<" + string_substitution(predicate_object_map.object_map.value, "{(.+?)}", row,
                                                           "object", ignore, triples_map.iterator) + ">"
                    elif "IRI" in predicate_object_map.object_map.term:
                        object = "<" + string_substitution(predicate_object_map.object_map.value, "{(.+?)}", row,
                                                           "object", ignore, triples_map.iterator) + ">"
                    elif "BlankNode" in predicate_object_map.object_map.term:
                        object = "_:" + string_substitution(predicate_object_map.object_map.value, "{(.+?)}", row,
                                                            "object", ignore, triples_map.iterator)
                        if "/" in object:
                            object = object.replace("/", "2F")
                            if blank_message:
                                logger.warning("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                                blank_message = False
                        if "." in object:
                            object = object.replace(".", "2E")
                        object = encode_char(object)
                    else:
                        object = "\"" + string_substitution(predicate_object_map.object_map.value, "{(.+?)}", row,
                                                            "object", ignore, triples_map.iterator) + "\""
                        if predicate_object_map.object_map.datatype != None:
                            object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(
                                predicate_object_map.object_map.datatype)
                        elif predicate_object_map.object_map.datatype_map != None:
                            datatype_value = string_substitution(predicate_object_map.object_map.datatype_map, "{(.+?)}", row,
                                                                   "object", ignore, triples_map.iterator)
                            if "http" in datatype_value:
                               object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(datatype_value)
                            else:
                               object = "\"" + object[1:-1] + "\"" + "^^<{}>".format("http://example.com/base/" + datatype_value) 
                        elif predicate_object_map.object_map.language != None:
                            if "spanish" == predicate_object_map.object_map.language or "es" == predicate_object_map.object_map.language:
                                object += "@es"
                            elif "english" == predicate_object_map.object_map.language or "en" == predicate_object_map.object_map.language:
                                object += "@en"
                            elif len(predicate_object_map.object_map.language) == 2:
                                object += "@" + predicate_object_map.object_map.language
                            else:
                                object = None
                        elif predicate_object_map.object_map.language_map != None:
                            lang = string_substitution(predicate_object_map.object_map.language_map, ".+", row,
                                                       "object", ignore, triples_map.iterator)
                            if lang != None:
                                object += "@" + string_substitution(predicate_object_map.object_map.language_map, ".+",
                                                                    row, "object", ignore, triples_map.iterator)[1:-1]
                except TypeError:
                    object = None
            elif predicate_object_map.object_map.mapping_type == "reference":
                object = string_substitution(predicate_object_map.object_map.value, ".+", row, "object", ignore,
                                             triples_map.iterator)
                if object != None:
                    if "\\" in object[1:-1]:
                        object = "\"" + object[1:-1].replace("\\", "\\\\") + "\""
                    if "'" in object[1:-1]:
                        object = "\"" + object[1:-1].replace("'", "\\\\'") + "\""
                    if "\"" in object[1:-1]:
                        object = "\"" + object[1:-1].replace("\"", "\\\"") + "\""
                    if "\n" in object:
                        object = object.replace("\n", "\\n")
                    if predicate_object_map.object_map.datatype != None:
                        if output_format.lower() == "n-triples":
                            object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(
                                predicate_object_map.object_map.datatype)
                        else:
                            object = "\"" + object[1:-1] + "\"" + "^^{}".format(
                                determine_prefix(predicate_object_map.object_map.datatype))
                    elif predicate_object_map.object_map.datatype_map != None:
                        datatype_value = string_substitution(predicate_object_map.object_map.datatype_map, "{(.+?)}", row,
                                                               "object", ignore, triples_map.iterator)
                        if "http" in datatype_value:
                           object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(datatype_value)
                        else:
                           object = "\"" + object[1:-1] + "\"" + "^^<{}>".format("http://example.com/base/" + datatype_value) 
                    elif predicate_object_map.object_map.language != None:
                        if "spanish" == predicate_object_map.object_map.language or "es" == predicate_object_map.object_map.language:
                            object += "@es"
                        elif "english" == predicate_object_map.object_map.language or "en" == predicate_object_map.object_map.language:
                            object += "@en"
                        elif len(predicate_object_map.object_map.language) == 2:
                            object += "@" + predicate_object_map.object_map.language
                        else:
                            object = None
                    elif predicate_object_map.object_map.language_map != None:
                        lang = string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object",
                                                   ignore, triples_map.iterator)
                        if lang != None:
                            object += "@" + string_substitution(predicate_object_map.object_map.language_map, ".+", row,
                                                                "object", ignore, triples_map.iterator)[1:-1]
                    elif predicate_object_map.object_map.term != None:
                        if "IRI" in predicate_object_map.object_map.term:
                            if " " not in object:
                                object = "\"" + object[1:-1].replace("\\\\'", "'") + "\""
                                object = "<" + encode_char(object[1:-1]) + ">"
                            else:
                                object = None
                        elif "BlankNode" in predicate_object_map.object_map.term:
                            if " " not in object:
                                object = "_:" + object[1:-1]
                            else:
                                object = None
            elif predicate_object_map.object_map.mapping_type == "parent triples map":
                if subject != None:
                    for triples_map_element in triples_map_list:
                        if triples_map_element.triples_map_id == predicate_object_map.object_map.value:
                            if triples_map_element.data_source != triples_map.data_source:
                                if len(predicate_object_map.object_map.child) == 1:
                                    if (triples_map_element.triples_map_id + "_" +
                                        predicate_object_map.object_map.child[0]) not in join_table:
                                        if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
                                            if "http" in triples_map_element.data_source:
                                                if triples_map_element.file_format == "JSONPath":
                                                    response = urlopen(triples_map_element.data_source)
                                                    data = json.loads(response.read())
                                                    if triples_map_element.iterator:
                                                        if triples_map_element.iterator != "None" and triples_map_element.iterator != "$.[*]" and triples_map_element.iterator != "[*]" and triples_map_element.iterator != "[*]":
                                                            join_iterator(data, triples_map_element.iterator,
                                                                          triples_map_element,
                                                                          predicate_object_map.object_map, triples_map_list)
                                                        else:
                                                            if isinstance(data, list):
                                                                hash_maker(data, triples_map_element,
                                                                           predicate_object_map.object_map,"", triples_map_list)
                                                            elif len(data) < 2:
                                                                hash_maker(data[list(data.keys())[0]],
                                                                           triples_map_element,
                                                                           predicate_object_map.object_map,"", triples_map_list)
                                                    else:
                                                        if isinstance(data, list):
                                                            hash_maker(data, triples_map_element,
                                                                       predicate_object_map.object_map,"", triples_map_list)
                                                        elif len(data) < 2:
                                                            hash_maker(data[list(data.keys())[0]], triples_map_element,
                                                                       predicate_object_map.object_map,"", triples_map_list)
                                            else:
                                                with open(str(triples_map_element.data_source),
                                                          "r") as input_file_descriptor:
                                                    if str(triples_map_element.file_format).lower() == "csv":
                                                        reader = pd.read_csv(str(triples_map_element.data_source),
                                                                             dtype=str, encoding="latin-1")
                                                        reader = reader.where(pd.notnull(reader), None)
                                                        reader = reader.drop_duplicates(keep='first')
                                                        data = reader.to_dict(orient='records')
                                                        hash_maker(data, triples_map_element,
                                                                   predicate_object_map.object_map,"", triples_map_list)
                                                    else:
                                                        data = json.load(input_file_descriptor)
                                                        if triples_map_element.iterator:
                                                            if triples_map_element.iterator != "None" and triples_map_element.iterator != "$.[*]" and triples_map_element.iterator != "[*]" and triples_map_element.iterator != "[*]":
                                                                join_iterator(data, triples_map_element.iterator,
                                                                              triples_map_element,
                                                                              predicate_object_map.object_map, triples_map_list)
                                                            else:
                                                                if isinstance(data, list):
                                                                    hash_maker(data, triples_map_element,
                                                                               predicate_object_map.object_map,"", triples_map_list)
                                                                elif len(data) < 2:
                                                                    hash_maker(data[list(data.keys())[0]],
                                                                               triples_map_element,
                                                                               predicate_object_map.object_map,"", triples_map_list)
                                                        else:
                                                            if isinstance(data, list):
                                                                hash_maker(data, triples_map_element,
                                                                           predicate_object_map.object_map,"", triples_map_list)
                                                            elif len(data) < 2:
                                                                hash_maker(data[list(data.keys())[0]],
                                                                           triples_map_element,
                                                                           predicate_object_map.object_map,"", triples_map_list)

                                        elif triples_map_element.file_format == "XPath":
                                            with open(str(triples_map_element.data_source),
                                                      "r") as input_file_descriptor:
                                                child_tree = ET.parse(input_file_descriptor)
                                                child_root = child_tree.getroot()
                                                parent_map = {c: p for p in child_tree.iter() for c in p}
                                                namespace = dict([node for _, node in
                                                                  ET.iterparse(str(triples_map_element.data_source),
                                                                               events=['start-ns'])])
                                                hash_maker_xml(child_root, triples_map_element,
                                                               predicate_object_map.object_map, parent_map, namespace)
                                        else:
                                            db = connector.connect(host=host, port=int(port), user=user,
                                                                   password=password)
                                            cursor = db.cursor(buffered=True)
                                            cursor.execute("use " + datab)
                                            if triples_map_element.query != "None":
                                                cursor.execute(triples_map_element.query)
                                            else:
                                                database, query_list = translate_sql(triples_map_element)
                                                for query in query_list:
                                                    cursor.execute(query)
                                            hash_maker_array(cursor, triples_map_element,
                                                             predicate_object_map.object_map)

                                    if sublist(predicate_object_map.object_map.child, row.keys()):
                                        if child_list_value(predicate_object_map.object_map.child, row) in join_table[
                                            triples_map_element.triples_map_id + "_" + child_list(
                                                    predicate_object_map.object_map.child)]:
                                            object_list = join_table[
                                                triples_map_element.triples_map_id + "_" + child_list(
                                                    predicate_object_map.object_map.child)][
                                                child_list_value(predicate_object_map.object_map.child, row)]
                                        else:
                                            if no_update:
                                                if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
                                                    if "http" in triples_map_element.data_source:
                                                        if triples_map_element.file_format == "JSONPath":
                                                            response = urlopen(triples_map_element.data_source)
                                                            data = json.loads(response.read())
                                                            if triples_map_element.iterator:
                                                                if triples_map_element.iterator != "None" and triples_map_element.iterator != "$.[*]" and triples_map_element.iterator != "[*]":
                                                                    join_iterator(data, triples_map_element.iterator,
                                                                                  triples_map_element,
                                                                                  predicate_object_map.object_map,
                                                                                  triples_map_list)
                                                                else:
                                                                    if isinstance(data, list):
                                                                        hash_maker(data, triples_map_element,
                                                                                   predicate_object_map.object_map,"", triples_map_list)
                                                                    elif len(data) < 2:
                                                                        hash_maker(data[list(data.keys())[0]],
                                                                                   triples_map_element,
                                                                                   predicate_object_map.object_map,"", triples_map_list)
                                                            else:
                                                                if isinstance(data, list):
                                                                    hash_maker(data, triples_map_element,
                                                                               predicate_object_map.object_map,"", triples_map_list)
                                                                elif len(data) < 2:
                                                                    hash_maker(data[list(data.keys())[0]],
                                                                               triples_map_element,
                                                                               predicate_object_map.object_map,"", triples_map_list)
                                                    else:
                                                        with open(str(triples_map_element.data_source),
                                                                  "r") as input_file_descriptor:
                                                            if str(triples_map_element.file_format).lower() == "csv":
                                                                reader = pd.read_csv(
                                                                    str(triples_map_element.data_source), dtype=str,
                                                                    encoding="latin-1")
                                                                reader = reader.where(pd.notnull(reader), None)
                                                                reader = reader.drop_duplicates(keep='first')
                                                                data = reader.to_dict(orient='records')
                                                                hash_update(data, triples_map_element,
                                                                            predicate_object_map.object_map,
                                                                            triples_map_element.triples_map_id + "_" +
                                                                            predicate_object_map.object_map.child[0])
                                                            else:
                                                                data = json.load(input_file_descriptor)
                                                                if triples_map_element.iterator:
                                                                    if triples_map_element.iterator != "None" and triples_map_element.iterator != "$.[*]" and triples_map_element.iterator != "[*]":
                                                                        join_iterator(data,
                                                                                      triples_map_element.iterator,
                                                                                      triples_map_element,
                                                                                      predicate_object_map.object_map,
                                                                                      triples_map_list)
                                                                    else:
                                                                        if isinstance(data, list):
                                                                            hash_maker(data, triples_map_element,
                                                                                       predicate_object_map.object_map,"", triples_map_list)
                                                                        elif len(data) < 2:
                                                                            hash_maker(data[list(data.keys())[0]],
                                                                                       triples_map_element,
                                                                                       predicate_object_map.object_map,"", triples_map_list)
                                                                else:
                                                                    if isinstance(data, list):
                                                                        hash_maker(data, triples_map_element,
                                                                                   predicate_object_map.object_map,"", triples_map_list)
                                                                    elif len(data) < 2:
                                                                        hash_maker(data[list(data.keys())[0]],
                                                                                   triples_map_element,
                                                                                   predicate_object_map.object_map,"", triples_map_list)
                                                if child_list_value(predicate_object_map.object_map.child, row) in \
                                                        join_table[triples_map_element.triples_map_id + "_" +
                                                                   predicate_object_map.object_map.child[0]]:
                                                    object_list = join_table[triples_map_element.triples_map_id + "_" +
                                                                             predicate_object_map.object_map.child[0]][
                                                        row[predicate_object_map.object_map.child[0]]]
                                                else:
                                                    object_list = []
                                                no_update = False
                                    object = None
                                else:
                                    if (triples_map_element.triples_map_id + "_" + child_list(
                                            predicate_object_map.object_map.child)) not in join_table:
                                        if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
                                            if "http" in triples_map_element.data_source:
                                                if triples_map_element.file_format == "JSONPath":
                                                    response = urlopen(triples_map_element.data_source)
                                                    data = json.loads(response.read())
                                                    if isinstance(data, list):
                                                        hash_maker_list(data, triples_map_element,
                                                                        predicate_object_map.object_map)
                                                    elif len(data) < 2:
                                                        hash_maker_list(data[list(data.keys())[0]], triples_map_element,
                                                                        predicate_object_map.object_map)
                                            else:
                                                with open(str(triples_map_element.data_source),
                                                          "r") as input_file_descriptor:
                                                    if str(triples_map_element.file_format).lower() == "csv":
                                                        data = csv.DictReader(input_file_descriptor,
                                                                              delimiter=delimiter)
                                                        hash_maker_list(data, triples_map_element,
                                                                        predicate_object_map.object_map)
                                                    else:
                                                        data = json.load(input_file_descriptor)
                                                        if isinstance(data, list):
                                                            hash_maker_list(data, triples_map_element,
                                                                            predicate_object_map.object_map)
                                                        elif len(data) < 2:
                                                            hash_maker_list(data[list(data.keys())[0]],
                                                                            triples_map_element,
                                                                            predicate_object_map.object_map)

                                        elif triples_map_element.file_format == "XPath":
                                            with open(str(triples_map_element.data_source),
                                                      "r") as input_file_descriptor:
                                                child_tree = ET.parse(input_file_descriptor)
                                                child_root = child_tree.getroot()
                                                parent_map = {c: p for p in child_tree.iter() for c in p}
                                                namespace = dict([node for _, node in
                                                                  ET.iterparse(str(triples_map_element.data_source),
                                                                               events=['start-ns'])])
                                                hash_maker_xml(child_root, triples_map_element,
                                                               predicate_object_map.object_map, parent_map, namespace)
                                        else:
                                            db = connector.connect(host=host, port=int(port), user=user,
                                                                   password=password)
                                            cursor = db.cursor(buffered=True)
                                            cursor.execute("use " + datab)
                                            if triples_map_element.query != "None":
                                                cursor.execute(triples_map_element.query)
                                            else:
                                                database, query_list = translate_sql(triples_map_element)
                                                for query in query_list:
                                                    cursor.execute(query)
                                            hash_maker_array(cursor, triples_map_element,
                                                             predicate_object_map.object_map)
                                    if sublist(predicate_object_map.object_map.child, row.keys()):
                                        if child_list_value(predicate_object_map.object_map.child, row) in join_table[
                                            triples_map_element.triples_map_id + "_" + child_list(
                                                    predicate_object_map.object_map.child)]:
                                            object_list = join_table[
                                                triples_map_element.triples_map_id + "_" + child_list(
                                                    predicate_object_map.object_map.child)][
                                                child_list_value(predicate_object_map.object_map.child, row)]
                                        else:
                                            object_list = []
                                    object = None
                            else:
                                if predicate_object_map.object_map.parent != None:
                                    if (triples_map_element.triples_map_id + "_" + child_list(
                                            predicate_object_map.object_map.child)) not in join_table:
                                        with open(str(triples_map_element.data_source),
                                                  "r") as input_file_descriptor:
                                            if str(triples_map_element.file_format).lower() == "csv":
                                                parent_data = csv.DictReader(input_file_descriptor,
                                                                             delimiter=delimiter)
                                                hash_maker_list(parent_data, triples_map_element,
                                                                predicate_object_map.object_map)
                                            else:
                                                parent_data = json.load(input_file_descriptor)
                                                if isinstance(parent_data, list):
                                                    hash_maker_list(parent_data, triples_map_element,
                                                                    predicate_object_map.object_map)
                                                else:
                                                    hash_maker_list(parent_data[list(parent_data.keys())[0]],
                                                                    triples_map_element,
                                                                    predicate_object_map.object_map)
                                    if sublist(predicate_object_map.object_map.child, row.keys()):
                                        if child_list_value(predicate_object_map.object_map.child, row) in \
                                                join_table[triples_map_element.triples_map_id + "_" + child_list(
                                                        predicate_object_map.object_map.child)]:
                                            object_list = join_table[
                                                triples_map_element.triples_map_id + "_" + child_list(
                                                    predicate_object_map.object_map.child)][
                                                child_list_value(predicate_object_map.object_map.child, row)]
                                        else:
                                            object_list = []
                                    object = None
                                else:
                                    try:
                                        object = "<" + string_substitution(triples_map_element.subject_map.value,
                                                                           "{(.+?)}", row, "object", ignore,
                                                                           triples_map.iterator) + ">"
                                    except TypeError:
                                        object = None
                            break
                        else:
                            continue
                else:
                    object = None
            elif predicate_object_map.object_map.mapping_type == "reference function":
                object = None
                if new_formulation == "no":
                    temp_dics = []
                    for triples_map_element in triples_map_list:
                        if triples_map_element.triples_map_id == predicate_object_map.object_map.value:
                            dic = create_dictionary(triples_map_element)
                            current_func = {"inputs":dic["inputs"], 
                                            "function":dic["executes"],
                                            "func_par":dic}
                            for inputs in dic["inputs"]:
                                temp_dic = {}
                                if "reference function" in inputs:
                                    temp_dic = {"inputs":dic["inputs"], 
                                                    "function":dic["executes"],
                                                    "func_par":dic,
                                                    "id":triples_map_element.triples_map_id}
                                    if inner_function_exists(temp_dic, temp_dics):
                                        temp_dics.append(temp_dic)
                            if temp_dics:
                                func = inner_function(row,current_func,triples_map_list)
                                if predicate_object_map.object_map.term is not None:
                                    if "IRI" in predicate_object_map.object_map.term:
                                        object = "<" + encode_char(func) + ">"
                                else:
                                    if "" != func:
                                        object = "\"" + func + "\""
                                    else:
                                        object = None
                            else:
                                if predicate_object_map.object_map.term is not None:
                                    func = execute_function(row,None,current_func)
                                    if "IRI" in predicate_object_map.object_map.term:
                                        object = "<" + encode_char(func) + ">"
                                else:
                                    func = execute_function(row,None,current_func)
                                    if "" != func:
                                        object = "\"" + func + "\""
                                    else:
                                        object = None
                else:
                    func = None
                    for func_map in triples_map.func_map_list:
                        if func_map.func_map_id == predicate_object_map.object_map.value:
                            current_func = {"inputs":func_map.parameters, 
                                            "function":func_map.name}
                            inner_func = False
                            for param in func_map.parameters:
                                if "function" in func_map.parameters[param]["type"]:
                                    inner_func = True
                            if inner_func:
                                func = new_inner_function(row,predicate_object_map.object_map.value,triples_map)
                            else:
                                func = execute_function(row,None,current_func)
                    if predicate_object_map.object_map.func_result != None and func != None:
                        func = func[predicate_object_map.object_map.func_result]
                    if predicate_object_map.object_map.term is not None:
                        if func != None:
                            if "IRI" in predicate_object_map.object_map.term:
                                if "http://" in func.lower() or "https://" in func.lower():
                                    object = "<" + func + ">"
                                else:
                                    object = "<" + encode_char(func) + ">"
                        else:
                            object = None
                    else:
                        if None != func:
                            object = "\"" + func + "\""
                        else:
                            object = None
            elif "quoted triples map" in predicate_object_map.object_map.mapping_type:
                for triples_map_element in triples_map_list:
                    if triples_map_element.triples_map_id == predicate_object_map.object_map.value:
                        if triples_map_element.data_source != triples_map.data_source:
                            if predicate_object_map.object_map.parent != None:
                                if ("quoted_" + triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]) not in join_table:
                                    if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
                                        with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
                                            if str(triples_map_element.file_format).lower() == "csv":
                                                data = csv.DictReader(input_file_descriptor, delimiter=',')
                                                hash_maker(data, triples_map_element, predicate_object_map.object_map, "quoted", triples_map_list)
                                            else:
                                                pass
                                if row[predicate_object_map.object_map.child[0]] in join_table["quoted_" + triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]]:
                                    object_list = join_table["quoted_" + triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]][row[predicate_object_map.object_map.child[0]]]
                        else:
                            object_list = inner_semantify_file(triples_map_element, triples_map_list, delimiter, row, base)
                        object = None
            else:
                object = None

            if is_current_output_valid(triples_map.triples_map_id,predicate_object_map,current_logical_dump,logical_dump):
                if duplicate == "yes":
                    if predicate in general_predicates:
                        dictionary_table_update(predicate + "_" + predicate_object_map.object_map.value)
                    else:
                        dictionary_table_update(predicate)

                if output_format.lower() == "turtle" and triples_map.predicate_object_maps_list[
                    0] == predicate_object_map and not duplicate_type:
                    if triples_map.subject_map.rdf_class != [None]:
                        if len(triples_map.predicate_object_maps_list) > 1:
                            output_file_descriptor.write(";\n")
                        elif len(triples_map.predicate_object_maps_list) == 1:
                            if object == None and object_list == []:
                                output_file_descriptor.write(".\n")
                                end_turtle = "."
                            else:
                                output_file_descriptor.write(";\n")
                        elif len(triples_map.predicate_object_maps_list) == 0:
                            output_file_descriptor.write(".\n")
                            end_turtle = "."

                if end_turtle == ";":
                    if predicate != None and object != None and subject != None:
                        if duplicate == "yes":
                            if predicate in general_predicates:
                                if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                    output_file_descriptor.write(";\n")
                                elif object in dic_table and subject in dic_table:
                                    if dic_table[subject] + "_" + dic_table[object] not in g_triples[
                                    dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                        output_file_descriptor.write(";\n")
                                    else:
                                        if triples_map.predicate_object_maps_list[
                                        len(triples_map.predicate_object_maps_list) - 1] == predicate_object_map:
                                            output_file_descriptor.write(";\n")
                                            end_turtle = "."
                                elif object not in dic_table or subject not in dic_table:
                                    if triples_map.predicate_object_maps_list[
                                        len(triples_map.predicate_object_maps_list) - 1] == predicate_object_map:
                                        output_file_descriptor.write(";\n")
                                        end_turtle = "."
                                    else:
                                        output_file_descriptor.write(";\n")
                                else:
                                    if triples_map.predicate_object_maps_list[
                                        len(triples_map.predicate_object_maps_list) - 1] == predicate_object_map:
                                        output_file_descriptor.write(";\n")
                            else:
                                if dic_table[predicate] not in g_triples:
                                    output_file_descriptor.write(";\n")
                                elif object in dic_table:
                                    if dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
                                        output_file_descriptor.write(";\n")
                                    else:
                                        if triples_map.predicate_object_maps_list[
                                        len(triples_map.predicate_object_maps_list) - 1] == predicate_object_map:
                                            output_file_descriptor.write(";\n")
                                            end_turtle = "."
                                elif object not in dic_table or subject not in dic_table:
                                    if triples_map.predicate_object_maps_list[
                                        len(triples_map.predicate_object_maps_list) - 1] == predicate_object_map:
                                        output_file_descriptor.write(";\n")
                                        end_turtle = "."
                                    else:
                                        output_file_descriptor.write(";\n")
                                else:
                                    if triples_map.predicate_object_maps_list[
                                        len(triples_map.predicate_object_maps_list) - 1] == predicate_object_map:
                                        output_file_descriptor.write(";\n")
                                        end_turtle = "."
                        else:
                            output_file_descriptor.write(";\n")
                                
                    elif predicate != None and subject != None and object_list:
                        if triples_map.predicate_object_maps_list[
                            len(triples_map.predicate_object_maps_list) - 1] == predicate_object_map:
                            temp_end = "."
                            for obj in object_list:
                                if predicate in general_predicates:
                                    if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                        temp_end = ";"
                                    elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                        dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                        temp_end = ";"
                                else:
                                    if dic_table[predicate] not in g_triples:
                                        temp_end = ";"
                                    elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
                                        temp_end = ";"
                            if temp_end == ".":
                                output_file_descriptor.write(".\n\n")
                                end_turtle = "."
                            else:
                                output_file_descriptor.write(";\n")
                        else:
                            output_file_descriptor.write(";\n")
                    else:
                        if predicate == None or object == None or subject == None:
                            output_file_descriptor.write(".\n\n")
                            end_turtle = "."
                        elif predicate_object_map == triples_map.predicate_object_maps_list[
                            len(triples_map.predicate_object_maps_list) - 1]:
                            output_file_descriptor.write(".\n\n")
                            end_turtle = "."


                if predicate != None and object != None and subject != None:
                    for graph in triples_map.subject_map.graph:
                        triple = subject + " " + predicate + " " + object + ".\n"
                        if graph != None and "defaultGraph" not in graph:
                            if "{" in graph:
                                triple = triple[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject", ignore,
                                                                                  triples_map.iterator) + ">.\n"
                                dictionary_table_update("<" + string_substitution(graph, "{(.+?)}", row, "subject", ignore,
                                                                                  triples_map.iterator) + ">")
                            else:
                                triple = triple[:-2] + " <" + graph + ">.\n"
                                dictionary_table_update("<" + graph + ">")
                        if no_inner_cycle:
                            if predicate[1:-1] not in predicate_object_map.graph or graph != None or triples_map.subject_map.graph == [None]:
                                if duplicate == "yes":
                                    dictionary_table_update(subject)
                                    dictionary_table_update(object)
                                    if predicate in general_predicates:
                                        if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                            if output_format.lower() == "n-triples":
                                                output_file_descriptor.write(triple)
                                            else:
                                                end_turtle = turtle_print(subject, predicate, object, object_list, duplicate_type,
                                                                          predicate_object_map, triples_map, output_file_descriptor,
                                                                          generated)
                                            g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value]: {
                                                dic_table[subject] + "_" + dic_table[object]: ""}})
                                            i += 1
                                            generated += 1
                                        elif dic_table[subject] + "_" + dic_table[object] not in g_triples[
                                            dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                            if output_format.lower() == "n-triples":
                                                output_file_descriptor.write(triple)
                                            else:
                                                end_turtle = turtle_print(subject, predicate, object, object_list, duplicate_type,
                                                                          predicate_object_map, triples_map, output_file_descriptor,
                                                                          generated)
                                            g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update(
                                                {dic_table[subject] + "_" + dic_table[object]: ""})
                                            i += 1
                                            generated += 1
                                    else:
                                        if dic_table[predicate] not in g_triples:
                                            if output_format.lower() == "n-triples":
                                                output_file_descriptor.write(triple)
                                            else:
                                                end_turtle = turtle_print(subject, predicate, object, object_list, duplicate_type,
                                                                          predicate_object_map, triples_map, output_file_descriptor,
                                                                          generated)
                                            g_triples.update(
                                                {dic_table[predicate]: {dic_table[subject] + "_" + dic_table[object]: ""}})
                                            i += 1
                                            generated += 1
                                        elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
                                            if output_format.lower() == "n-triples":
                                                output_file_descriptor.write(triple)
                                            else:
                                                end_turtle = turtle_print(subject, predicate, object, object_list, duplicate_type,
                                                                          predicate_object_map, triples_map, output_file_descriptor,
                                                                          generated)
                                            g_triples[dic_table[predicate]].update(
                                                {dic_table[subject] + "_" + dic_table[object]: ""})
                                            i += 1
                                            generated += 1
                                else:
                                    if output_format.lower() == "n-triples":
                                        output_file_descriptor.write(triple)
                                    else:
                                        end_turtle = turtle_print(subject, predicate, object, object_list, duplicate_type,
                                                                  predicate_object_map, triples_map, output_file_descriptor,
                                                                  generated)
                                    i += 1
                                    generated += 1
                    if predicate[1:-1] in predicate_object_map.graph:
                        triple = subject + " " + predicate + " " + object + ".\n"
                        if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in \
                                predicate_object_map.graph[predicate[1:-1]]:
                            if "{" in predicate_object_map.graph[predicate[1:-1]]:
                                triple = triple[:-2] + " <" + string_substitution(
                                    predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, "subject", ignore,
                                    triples_map.iterator) + ">.\n"
                                dictionary_table_update(
                                    "<" + string_substitution(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row,
                                                              "subject", ignore, triples_map.iterator) + ">")
                            else:
                                triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
                                dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
                            if no_inner_cycle:
                                if duplicate == "yes":
                                    if predicate in general_predicates:
                                        if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                            if output_format.lower() == "n-triples":
                                                output_file_descriptor.write(triple)
                                            else:
                                                end_turtle = turtle_print(subject, predicate, object, object_list,
                                                                          duplicate_type, predicate_object_map, triples_map,
                                                                          output_file_descriptor, generated)
                                            g_triples.update({dic_table[
                                                                  predicate + "_" + predicate_object_map.object_map.value]: {
                                                dic_table[subject] + "_" + dic_table[object]: ""}})
                                            i += 1
                                            generated += 1
                                        elif dic_table[subject] + "_" + dic_table[object] not in g_triples[
                                            predicate + "_" + predicate_object_map.object_map.value]:
                                            if output_format.lower() == "n-triples":
                                                output_file_descriptor.write(triple)
                                            else:
                                                end_turtle = turtle_print(subject, predicate, object, object_list,
                                                                          duplicate_type, predicate_object_map, triples_map,
                                                                          output_file_descriptor, generated)
                                            g_triples[
                                                dic_table[predicate + "_" + predicate_object_map.object_map.value]].update(
                                                {dic_table[subject] + "_" + dic_table[object]: ""})
                                            i += 1
                                            generated += 1
                                    else:
                                        if dic_table[predicate] not in g_triples:
                                            if output_format.lower() == "n-triples":
                                                output_file_descriptor.write(triple)
                                            else:
                                                end_turtle = turtle_print(subject, predicate, object, object_list,
                                                                          duplicate_type, predicate_object_map, triples_map,
                                                                          output_file_descriptor, generated)
                                            g_triples.update(
                                                {dic_table[predicate]: {dic_table[subject] + "_" + dic_table[object]: ""}})
                                            i += 1
                                            generated += 1
                                        elif dic_table[subject] + "_" + dic_table[object] not in g_triples[
                                            dic_table[predicate]]:
                                            if output_format.lower() == "n-triples":
                                                output_file_descriptor.write(triple)
                                            else:
                                                end_turtle = turtle_print(subject, predicate, object, object_list,
                                                                          duplicate_type, predicate_object_map, triples_map,
                                                                          output_file_descriptor, generated)
                                            g_triples[dic_table[predicate]].update(
                                                {dic_table[subject] + "_" + dic_table[object]: ""})
                                            i += 1
                                            generated += 1
                                else:
                                    if output_format.lower() == "n-triples":
                                        output_file_descriptor.write(triple)
                                    else:
                                        end_turtle = turtle_print(subject, predicate, object, object_list, duplicate_type,
                                                                  predicate_object_map, triples_map, output_file_descriptor,
                                                                  generated)
                                    i += 1
                                    generated += 1
                elif predicate != None and subject != None and object_list:
                    for obj in object_list:
                        if obj != None:
                            for graph in triples_map.subject_map.graph:
                                if predicate_object_map.object_map.term != None:
                                    if "IRI" in predicate_object_map.object_map.term:
                                        triple = subject + " " + predicate + " <" + obj[1:-1] + ">.\n"
                                    else:
                                        triple = subject + " " + predicate + " " + obj + ".\n"
                                else:
                                    if "quoted triples map" in predicate_object_map.object_map.mapping_type:
                                        triple = subject + " " + predicate + " <<" + obj + ">>.\n"
                                    else:
                                        triple = subject + " " + predicate + " " + obj + ".\n"
                                if graph != None and "defaultGraph" not in graph:
                                    if "{" in graph:
                                        triple = triple[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject",
                                                                                          ignore,
                                                                                          triples_map.iterator) + ">.\n"
                                        dictionary_table_update(
                                            "<" + string_substitution(graph, "{(.+?)}", row, "subject", ignore,
                                                                      triples_map.iterator) + ">")
                                    else:
                                        triple = triple[:-2] + " <" + graph + ">.\n"
                                        dictionary_table_update("<" + graph + ">")
                                if no_inner_cycle:
                                    if predicate[1:-1] not in predicate_object_map.graph or graph != None or triples_map.subject_map.graph == [None]:        
                                        if duplicate == "yes":
                                            dictionary_table_update(subject)
                                            dictionary_table_update(obj)
                                            if predicate in general_predicates:
                                                if dic_table[
                                                    predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                                    if output_format.lower() == "n-triples":
                                                        output_file_descriptor.write(triple)
                                                    else:
                                                        end_turtle = turtle_print(subject, predicate, obj, object_list,
                                                                                  duplicate_type, predicate_object_map, triples_map,
                                                                                  output_file_descriptor, generated)
                                                    g_triples.update({dic_table[
                                                                          predicate + "_" + predicate_object_map.object_map.value]: {
                                                        dic_table[subject] + "_" + dic_table[obj]: ""}})
                                                    i += 1
                                                    generated += 1
                                                elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                                    dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                                    if output_format.lower() == "n-triples":
                                                        output_file_descriptor.write(triple)
                                                    else:
                                                        end_turtle = turtle_print(subject, predicate, obj, object_list,
                                                                                  duplicate_type, predicate_object_map, triples_map,
                                                                                  output_file_descriptor, generated)
                                                    g_triples[
                                                        dic_table[predicate + "_" + predicate_object_map.object_map.value]].update(
                                                        {dic_table[subject] + "_" + dic_table[obj]: ""})
                                                    i += 1
                                                    generated += 1
                                            else:
                                                if dic_table[predicate] not in g_triples:
                                                    if output_format.lower() == "n-triples":
                                                        output_file_descriptor.write(triple)
                                                    else:
                                                        end_turtle = turtle_print(subject, predicate, obj, object_list,
                                                                                  duplicate_type, predicate_object_map, triples_map,
                                                                                  output_file_descriptor, generated)
                                                    g_triples.update(
                                                        {dic_table[predicate]: {dic_table[subject] + "_" + dic_table[obj]: ""}})
                                                    i += 1
                                                    generated += 1
                                                elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                                    dic_table[predicate]]:
                                                    if output_format.lower() == "n-triples":
                                                        output_file_descriptor.write(triple)
                                                    else:
                                                        end_turtle = turtle_print(subject, predicate, obj, object_list,
                                                                                  duplicate_type, predicate_object_map, triples_map,
                                                                                  output_file_descriptor, generated)
                                                    g_triples[dic_table[predicate]].update(
                                                        {dic_table[subject] + "_" + dic_table[obj]: ""})
                                                    i += 1
                                                    generated += 1

                                        else:
                                            if output_format.lower() == "n-triples":
                                                output_file_descriptor.write(triple)
                                            else:
                                                end_turtle = turtle_print(subject, predicate, obj, object_list, duplicate_type,
                                                                          predicate_object_map, triples_map, output_file_descriptor,
                                                                          generated)
                                            i += 1
                                            generated += 1
                            if predicate[1:-1] in predicate_object_map.graph:
                                if predicate_object_map.object_map.term != None:
                                    if "IRI" in predicate_object_map.object_map.term:
                                        triple = subject + " " + predicate + " <" + obj[1:-1] + ">.\n"
                                    else:
                                        triple = subject + " " + predicate + " " + obj + ".\n"
                                else:
                                    triple = subject + " " + predicate + " " + obj + ".\n"
                                if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in \
                                        predicate_object_map.graph[predicate[1:-1]]:
                                    if "{" in predicate_object_map.graph[predicate[1:-1]]:
                                        triple = triple[:-2] + " <" + string_substitution(
                                            predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, "subject", ignore,
                                            triples_map.iterator) + ">.\n"
                                        dictionary_table_update(
                                            "<" + string_substitution(predicate_object_map.graph[predicate[1:-1]],
                                                                      "{(.+?)}", row, "subject", ignore,
                                                                      triples_map.iterator) + ">")
                                    else:
                                        triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
                                        dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
                                    if no_inner_cycle:
                                        if duplicate == "yes":
                                            if predicate in general_predicates:
                                                if dic_table[
                                                    predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                                    if output_format.lower() == "n-triples":
                                                        output_file_descriptor.write(triple)
                                                    else:
                                                        end_turtle = turtle_print(subject, predicate, obj, object_list,
                                                                                  duplicate_type, predicate_object_map,
                                                                                  triples_map, output_file_descriptor,
                                                                                  generated)
                                                    g_triples.update({dic_table[
                                                                          predicate + "_" + predicate_object_map.object_map.value]: {
                                                        dic_table[subject] + "_" + dic_table[obj]: ""}})
                                                    i += 1
                                                    generated += 1
                                                elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                                    dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                                    if output_format.lower() == "n-triples":
                                                        output_file_descriptor.write(triple)
                                                    else:
                                                        end_turtle = turtle_print(subject, predicate, obj, object_list,
                                                                                  duplicate_type, predicate_object_map,
                                                                                  triples_map, output_file_descriptor,
                                                                                  generated)
                                                    g_triples[dic_table[
                                                        predicate + "_" + predicate_object_map.object_map.value]].update(
                                                        {dic_table[subject] + "_" + dic_table[obj]: ""})
                                                    i += 1
                                                    generated += 1
                                            else:
                                                if dic_table[predicate] not in g_triples:
                                                    if output_format.lower() == "n-triples":
                                                        output_file_descriptor.write(triple)
                                                    else:
                                                        end_turtle = turtle_print(subject, predicate, obj, object_list,
                                                                                  duplicate_type, predicate_object_map,
                                                                                  triples_map, output_file_descriptor,
                                                                                  generated)
                                                    g_triples.update(
                                                        {dic_table[predicate]: {dic_table[subject] + "_" + dic_table[obj]: ""}})
                                                    i += 1
                                                    generated += 1
                                                elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                                    dic_table[predicate]]:
                                                    if output_format.lower() == "n-triples":
                                                        output_file_descriptor.write(triple)
                                                    else:
                                                        end_turtle = turtle_print(subject, predicate, obj, object_list,
                                                                                  duplicate_type, predicate_object_map,
                                                                                  triples_map, output_file_descriptor,
                                                                                  generated)
                                                    g_triples[dic_table[predicate]].update(
                                                        {dic_table[subject] + "_" + dic_table[obj]: ""})
                                                    i += 1
                                                    generated += 1
                                    else:
                                        if output_format.lower() == "n-triples":
                                            output_file_descriptor.write(triple)
                                        else:
                                            end_turtle = turtle_print(subject, predicate, obj, object_list, duplicate_type,
                                                                      predicate_object_map, triples_map,
                                                                      output_file_descriptor, generated)
                                        i += 1
                                        generated += 1
                    object_list = []
                elif predicate != None and subject_list and object != None:
                    dictionary_table_update(object)
                    for subj in subject_list:
                        if subj != None:
                            for graph in triples_map.subject_map.graph:
                                if predicate_object_map.object_map.term != None:
                                    if "IRI" in predicate_object_map.object_map.term:
                                        triple = "<<" + subj + ">> " + predicate + " <" + object[1:-1] + ">.\n"
                                    else:
                                        triple = "<<" + subj + ">> " + predicate + " " + object + ".\n"
                                else:
                                    triple = "<<" + subj + ">> " + predicate + " " + object + ".\n"
                                if graph != None and "defaultGraph" not in graph:
                                    if "{" in graph:
                                        triple = triple[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject",
                                                                                          ignore,
                                                                                          triples_map.iterator) + ">.\n"
                                        dictionary_table_update(
                                            "<" + string_substitution(graph, "{(.+?)}", row, "subject", ignore,
                                                                      triples_map.iterator) + ">")
                                    else:
                                        triple = triple[:-2] + " <" + graph + ">.\n"
                                        dictionary_table_update("<" + graph + ">")
                                if no_inner_cycle:
                                    if predicate[1:-1] not in predicate_object_map.graph or graph != None or triples_map.subject_map.graph == [None]:        
                                        if duplicate == "yes":
                                            dictionary_table_update(subj)
                                            if predicate in general_predicates:
                                                if dic_table[
                                                    predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                                    if output_format.lower() == "n-triples":
                                                        output_file_descriptor.write(triple)
                                                    else:
                                                        end_turtle = turtle_print(subj, predicate, object, object_list,
                                                                                  duplicate_type, predicate_object_map, triples_map,
                                                                                  output_file_descriptor, generated)
                                                    g_triples.update({dic_table[
                                                                          predicate + "_" + predicate_object_map.object_map.value]: {
                                                        dic_table[subj] + "_" + dic_table[object]: ""}})
                                                    i += 1
                                                    generated += 1
                                                elif dic_table[subj] + "_" + dic_table[object] not in g_triples[
                                                    dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                                    if output_format.lower() == "n-triples":
                                                        output_file_descriptor.write(triple)
                                                    else:
                                                        end_turtle = turtle_print(subj, predicate, object, object_list,
                                                                                  duplicate_type, predicate_object_map, triples_map,
                                                                                  output_file_descriptor, generated)
                                                    g_triples[
                                                        dic_table[predicate + "_" + predicate_object_map.object_map.value]].update(
                                                        {dic_table[subj] + "_" + dic_table[object]: ""})
                                                    i += 1
                                                    generated += 1
                                            else:
                                                if dic_table[predicate] not in g_triples:
                                                    if output_format.lower() == "n-triples":
                                                        output_file_descriptor.write(triple)
                                                    else:
                                                        end_turtle = turtle_print(subj, predicate, object, object_list,
                                                                                  duplicate_type, predicate_object_map, triples_map,
                                                                                  output_file_descriptor, generated)
                                                    g_triples.update(
                                                        {dic_table[predicate]: {dic_table[subj] + "_" + dic_table[object]: ""}})
                                                    i += 1
                                                    generated += 1
                                                elif dic_table[subj] + "_" + dic_table[object] not in g_triples[
                                                    dic_table[predicate]]:
                                                    if output_format.lower() == "n-triples":
                                                        output_file_descriptor.write(triple)
                                                    else:
                                                        end_turtle = turtle_print(subj, predicate, object, object_list,
                                                                                  duplicate_type, predicate_object_map, triples_map,
                                                                                  output_file_descriptor, generated)
                                                    g_triples[dic_table[predicate]].update(
                                                        {dic_table[subj] + "_" + dic_table[object]: ""})
                                                    i += 1
                                                    generated += 1

                                        else:
                                            if output_format.lower() == "n-triples":
                                                output_file_descriptor.write(triple)
                                            else:
                                                end_turtle = turtle_print(subj, predicate, obj, object_list, duplicate_type,
                                                                          predicate_object_map, triples_map, output_file_descriptor,
                                                                          generated)
                                            i += 1
                                            generated += 1
                            if predicate[1:-1] in predicate_object_map.graph:
                                if predicate_object_map.object_map.term != None:
                                    if "IRI" in predicate_object_map.object_map.term:
                                        triple = subj + " " + predicate + " <" + object[1:-1] + ">.\n"
                                    else:
                                        triple = subj + " " + predicate + " " + object + ".\n"
                                else:
                                    triple = subj + " " + predicate + " " + object + ".\n"
                                if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in \
                                        predicate_object_map.graph[predicate[1:-1]]:
                                    if "{" in predicate_object_map.graph[predicate[1:-1]]:
                                        triple = triple[:-2] + " <" + string_substitution(
                                            predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, "subject", ignore,
                                            triples_map.iterator) + ">.\n"
                                        dictionary_table_update(
                                            "<" + string_substitution(predicate_object_map.graph[predicate[1:-1]],
                                                                      "{(.+?)}", row, "subject", ignore,
                                                                      triples_map.iterator) + ">")
                                    else:
                                        triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
                                        dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
                                    if no_inner_cycle:
                                        if duplicate == "yes":
                                            if predicate in general_predicates:
                                                if dic_table[
                                                    predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                                    if output_format.lower() == "n-triples":
                                                        output_file_descriptor.write(triple)
                                                    else:
                                                        end_turtle = turtle_print(subj, predicate, object, object_list,
                                                                                  duplicate_type, predicate_object_map,
                                                                                  triples_map, output_file_descriptor,
                                                                                  generated)
                                                    g_triples.update({dic_table[
                                                                          predicate + "_" + predicate_object_map.object_map.value]: {
                                                        dic_table[subj] + "_" + dic_table[object]: ""}})
                                                    i += 1
                                                    generated += 1
                                                elif dic_table[subj] + "_" + dic_table[object] not in g_triples[
                                                    dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                                    if output_format.lower() == "n-triples":
                                                        output_file_descriptor.write(triple)
                                                    else:
                                                        end_turtle = turtle_print(subj, predicate, object, object_list,
                                                                                  duplicate_type, predicate_object_map,
                                                                                  triples_map, output_file_descriptor,
                                                                                  generated)
                                                    g_triples[dic_table[
                                                        predicate + "_" + predicate_object_map.object_map.value]].update(
                                                        {dic_table[subj] + "_" + dic_table[object]: ""})
                                                    i += 1
                                                    generated += 1
                                            else:
                                                if dic_table[predicate] not in g_triples:
                                                    if output_format.lower() == "n-triples":
                                                        output_file_descriptor.write(triple)
                                                    else:
                                                        end_turtle = turtle_print(subj, predicate, object, object_list,
                                                                                  duplicate_type, predicate_object_map,
                                                                                  triples_map, output_file_descriptor,
                                                                                  generated)
                                                    g_triples.update(
                                                        {dic_table[predicate]: {dic_table[subj] + "_" + dic_table[object]: ""}})
                                                    i += 1
                                                    generated += 1
                                                elif dic_table[subj] + "_" + dic_table[object] not in g_triples[
                                                    dic_table[predicate]]:
                                                    if output_format.lower() == "n-triples":
                                                        output_file_descriptor.write(triple)
                                                    else:
                                                        end_turtle = turtle_print(subj, predicate, object, object_list,
                                                                                  duplicate_type, predicate_object_map,
                                                                                  triples_map, output_file_descriptor,
                                                                                  generated)
                                                    g_triples[dic_table[predicate]].update(
                                                        {dic_table[subj] + "_" + dic_table[object]: ""})
                                                    i += 1
                                                    generated += 1
                                    else:
                                        if output_format.lower() == "n-triples":
                                            output_file_descriptor.write(triple)
                                        else:
                                            end_turtle = turtle_print(subj, predicate, object, object_list, duplicate_type,
                                                                      predicate_object_map, triples_map,
                                                                      output_file_descriptor, generated)
                                        i += 1
                                        generated += 1
                    subject_list = []
                elif predicate != None and subject_list and object_list:
                    for subj in subject_list:
                        for obj in object_list:
                            if obj != None and subj != None:
                                for graph in triples_map.subject_map.graph:
                                    if predicate_object_map.object_map.term != None:
                                        if "IRI" in predicate_object_map.object_map.term:
                                            triple = "<<" + subj + ">> " + predicate + " <" + obj[1:-1] + ">.\n"
                                        else:
                                            triple = "<<" + subj + ">> " + predicate + " " + obj + ".\n"
                                    else:
                                        if "quoted triples map" in predicate_object_map.object_map.mapping_type:
                                            triple = "<<" + subj + ">> " + predicate + " <<" + obj + ">>.\n"
                                        else:
                                            triple = "<<" + subj + ">> " + predicate + " " + obj + ".\n"
                                    if graph != None and "defaultGraph" not in graph:
                                        if "{" in graph:
                                            triple = triple[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject",
                                                                                              ignore,
                                                                                              triples_map.iterator) + ">.\n"
                                            dictionary_table_update(
                                                "<" + string_substitution(graph, "{(.+?)}", row, "subject", ignore,
                                                                          triples_map.iterator) + ">")
                                        else:
                                            triple = triple[:-2] + " <" + graph + ">.\n"
                                            dictionary_table_update("<" + graph + ">")
                                    if no_inner_cycle:
                                        if predicate[1:-1] not in predicate_object_map.graph or graph != None or triples_map.subject_map.graph == [None]:        
                                            if duplicate == "yes":
                                                dictionary_table_update(subj)
                                                dictionary_table_update(obj)
                                                if predicate in general_predicates:
                                                    if dic_table[
                                                        predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                                        if output_format.lower() == "n-triples":
                                                            output_file_descriptor.write(triple)
                                                        else:
                                                            end_turtle = turtle_print(subj, predicate, obj, object_list,
                                                                                      duplicate_type, predicate_object_map, triples_map,
                                                                                      output_file_descriptor, generated)
                                                        g_triples.update({dic_table[
                                                                              predicate + "_" + predicate_object_map.object_map.value]: {
                                                            dic_table[subj] + "_" + dic_table[obj]: ""}})
                                                        i += 1
                                                        generated += 1
                                                    elif dic_table[subj] + "_" + dic_table[obj] not in g_triples[
                                                        dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                                        if output_format.lower() == "n-triples":
                                                            output_file_descriptor.write(triple)
                                                        else:
                                                            end_turtle = turtle_print(subj, predicate, obj, object_list,
                                                                                      duplicate_type, predicate_object_map, triples_map,
                                                                                      output_file_descriptor, generated)
                                                        g_triples[
                                                            dic_table[predicate + "_" + predicate_object_map.object_map.value]].update(
                                                            {dic_table[subj] + "_" + dic_table[obj]: ""})
                                                        i += 1
                                                        generated += 1
                                                else:
                                                    if dic_table[predicate] not in g_triples:
                                                        if output_format.lower() == "n-triples":
                                                            output_file_descriptor.write(triple)
                                                        else:
                                                            end_turtle = turtle_print(subj, predicate, obj, object_list,
                                                                                      duplicate_type, predicate_object_map, triples_map,
                                                                                      output_file_descriptor, generated)
                                                        g_triples.update(
                                                            {dic_table[predicate]: {dic_table[subj] + "_" + dic_table[obj]: ""}})
                                                        i += 1
                                                        generated += 1
                                                    elif dic_table[subj] + "_" + dic_table[obj] not in g_triples[
                                                        dic_table[predicate]]:
                                                        if output_format.lower() == "n-triples":
                                                            output_file_descriptor.write(triple)
                                                        else:
                                                            end_turtle = turtle_print(subj, predicate, obj, object_list,
                                                                                      duplicate_type, predicate_object_map, triples_map,
                                                                                      output_file_descriptor, generated)
                                                        g_triples[dic_table[predicate]].update(
                                                            {dic_table[subj] + "_" + dic_table[obj]: ""})
                                                        i += 1
                                                        generated += 1

                                            else:
                                                if output_format.lower() == "n-triples":
                                                    output_file_descriptor.write(triple)
                                                else:
                                                    end_turtle = turtle_print(subj, predicate, obj, object_list, duplicate_type,
                                                                              predicate_object_map, triples_map, output_file_descriptor,
                                                                              generated)
                                                i += 1
                                                generated += 1
                                if predicate[1:-1] in predicate_object_map.graph:
                                    if predicate_object_map.object_map.term != None:
                                        if "IRI" in predicate_object_map.object_map.term:
                                            triple = subj + " " + predicate + " <" + obj[1:-1] + ">.\n"
                                        else:
                                            triple = subj + " " + predicate + " " + obj + ".\n"
                                    else:
                                        triple = subj + " " + predicate + " " + obj + ".\n"
                                    if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in \
                                            predicate_object_map.graph[predicate[1:-1]]:
                                        if "{" in predicate_object_map.graph[predicate[1:-1]]:
                                            triple = triple[:-2] + " <" + string_substitution(
                                                predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, "subject", ignore,
                                                triples_map.iterator) + ">.\n"
                                            dictionary_table_update(
                                                "<" + string_substitution(predicate_object_map.graph[predicate[1:-1]],
                                                                          "{(.+?)}", row, "subject", ignore,
                                                                          triples_map.iterator) + ">")
                                        else:
                                            triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
                                            dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
                                        if no_inner_cycle:
                                            if duplicate == "yes":
                                                if predicate in general_predicates:
                                                    if dic_table[
                                                        predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                                        if output_format.lower() == "n-triples":
                                                            output_file_descriptor.write(triple)
                                                        else:
                                                            end_turtle = turtle_print(subj, predicate, obj, object_list,
                                                                                      duplicate_type, predicate_object_map,
                                                                                      triples_map, output_file_descriptor,
                                                                                      generated)
                                                        g_triples.update({dic_table[
                                                                              predicate + "_" + predicate_object_map.object_map.value]: {
                                                            dic_table[subj] + "_" + dic_table[obj]: ""}})
                                                        i += 1
                                                        generated += 1
                                                    elif dic_table[subj] + "_" + dic_table[obj] not in g_triples[
                                                        dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                                        if output_format.lower() == "n-triples":
                                                            output_file_descriptor.write(triple)
                                                        else:
                                                            end_turtle = turtle_print(subj, predicate, obj, object_list,
                                                                                      duplicate_type, predicate_object_map,
                                                                                      triples_map, output_file_descriptor,
                                                                                      generated)
                                                        g_triples[dic_table[
                                                            predicate + "_" + predicate_object_map.object_map.value]].update(
                                                            {dic_table[subj] + "_" + dic_table[obj]: ""})
                                                        i += 1
                                                        generated += 1
                                                else:
                                                    if dic_table[predicate] not in g_triples:
                                                        if output_format.lower() == "n-triples":
                                                            output_file_descriptor.write(triple)
                                                        else:
                                                            end_turtle = turtle_print(subj, predicate, obj, object_list,
                                                                                      duplicate_type, predicate_object_map,
                                                                                      triples_map, output_file_descriptor,
                                                                                      generated)
                                                        g_triples.update(
                                                            {dic_table[predicate]: {dic_table[subj] + "_" + dic_table[obj]: ""}})
                                                        i += 1
                                                        generated += 1
                                                    elif dic_table[subj] + "_" + dic_table[obj] not in g_triples[
                                                        dic_table[predicate]]:
                                                        if output_format.lower() == "n-triples":
                                                            output_file_descriptor.write(triple)
                                                        else:
                                                            end_turtle = turtle_print(subj, predicate, obj, object_list,
                                                                                      duplicate_type, predicate_object_map,
                                                                                      triples_map, output_file_descriptor,
                                                                                      generated)
                                                        g_triples[dic_table[predicate]].update(
                                                            {dic_table[subj] + "_" + dic_table[obj]: ""})
                                                        i += 1
                                                        generated += 1
                                        else:
                                            if output_format.lower() == "n-triples":
                                                output_file_descriptor.write(triple)
                                            else:
                                                end_turtle = turtle_print(subj, predicate, obj, object_list, duplicate_type,
                                                                          predicate_object_map, triples_map,
                                                                          output_file_descriptor, generated)
                                            i += 1
                                            generated += 1
                    object_list = []
                    subject_list = []
                else:
                    continue
    return i


def semantify_mysql(row, row_headers, triples_map, triples_map_list, output_file_descriptor, host, port, user, password,
                    dbase, predicate):
    """
	(Private function, not accessible from outside this package)

	Takes a triples-map rule and applies it to each one of the rows of its CSV data
	source

	Parameters
	----------
	triples_map : TriplesMap object
		Mapping rule consisting of a logical source, a subject-map and several predicateObjectMaps
		(refer to the TriplesMap.py file in the triplesmap folder)
	triples_map_list : list of TriplesMap objects
		List of triples-maps parsed from current mapping being used for the semantification of a
		dataset (mainly used to perform rr:joinCondition mappings)
	delimiter : string
		Delimiter value for the CSV or TSV file ("\s" and "\t" respectively)
	output_file_descriptor : file object
		Descriptor to the output file (refer to the Python 3 documentation)

	Returns
	-------
	An .nt file per each dataset mentioned in the configuration file semantified.
	If the duplicates are asked to be removed in main memory, also returns a -min.nt
	file with the triples sorted and with the duplicates removed.
	"""
    global blank_message
    global generated_subjects
    triples_map_triples = {}
    generated_triples = {}
    object_list = []
    create_subject = True
    i = 0

    if mapping_partitions == "yes":
        if "_" in triples_map.triples_map_id:
            componets = triples_map.triples_map_id.split("_")[:-1]
            triples_map_id = ""
            for name in componets:
                triples_map_id += name + "_"
            triples_map_id = triples_map_id[:-1]
        else:
            triples_map_id = triples_map.triples_map_id

        if triples_map_id in generated_subjects:
            subject_attr = ""
            for attr in generated_subjects[triples_map_id]["subject_attr"]:
                subject_attr += str(row[row_headers.index(attr)]) + "_"
            subject_attr = subject_attr[:-1]

            if subject_attr in generated_subjects[triples_map_id]:
                subject = generated_subjects[triples_map_id][subject_attr]
                create_subject = False

    if create_subject:
        subject_value = string_substitution_array(triples_map.subject_map.value, "{(.+?)}", row, row_headers, "subject",
                                                  ignore)

        if triples_map.subject_map.subject_mapping_type == "template":
            if triples_map.subject_map.term_type is None:
                if triples_map.subject_map.condition == "":

                    try:
                        subject = "<" + subject_value + ">"
                    except:
                        subject = None

                else:
                    #	field, condition = condition_separetor(triples_map.subject_map.condition)
                    #	if row[field] == condition:
                    try:
                        subject = "<" + subject_value + ">"
                    except:
                        subject = None
            else:
                if "IRI" in triples_map.subject_map.term_type:
                    if triples_map.subject_map.condition == "":

                        try:
                            if "http" not in subject_value:
                                if base != "":
                                    subject = "<" + base + subject_value + ">"
                                else:
                                    subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                            else:
                                if is_valid_url_syntax(subject_value):
                                    subject = "<" + subject_value + ">"
                                else:
                                    if base != "":
                                        subject = "<" + base + subject_value + ">"
                                    else:
                                        subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                        except:
                            subject = None

                    else:
                        #	field, condition = condition_separetor(triples_map.subject_map.condition)
                        #	if row[field] == condition:
                        try:
                            if "http" not in subject_value:
                                if base != "":
                                    subject = "<" + base + subject_value + ">"
                                else:
                                    subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                            else:
                                if is_valid_url_syntax(subject_value):
                                    subject = "<" + subject_value + ">"
                                else:
                                    if base != "":
                                        subject = "<" + base + subject_value + ">"
                                    else:
                                        subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                        except:
                            subject = None

                elif "BlankNode" in triples_map.subject_map.term_type:
                    if triples_map.subject_map.condition == "":

                        try:
                            if "/" in subject_value:
                                subject = "_:" + encode_char(subject_value.replace("/", "2F")).replace("%", "")
                                if "." in subject:
                                    subject = subject.replace(".", "2E")
                                if blank_message:
                                    logger.warning(
                                        "Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                                    blank_message = False
                            else:
                                subject = "_:" + encode_char(subject_value).replace("%", "")
                                if "." in subject:
                                    subject = subject.replace(".", "2E")
                        except:
                            subject = None

                    else:
                        #	field, condition = condition_separetor(triples_map.subject_map.condition)
                        #	if row[field] == condition:
                        try:
                            subject = "_:" + subject_value
                        except:
                            subject = None
                elif "Literal" in triples_map.subject_map.term_type:
                    subject = None
                else:
                    if triples_map.subject_map.condition == "":

                        try:
                            subject = "<" + subject_value + ">"
                        except:
                            subject = None

                    else:
                        #	field, condition = condition_separetor(triples_map.subject_map.condition)
                        #	if row[field] == condition:
                        try:
                            subject = "<" + subject_value + ">"
                        except:
                            subject = None

        elif triples_map.subject_map.subject_mapping_type == "reference":
            if triples_map.subject_map.condition == "":
                try:
                    subject_value = string_substitution_array(triples_map.subject_map.value, ".+", row, row_headers,
                                                              "subject", ignore)
                    subject_value = subject_value[1:-1]
                    if " " not in subject_value:
                        if "http" not in subject_value:
                            if base != "":
                                subject = "<" + base + subject_value + ">"
                            else:
                                subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                        else:
                            if is_valid_url_syntax(subject_value):
                                subject = "<" + subject_value + ">"
                            else:
                                if base != "":
                                    subject = "<" + base + subject_value + ">"
                                else:
                                    subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                    else:
                        logger.error("<http://example.com/base/" + subject_value + "> is an invalid URL")
                        subject = None
                    if triples_map.subject_map.term_type == "IRI":
                        if " " not in subject_value:
                            subject = "<" + subject_value + ">"
                        else:
                            subject = None
                except:
                    subject = None

            else:
                #	field, condition = condition_separetor(triples_map.subject_map.condition)
                #	if row[field] == condition:
                try:
                    if "http" not in subject_value:
                        if base != "":
                            subject = "<" + base + subject_value + ">"
                        else:
                            subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                    else:
                        if is_valid_url_syntax(subject_value):
                            subject = "<" + subject_value + ">"
                        else:
                            if base != "":
                                subject = "<" + base + subject_value + ">"
                            else:
                                subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                except:
                    subject = None

        elif "constant" in triples_map.subject_map.subject_mapping_type:
            subject = "<" + triples_map.subject_map.value + ">"

        else:
            if triples_map.subject_map.condition == "":

                try:
                    subject = "\"" + triples_map.subject_map.value + "\""
                except:
                    subject = None

            else:
                #	field, condition = condition_separetor(triples_map.subject_map.condition)
                #	if row[field] == condition:
                try:
                    subject = "\"" + triples_map.subject_map.value + "\""
                except:
                    subject = None

        if mapping_partitions == "yes":
            if triples_map_id in generated_subjects:
                if subject_attr in generated_subjects[triples_map_id]:
                    pass
                else:
                    generated_subjects[triples_map_id][subject_attr] = subject
            else:
                generated_subjects[triples_map_id] = {subject_attr: subject}

    if triples_map.subject_map.rdf_class != [None] and subject != None:
        predicate = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
        for rdf_class in triples_map.subject_map.rdf_class:
            if rdf_class != None and ("str" == type(rdf_class).__name__ or "URIRef" == type(rdf_class).__name__):
                obj = "<{}>".format(rdf_class)
                dictionary_table_update(subject)
                dictionary_table_update(obj)
                dictionary_table_update(predicate + "_" + obj)
                rdf_type = subject + " " + predicate + " " + obj + ".\n"
                for graph in triples_map.subject_map.graph:
                    if graph != None and "defaultGraph" not in graph:
                        if "{" in graph:
                            rdf_type = rdf_type[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject",
                                                                                  ignore,
                                                                                  triples_map.iterator) + ">.\n"
                            dictionary_table_update("<" + string_substitution(graph, "{(.+?)}", row, "subject", ignore,
                                                                              triples_map.iterator) + ">")
                        else:
                            rdf_type = rdf_type[:-2] + " <" + graph + ">.\n"
                            dictionary_table_update("<" + graph + ">")
                    if duplicate == "yes":
                        if dic_table[predicate + "_" + obj] not in g_triples:
                            output_file_descriptor.write(rdf_type)
                            g_triples.update(
                                {dic_table[predicate + "_" + obj]: {dic_table[subject] + "_" + dic_table[obj]: ""}})
                            i += 1
                        elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                            dic_table[predicate + "_" + obj]]:
                            output_file_descriptor.write(rdf_type)
                            g_triples[dic_table[predicate + "_" + obj]].update(
                                {dic_table[subject] + "_" + dic_table[obj]: ""})
                            i += 1
                    else:
                        output_file_descriptor.write(rdf_type)
                        i += 1

    for predicate_object_map in triples_map.predicate_object_maps_list:
        if predicate_object_map.predicate_map.mapping_type == "constant" or predicate_object_map.predicate_map.mapping_type == "constant shortcut":
            predicate = "<" + predicate_object_map.predicate_map.value + ">"
        elif predicate_object_map.predicate_map.mapping_type == "template":
            if predicate_object_map.predicate_map.condition != "":
                try:
                    predicate = "<" + string_substitution_array(predicate_object_map.predicate_map.value, "{(.+?)}",
                                                                row, row_headers, "predicate", ignore) + ">"
                except:
                    predicate = None
            else:
                try:
                    predicate = "<" + string_substitution_array(predicate_object_map.predicate_map.value, "{(.+?)}",
                                                                row, row_headers, "predicate", ignore) + ">"
                except:
                    predicate = None
        elif predicate_object_map.predicate_map.mapping_type == "reference":
            predicate = string_substitution_array(predicate_object_map.predicate_map.value, ".+", row, row_headers,
                                                  "predicate", ignore)
            predicate = "<" + predicate[1:-1] + ">"
        else:
            predicate = None

        if predicate_object_map.object_map.mapping_type == "constant" or predicate_object_map.object_map.mapping_type == "constant shortcut":
            if "/" in predicate_object_map.object_map.value:
                object = "<" + predicate_object_map.object_map.value + ">"
            else:
                object = "\"" + predicate_object_map.object_map.value + "\""
            if predicate_object_map.object_map.datatype != None:
                object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
            elif predicate_object_map.object_map.datatype_map != None:
                datatype_value = string_substitution_array(predicate_object_map.object_map.datatype_map, "{(.+?)}", row,
                                                       row_headers, "object", ignore)
                if "http" in datatype_value:
                   object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(datatype_value)
                else:
                   object = "\"" + object[1:-1] + "\"" + "^^<{}>".format("http://example.com/base/" + datatype_value)
            elif predicate_object_map.object_map.language != None:
                if "spanish" == predicate_object_map.object_map.language or "es" == predicate_object_map.object_map.language:
                    object += "@es"
                elif "english" == predicate_object_map.object_map.language or "en" == predicate_object_map.object_map.language:
                    object += "@en"
                elif len(predicate_object_map.object_map.language) == 2:
                    object += "@" + predicate_object_map.object_map.language
                else:
                    object = None
            elif predicate_object_map.object_map.language_map != None:
                lang = string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object",
                                           ignore, triples_map.iterator)
                if lang != None:
                    object += "@" + string_substitution(predicate_object_map.object_map.language_map, ".+", row,
                                                        "object", ignore, triples_map.iterator)[1:-1] 
        elif predicate_object_map.object_map.mapping_type == "template":
            try:
                if predicate_object_map.object_map.term is None:
                    object = "<" + string_substitution_array(predicate_object_map.object_map.value, "{(.+?)}", row,
                                                             row_headers, "object", ignore) + ">"
                elif "IRI" in predicate_object_map.object_map.term:
                    object = "<" + string_substitution_array(predicate_object_map.object_map.value, "{(.+?)}", row,
                                                             row_headers, "object", ignore) + ">"
                elif "BlankNode" in predicate_object_map.object_map.term:
                    object = "_:" + string_substitution_array(predicate_object_map.object_map.value, "{(.+?)}", row,
                                                              row_headers, "object", ignore)
                    if "/" in object:
                        object = object.replace("/", "2F")
                        if blank_message:
                            logger.warning("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                            blank_message = False
                    if "." in object:
                        object = object.replace(".", "2E")
                    object = encode_char(object)
                else:
                    object = "\"" + string_substitution_array(predicate_object_map.object_map.value, "{(.+?)}", row,
                                                              row_headers, "object", ignore) + "\""
                    if predicate_object_map.object_map.datatype != None:
                        object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
                    elif predicate_object_map.object_map.datatype_map != None:
                        datatype_value = string_substitution_array(predicate_object_map.object_map.datatype_map, "{(.+?)}", row,
                                                               row_headers, "object", ignore)
                        if "http" in datatype_value:
                           object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(datatype_value)
                        else:
                           object = "\"" + object[1:-1] + "\"" + "^^<{}>".format("http://example.com/base/" + datatype_value) 
                    elif predicate_object_map.object_map.language != None:
                        if "spanish" == predicate_object_map.object_map.language or "es" == predicate_object_map.object_map.language:
                            object += "@es"
                        elif "english" == predicate_object_map.object_map.language or "en" == predicate_object_map.object_map.language:
                            object += "@en"
                        elif len(predicate_object_map.object_map.language) == 2:
                            object += "@" + predicate_object_map.object_map.language
                        else:
                            object = None
                    elif predicate_object_map.object_map.language_map != None:
                        lang = string_substitution_array(predicate_object_map.object_map.language_map, ".+", row,
                                                         row_headers, "object", ignore)
                        if lang != None:
                            object += "@" + string_substitution_array(predicate_object_map.object_map.language_map,
                                                                      ".+", row, row_headers, "object", ignore)[1:-1]
            except TypeError:
                object = None
        elif predicate_object_map.object_map.mapping_type == "reference":
            object = string_substitution_array(predicate_object_map.object_map.value, ".+", row, row_headers, "object",
                                               ignore)
            if object != None:
                if "\\" in object[1:-1]:
                    object = "\"" + object[1:-1].replace("\\", "\\\\") + "\""
                if "'" in object[1:-1]:
                    object = "\"" + object[1:-1].replace("'", "\\\\'") + "\""
                if "\"" in object[1:-1]:
                    object = "\"" + object[1:-1].replace("\"", "\\\"") + "\""
                if "\n" in object:
                    object = object.replace("\n", "\\n")
                if predicate_object_map.object_map.datatype != None:
                    object += "^^<{}>".format(predicate_object_map.object_map.datatype)
                elif predicate_object_map.object_map.datatype_map != None:
                    datatype_value = string_substitution_array(predicate_object_map.object_map.datatype_map, "{(.+?)}", row,
                                                           row_headers, "object", ignore)
                    if "http" in datatype_value:
                       object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(datatype_value)
                    else:
                       object = "\"" + object[1:-1] + "\"" + "^^<{}>".format("http://example.com/base/" + datatype_value) 
                elif predicate_object_map.object_map.language != None:
                    if "spanish" == predicate_object_map.object_map.language or "es" == predicate_object_map.object_map.language:
                        object += "@es"
                    elif "english" == predicate_object_map.object_map.language or "en" == predicate_object_map.object_map.language:
                        object += "@en"
                    elif len(predicate_object_map.object_map.language) == 2:
                        object += "@" + predicate_object_map.object_map.language
                    else:
                        object = None
                elif predicate_object_map.object_map.language_map != None:
                    lang = string_substitution_array(predicate_object_map.object_map.language_map, ".+", row,
                                                     row_headers, "object", ignore)
                    if lang != None:
                        object += "@" + string_substitution_array(predicate_object_map.object_map.language_map, ".+",
                                                                  row, row_headers, "object", ignore)[1:-1]
                elif predicate_object_map.object_map.term != None:
                    if "IRI" in predicate_object_map.object_map.term:
                        if " " not in object:
                            object = "\"" + object[1:-1].replace("\\\\'", "'") + "\""
                            object = "<" + encode_char(object[1:-1]) + ">"
                        else:
                            object = None

        elif predicate_object_map.object_map.mapping_type == "parent triples map":
            for triples_map_element in triples_map_list:
                if triples_map_element.triples_map_id == predicate_object_map.object_map.value:
                    if (triples_map_element.data_source != triples_map.data_source) or (
                            triples_map_element.tablename != triples_map.tablename) or (
                            triples_map_element.query != triples_map.query):
                        if len(predicate_object_map.object_map.child) == 1:
                            if triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[
                                0] not in join_table:
                                if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
                                    if "http" in triples_map_element.data_source:
                                        if triples_map_element.file_format == "JSONPath":
                                            response = urlopen(triples_map_element.data_source)
                                            data = json.loads(response.read())
                                            if isinstance(data, list):
                                                hash_maker_list(data, triples_map_element,
                                                                predicate_object_map.object_map)
                                            elif len(data) < 2:
                                                hash_maker_list(data[list(data.keys())[0]], triples_map_element,
                                                                predicate_object_map.object_map)
                                    else:
                                        with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
                                            if str(triples_map_element.file_format).lower() == "csv":
                                                data = csv.DictReader(input_file_descriptor, delimiter=",")
                                                hash_maker(data, triples_map_element, predicate_object_map.object_map,"", triples_map_list)
                                            else:
                                                data = json.load(input_file_descriptor)
                                                if isinstance(data, list):
                                                    hash_maker_list(data, triples_map_element,
                                                                    predicate_object_map.object_map)
                                                elif len(data) < 2:
                                                    hash_maker_list(data[list(data.keys())[0]], triples_map_element,
                                                                    predicate_object_map.object_map)
                                elif triples_map_element.file_format == "XPath":
                                    with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
                                        child_tree = ET.parse(input_file_descriptor)
                                        child_root = child_tree.getroot()
                                        parent_map = {c: p for p in child_tree.iter() for c in p}
                                        namespace = dict([node for _, node in
                                                          ET.iterparse(str(triples_map_element.data_source),
                                                                       events=['start-ns'])])
                                        hash_maker_xml(child_root, triples_map_element, predicate_object_map.object_map,
                                                       parent_map, namespace)
                                else:
                                    database, query_list = translate_sql(triples_map_element)
                                    db = connector.connect(host=host, port=int(port), user=user, password=password)
                                    cursor = db.cursor(buffered=True)
                                    if dbase.lower() != "none":
                                        cursor.execute("use " + dbase)
                                    else:
                                        if database != "None":
                                            cursor.execute("use " + database)
                                    if triples_map_element.query != "None":
                                        cursor.execute(triples_map_element.query)
                                    else:
                                        for query in query_list:
                                            temp_query = query.split("FROM")
                                            parent_list = ""
                                            for parent in predicate_object_map.object_map.parent:
                                                parent_list += ", `" + parent + "`"
                                            new_query = temp_query[0] + parent_list + " FROM " + temp_query[1]
                                            cursor.execute(new_query)
                                    hash_maker_array(cursor, triples_map_element, predicate_object_map.object_map)
                            jt = join_table[
                                triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]]
                            if row[row_headers.index(predicate_object_map.object_map.child[0])] != None and row[
                                row_headers.index(predicate_object_map.object_map.child[0])] in jt:
                                object_list = jt[row[row_headers.index(predicate_object_map.object_map.child[0])]]
                            object = None
                        else:
                            if (triples_map_element.triples_map_id + "_" + child_list(
                                    predicate_object_map.object_map.child)) not in join_table:
                                if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
                                    if "http" in triples_map_element.data_source:
                                        if triples_map_element.file_format == "JSONPath":
                                            response = urlopen(triples_map_element.data_source)
                                            data = json.loads(response.read())
                                            if isinstance(data, list):
                                                hash_maker_list(data, triples_map_element,
                                                                predicate_object_map.object_map)
                                            elif len(data) < 2:
                                                hash_maker_list(data[list(data.keys())[0]], triples_map_element,
                                                                predicate_object_map.object_map)
                                    else:
                                        with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
                                            if str(triples_map_element.file_format).lower() == "csv":
                                                data = csv.DictReader(input_file_descriptor, delimiter=",")
                                                hash_maker_list(data, triples_map_element,
                                                                predicate_object_map.object_map)
                                            else:
                                                data = json.load(input_file_descriptor)
                                                if isinstance(data, list):
                                                    hash_maker_list(data, triples_map_element,
                                                                    predicate_object_map.object_map)
                                                elif len(data) < 2:
                                                    hash_maker_list(data[list(data.keys())[0]], triples_map_element,
                                                                    predicate_object_map.object_map)

                                elif triples_map_element.file_format == "XPath":
                                    with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
                                        child_tree = ET.parse(input_file_descriptor)
                                        child_root = child_tree.getroot()
                                        parent_map = {c: p for p in child_tree.iter() for c in p}
                                        namespace = dict([node for _, node in
                                                          ET.iterparse(str(triples_map_element.data_source),
                                                                       events=['start-ns'])])
                                        hash_maker_xml(child_root, triples_map_element, predicate_object_map.object_map,
                                                       parent_map, namespace)
                                else:
                                    database, query_list = translate_sql(triples_map_element)
                                    db = connector.connect(host=host, port=int(port), user=user, password=password)
                                    cursor = db.cursor(buffered=True)
                                    if dbase.lower() != "none":
                                        cursor.execute("use " + dbase)
                                    else:
                                        if database != "None":
                                            cursor.execute("use " + database)
                                    if triples_map_element.query != "None":
                                        cursor.execute(triples_map_element.query)
                                    else:
                                        for query in query_list:
                                            temp_query = query.split("FROM")
                                            parent_list = ""
                                            for parent in predicate_object_map.object_map.parent:
                                                parent_list += ", `" + parent + "`"
                                            new_query = temp_query[0] + parent_list + " FROM " + temp_query[1]
                                            cursor.execute(new_query)
                                    hash_maker_array_list(cursor, triples_map_element, predicate_object_map.object_map,
                                                          row_headers)
                            if sublist(predicate_object_map.object_map.child, row_headers):
                                if child_list_value_array(predicate_object_map.object_map.child, row, row_headers) in \
                                        join_table[triples_map_element.triples_map_id + "_" + child_list(
                                                predicate_object_map.object_map.child)]:
                                    object_list = join_table[triples_map_element.triples_map_id + "_" + child_list(
                                        predicate_object_map.object_map.child)][
                                        child_list_value_array(predicate_object_map.object_map.child, row, row_headers)]
                                else:
                                    object_list = []
                            object = None
                    else:
                        if predicate_object_map.object_map.parent != None:
                            if len(predicate_object_map.object_map.parent) == 1:
                                if triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[
                                    0] not in join_table:
                                    database, query_list = translate_sql(triples_map_element)
                                    db = connector.connect(host=host, port=int(port), user=user, password=password)
                                    cursor = db.cursor(buffered=True)
                                    if dbase.lower() != "none":
                                        cursor.execute("use " + dbase)
                                    else:
                                        if database != "None":
                                            cursor.execute("use " + database)
                                    if triples_map_element.query != "None":
                                        cursor.execute(triples_map_element.query)
                                    else:
                                        for query in query_list:
                                            temp_query = query.split("FROM")
                                            parent_list = ""
                                            for parent in predicate_object_map.object_map.parent:
                                                parent_list += ", `" + parent + "`"
                                            new_query = temp_query[0] + parent_list + " FROM " + temp_query[1]
                                            cursor.execute(new_query)
                                    hash_maker_array(cursor, triples_map_element, predicate_object_map.object_map)

                            else:
                                if (triples_map_element.triples_map_id + "_" + child_list(
                                        predicate_object_map.object_map.child)) not in join_table:
                                    database, query_list = translate_sql(triples_map_element)
                                    db = connector.connect(host=host, port=int(port), user=user, password=password)
                                    cursor = db.cursor(buffered=True)
                                    if dbase.lower() != "none":
                                        cursor.execute("use " + dbase)
                                    else:
                                        if database != "None":
                                            cursor.execute("use " + database)
                                    if triples_map_element.query != "None":
                                        cursor.execute(triples_map_element.query)
                                    else:
                                        for query in query_list:
                                            temp_query = query.split("FROM")
                                            parent_list = ""
                                            for parent in predicate_object_map.object_map.parent:
                                                parent_list += ", `" + parent + "`"
                                            new_query = temp_query[0] + parent_list + " FROM " + temp_query[1]
                                            cursor.execute(new_query)
                                    hash_maker_array_list(cursor, triples_map_element, predicate_object_map.object_map,
                                                          row_headers)

                            if sublist(predicate_object_map.object_map.child, row_headers):
                                if child_list_value_array(predicate_object_map.object_map.child, row, row_headers) in \
                                        join_table[triples_map_element.triples_map_id + "_" + child_list(
                                                predicate_object_map.object_map.child)]:
                                    object_list = join_table[triples_map_element.triples_map_id + "_" + child_list(
                                        predicate_object_map.object_map.child)][
                                        child_list_value_array(predicate_object_map.object_map.child, row, row_headers)]
                                else:
                                    object_list = []
                            object = None
                        else:
                            try:
                                object = "<" + string_substitution_array(triples_map_element.subject_map.value,
                                                                         "{(.+?)}", row, row_headers, "object",
                                                                         ignore) + ">"
                            except TypeError:
                                object = None
                    break
                else:
                    continue
        else:
            object = None

        if is_current_output_valid(triples_map.triples_map_id,predicate_object_map,current_logical_dump,logical_dump):
            if predicate in general_predicates:
                dictionary_table_update(predicate + "_" + predicate_object_map.object_map.value)
            else:
                dictionary_table_update(predicate)
            if predicate != None and object != None and subject != None:
                dictionary_table_update(subject)
                dictionary_table_update(object)
                for graph in triples_map.subject_map.graph:
                    triple = subject + " " + predicate + " " + object + ".\n"
                    if graph != None and "defaultGraph" not in graph:
                        if "{" in graph:
                            triple = triple[:-2] + " <" + string_substitution_array(graph, "{(.+?)}", row, row_headers,
                                                                                    "subject", ignore) + ">.\n"
                            dictionary_table_update(
                                "<" + string_substitution_array(graph, "{(.+?)}", row, row_headers, "subject",
                                                                ignore) + ">")
                        else:
                            triple = triple[:-2] + " <" + graph + ">.\n"
                            dictionary_table_update("<" + graph + ">")
                    if predicate[1:-1] not in predicate_object_map.graph or graph != None or triples_map.subject_map.graph == [None]:
                        if duplicate == "yes":
                            if predicate in general_predicates:
                                if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                    try:
                                        output_file_descriptor.write(triple)
                                    except:
                                        output_file_descriptor.write(triple.encode("utf-8"))
                                    g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value]: {
                                        dic_table[subject] + "_" + dic_table[object]: ""}})
                                    i += 1
                                elif dic_table[subject] + "_" + dic_table[object] not in g_triples[
                                    dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                    try:
                                        output_file_descriptor.write(triple)
                                    except:
                                        output_file_descriptor.write(triple.encode("utf-8"))
                                    g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update(
                                        {dic_table[subject] + "_" + dic_table[object]: ""})
                                    i += 1
                            else:
                                if dic_table[predicate] not in g_triples:
                                    try:
                                        output_file_descriptor.write(triple)
                                    except:
                                        output_file_descriptor.write(triple.encode("utf-8"))
                                    g_triples.update({dic_table[predicate]: {dic_table[subject] + "_" + dic_table[object]: ""}})
                                    i += 1
                                elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
                                    try:
                                        output_file_descriptor.write(triple)
                                    except:
                                        output_file_descriptor.write(triple.encode("utf-8"))
                                    g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[object]: ""})
                                    i += 1
                        else:
                            try:
                                output_file_descriptor.write(triple)
                            except:
                                output_file_descriptor.write(triple.encode("utf-8"))
                        i += 1
                if predicate[1:-1] in predicate_object_map.graph:
                    triple = subject + " " + predicate + " " + object + ".\n"
                    if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in \
                            predicate_object_map.graph[predicate[1:-1]]:
                        if "{" in predicate_object_map.graph[predicate[1:-1]]:
                            triple = triple[:-2] + " <" + string_substitution_array(
                                predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, row_headers, "subject",
                                ignore) + ">.\n"
                            dictionary_table_update(
                                "<" + string_substitution_array(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row,
                                                                row_headers, "subject", ignore) + ">")
                        else:
                            triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
                            dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
                        if duplicate == "yes":
                            if predicate in general_predicates:
                                if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                    try:
                                        output_file_descriptor.write(triple)
                                    except:
                                        output_file_descriptor.write(triple.encode("utf-8"))
                                    g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value]: {
                                        dic_table[subject] + "_" + dic_table[object]: ""}})
                                    i += 1
                                elif dic_table[subject] + "_" + dic_table[object] not in g_triples[
                                    predicate + "_" + predicate_object_map.object_map.value]:
                                    try:
                                        output_file_descriptor.write(triple)
                                    except:
                                        output_file_descriptor.write(triple.encode("utf-8"))
                                    g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update(
                                        {dic_table[subject] + "_" + dic_table[object]: ""})
                                    i += 1
                            else:
                                if dic_table[predicate] not in g_triples:
                                    try:
                                        output_file_descriptor.write(triple)
                                    except:
                                        output_file_descriptor.write(triple.encode("utf-8"))
                                    g_triples.update(
                                        {dic_table[predicate]: {dic_table[subject] + "_" + dic_table[object]: ""}})
                                    i += 1
                                elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
                                    try:
                                        output_file_descriptor.write(triple)
                                    except:
                                        output_file_descriptor.write(triple.encode("utf-8"))
                                    g_triples[dic_table[predicate]].update(
                                        {dic_table[subject] + "_" + dic_table[object]: ""})
                                    i += 1
                        else:
                            try:
                                output_file_descriptor.write(triple)
                            except:
                                output_file_descriptor.write(triple.encode("utf-8"))
                            i += 1
            elif predicate != None and subject != None and object_list:
                dictionary_table_update(subject)
                for obj in object_list:
                    dictionary_table_update(obj)
                    triple = subject + " " + predicate + " " + obj + ".\n"
                    for graph in triples_map.subject_map.graph:
                        if graph != None and "defaultGraph" not in graph:
                            if "{" in graph:
                                triple = triple[:-2] + " <" + string_substitution_array(graph, "{(.+?)}", row, row_headers,
                                                                                        "subject", ignore) + ">.\n"
                                dictionary_table_update(
                                    "<" + string_substitution_array(graph, "{(.+?)}", row, row_headers, "subject",
                                                                    ignore) + ">")
                            else:
                                triple = triple[:-2] + " <" + graph + ">.\n"
                                dictionary_table_update("<" + graph + ">")
                        if duplicate == "yes":
                            if predicate in general_predicates:
                                if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                    try:
                                        output_file_descriptor.write(triple)
                                    except:
                                        output_file_descriptor.write(triple.encode("utf-8"))
                                    g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value]: {
                                        dic_table[subject] + "_" + dic_table[obj]: ""}})
                                    i += 1
                                elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                    dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                    try:
                                        output_file_descriptor.write(triple)
                                    except:
                                        output_file_descriptor.write(triple.encode("utf-8"))
                                    g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update(
                                        {dic_table[subject] + "_" + dic_table[obj]: ""})
                                    i += 1
                            else:
                                if dic_table[predicate] not in g_triples:
                                    try:
                                        output_file_descriptor.write(triple)
                                    except:
                                        output_file_descriptor.write(triple.encode("utf-8"))
                                    g_triples.update(
                                        {dic_table[predicate]: {dic_table[subject] + "_" + dic_table[obj]: ""}})
                                    i += 1
                                elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
                                    try:
                                        output_file_descriptor.write(triple)
                                    except:
                                        output_file_descriptor.write(triple.encode("utf-8"))
                                    g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
                                    i += 1
                        else:
                            try:
                                output_file_descriptor.write(triple)
                            except:
                                output_file_descriptor.write(triple.encode("utf-8"))
                            i += 1
                    if predicate[1:-1] in predicate_object_map.graph:
                        triple = subject + " " + predicate + " " + obj + ".\n"
                        if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in \
                                predicate_object_map.graph[predicate[1:-1]]:
                            if "{" in predicate_object_map.graph[predicate[1:-1]]:
                                triple = triple[:-2] + " <" + string_substitution_array(
                                    predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, row_headers, "subject",
                                    ignore) + ">.\n"
                                dictionary_table_update(
                                    "<" + string_substitution_array(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}",
                                                                    row, row_headers, "subject", ignore) + ">")
                            else:
                                triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
                                dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
                            if duplicate == "yes":
                                if predicate in general_predicates:
                                    if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                        try:
                                            output_file_descriptor.write(triple)
                                        except:
                                            output_file_descriptor.write(triple.encode("utf-8"))
                                        g_triples.update({dic_table[
                                                              predicate + "_" + predicate_object_map.object_map.value]: {
                                            dic_table[subject] + "_" + dic_table[obj]: ""}})
                                        i += 1
                                    elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                        dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                        try:
                                            output_file_descriptor.write(triple)
                                        except:
                                            output_file_descriptor.write(triple.encode("utf-8"))
                                        g_triples[
                                            dic_table[predicate + "_" + predicate_object_map.object_map.value]].update(
                                            {dic_table[subject] + "_" + dic_table[obj]: ""})
                                        i += 1
                                else:
                                    if dic_table[predicate] not in g_triples:
                                        try:
                                            output_file_descriptor.write(triple)
                                        except:
                                            output_file_descriptor.write(triple.encode("utf-8"))
                                        g_triples.update(
                                            {dic_table[predicate]: {dic_table[subject] + "_" + dic_table[obj]: ""}})
                                        i += 1
                                    elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
                                        try:
                                            output_file_descriptor.write(triple)
                                        except:
                                            output_file_descriptor.write(triple.encode("utf-8"))
                                        g_triples[dic_table[predicate]].update(
                                            {dic_table[subject] + "_" + dic_table[obj]: ""})
                                        i += 1
                            else:
                                try:
                                    output_file_descriptor.write(triple)
                                except:
                                    output_file_descriptor.write(triple.encode("utf-8"))
                                i += 1
                object_list = []
            else:
                continue
            predicate = None
    return i


def semantify_postgres(row, row_headers, triples_map, triples_map_list, output_file_descriptor, user, password, db,
                       host, predicate):
    """
	(Private function, not accessible from outside this package)

	Takes a triples-map rule and applies it to each one of the rows of its CSV data
	source

	Parameters
	----------
	triples_map : TriplesMap object
		Mapping rule consisting of a logical source, a subject-map and several predicateObjectMaps
		(refer to the TriplesMap.py file in the triplesmap folder)
	triples_map_list : list of TriplesMap objects
		List of triples-maps parsed from current mapping being used for the semantification of a
		dataset (mainly used to perform rr:joinCondition mappings)
	delimiter : string
		Delimiter value for the CSV or TSV file ("\s" and "\t" respectively)
	output_file_descriptor : file object
		Descriptor to the output file (refer to the Python 3 documentation)

	Returns
	-------
	An .nt file per each dataset mentioned in the configuration file semantified.
	If the duplicates are asked to be removed in main memory, also returns a -min.nt
	file with the triples sorted and with the duplicates removed.
	"""
    triples_map_triples = {}
    generated_triples = {}
    object_list = []
    global blank_message
    global generated_subjects
    i = 0
    create_subject = True
    if mapping_partitions == "yes":
        if "_" in triples_map.triples_map_id:
            componets = triples_map.triples_map_id.split("_")[:-1]
            triples_map_id = ""
            for name in componets:
                triples_map_id += name + "_"
            triples_map_id = triples_map_id[:-1]
        else:
            triples_map_id = triples_map.triples_map_id

        if triples_map_id in generated_subjects:
            subject_attr = ""
            for attr in generated_subjects[triples_map_id]["subject_attr"]:
                subject_attr += str(row[row_headers.index(attr)]) + "_"
            subject_attr = subject_attr[:-1]

            if subject_attr in generated_subjects[triples_map_id]:
                subject = generated_subjects[triples_map_id][subject_attr]
                create_subject = False

    if create_subject:
        subject_value = string_substitution_array(triples_map.subject_map.value, "{(.+?)}", row, row_headers, "subject",
                                                  ignore)
        if triples_map.subject_map.subject_mapping_type == "template":
            if triples_map.subject_map.term_type is None:
                if triples_map.subject_map.condition == "":

                    try:
                        subject = "<" + subject_value + ">"

                    except:
                        subject = None

                else:
                    #	field, condition = condition_separetor(triples_map.subject_map.condition)
                    #	if row[field] == condition:
                    try:
                        subject = "<" + subject_value + ">"

                    except:
                        subject = None
            else:
                if "IRI" in triples_map.subject_map.term_type:
                    if triples_map.subject_map.condition == "":

                        try:
                            if "http" not in subject_value:
                                if base != "":
                                    subject = "<" + base + subject_value + ">"
                                else:
                                    subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                            else:
                                if is_valid_url_syntax(subject_value):
                                    subject = "<" + subject_value + ">"
                                else:
                                    if base != "":
                                        subject = "<" + base + subject_value + ">"
                                    else:
                                        subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                        except:
                            subject = None

                    else:
                        #	field, condition = condition_separetor(triples_map.subject_map.condition)
                        #	if row[field] == condition:
                        try:
                            if "http" not in subject_value:
                                if base != "":
                                    subject = "<" + base + subject_value + ">"
                                else:
                                    subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                            else:
                                if is_valid_url_syntax(subject_value):
                                    subject = "<" + subject_value + ">"
                                else:
                                    if base != "":
                                        subject = "<" + base + subject_value + ">"
                                    else:
                                        subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"

                        except:
                            subject = None

                elif "BlankNode" in triples_map.subject_map.term_type:
                    if triples_map.subject_map.condition == "":

                        try:
                            if "/" in subject_value:
                                subject = "_:" + encode_char(subject_value.replace("/", "2F")).replace("%", "")
                                if "." in subject:
                                    subject = subject.replace(".", "2E")
                                if blank_message:
                                    logger.warning(
                                        "Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                                    blank_message = False
                            else:
                                subject = "_:" + encode_char(subject_value).replace("%", "")
                                if "." in subject:
                                    subject = subject.replace(".", "2E")
                        except:
                            subject = None

                    else:
                        #	field, condition = condition_separetor(triples_map.subject_map.condition)
                        #	if row[field] == condition:
                        try:
                            subject = "_:" + subject_value
                        except:
                            subject = None

                elif "Literal" in triples_map.subject_map.term_type:
                    subject = None

                else:
                    if triples_map.subject_map.condition == "":

                        try:
                            subject = "<" + subject_value + ">"
                        except:
                            subject = None

                    else:
                        #	field, condition = condition_separetor(triples_map.subject_map.condition)
                        #	if row[field] == condition:
                        try:
                            subject = "<" + subject_value + ">"
                        except:
                            subject = None

        elif triples_map.subject_map.subject_mapping_type == "reference":
            if triples_map.subject_map.condition == "":
                subject_value = string_substitution_array(triples_map.subject_map.value, ".+", row, row_headers,
                                                          "subject", ignore)
                subject_value = subject_value[1:-1]
                try:
                    if " " not in subject_value:
                        if "http" not in subject_value:
                            if base != "":
                                subject = "<" + base + subject_value + ">"
                            else:
                                subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                        else:
                            if is_valid_url_syntax(subject_value):
                                subject = "<" + subject_value + ">"
                            else:
                                if base != "":
                                    subject = "<" + base + subject_value + ">"
                                else:
                                    subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                    else:
                        logger.error("<http://example.com/base/" + subject_value + "> is an invalid URL")
                        subject = None
                except:
                    subject = None
                if triples_map.subject_map.term_type == "IRI":
                    if " " not in subject_value:
                        if "http" in subject_value:
                            temp = encode_char(subject_value.replace("http:", ""))
                            subject = "<" + "http:" + temp + ">"
                        else:
                            subject = "<" + encode_char(subject_value) + ">"
                    else:
                        subject = None

            else:
                #	field, condition = condition_separetor(triples_map.subject_map.condition)
                #	if row[field] == condition:
                try:
                    if "http" not in subject_value:
                        if base != "":
                            subject = "<" + base + subject_value + ">"
                        else:
                            subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                    else:
                        if is_valid_url_syntax(subject_value):
                            subject = "<" + subject_value + ">"
                        else:
                            if base != "":
                                subject = "<" + base + subject_value + ">"
                            else:
                                subject = "<" + "http://example.com/base/" + encode_char(subject_value) + ">"
                except:
                    subject = None

        elif "constant" in triples_map.subject_map.subject_mapping_type:
            subject = "<" + triples_map.subject_map.value + ">"

        else:
            if triples_map.subject_map.condition == "":

                try:
                    subject = "\"" + triples_map.subject_map.value + "\""
                except:
                    subject = None

            else:
                #	field, condition = condition_separetor(triples_map.subject_map.condition)
                #	if row[field] == condition:
                try:
                    subject = "\"" + triples_map.subject_map.value + "\""
                except:
                    subject = None

        if mapping_partitions == "yes":
            if triples_map_id in generated_subjects:
                if subject_attr in generated_subjects[triples_map_id]:
                    pass
                else:
                    generated_subjects[triples_map_id][subject_attr] = subject
            else:
                generated_subjects[triples_map_id] = {subject_attr: subject}

    if triples_map.subject_map.rdf_class != [None] and subject != None:
        predicate = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
        for rdf_class in triples_map.subject_map.rdf_class:
            if rdf_class != None and ("str" == type(rdf_class).__name__ or "URIRef" == type(rdf_class).__name__):
                obj = "<{}>".format(rdf_class)
                dictionary_table_update(subject)
                dictionary_table_update(obj)
                dictionary_table_update(predicate + "_" + obj)
                rdf_type = subject + " " + predicate + " " + obj + ".\n"
                for graph in triples_map.subject_map.graph:
                    if graph != None and "defaultGraph" not in graph:
                        if "{" in graph:
                            rdf_type = rdf_type[:-2] + " <" + string_substitution_array(graph, "{(.+?)}", row,
                                                                                        row_headers, "subject",
                                                                                        ignore) + ">.\n"
                            dictionary_table_update(
                                "<" + string_substitution_array(graph, "{(.+?)}", row, row_headers, "subject",
                                                                ignore) + ">")
                        else:
                            rdf_type = rdf_type[:-2] + " <" + graph + ">.\n"
                            dictionary_table_update("<" + graph + ">")
                    if duplicate == "yes":
                        if dic_table[predicate + "_" + obj] not in g_triples:
                            try:
                                output_file_descriptor.write(rdf_type)
                            except:
                                output_file_descriptor.write(rdf_type.encode("utf-8"))
                            g_triples.update(
                                {dic_table[predicate + "_" + obj]: {dic_table[subject] + "_" + dic_table[obj]: ""}})
                            i += 1
                        elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                            dic_table[predicate + "_" + obj]]:
                            try:
                                output_file_descriptor.write(rdf_type)
                            except:
                                output_file_descriptor.write(rdf_type.encode("utf-8"))
                            g_triples[dic_table[predicate + "_" + obj]].update(
                                {dic_table[subject] + "_" + dic_table[obj]: ""})
                            i += 1
                    else:
                        try:
                            output_file_descriptor.write(rdf_type)
                        except:
                            output_file_descriptor.write(rdf_type.encode("utf-8"))
                        i += 1
        predicate = None

    for predicate_object_map in triples_map.predicate_object_maps_list:
        if predicate == None:
            if predicate_object_map.predicate_map.mapping_type == "constant" or predicate_object_map.predicate_map.mapping_type == "constant shortcut":
                predicate = "<" + predicate_object_map.predicate_map.value + ">"
            elif predicate_object_map.predicate_map.mapping_type == "template":
                if predicate_object_map.predicate_map.condition != "":
                    try:
                        predicate = "<" + string_substitution_postgres(predicate_object_map.predicate_map.value,
                                                                       "{(.+?)}", row, row_headers, "predicate",
                                                                       ignore) + ">"
                    except:
                        predicate = None
                else:
                    try:
                        predicate = "<" + string_substitution_postgres(predicate_object_map.predicate_map.value,
                                                                       "{(.+?)}", row, row_headers, "predicate",
                                                                       ignore) + ">"
                    except:
                        predicate = None
            elif predicate_object_map.predicate_map.mapping_type == "reference":
                predicate = string_substitution_postgres(predicate_object_map.predicate_map.value, ".+", row,
                                                         row_headers, "predicate", ignore)
                predicate = "<" + predicate[1:-1] + ">"
            else:
                predicate = None

        if predicate_object_map.object_map.mapping_type == "constant" or predicate_object_map.object_map.mapping_type == "constant shortcut":
            if "/" in predicate_object_map.object_map.value:
                object = "<" + predicate_object_map.object_map.value + ">"
            else:
                object = "\"" + predicate_object_map.object_map.value + "\""
            if predicate_object_map.object_map.datatype != None:
                object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
            elif predicate_object_map.object_map.datatype_map != None:
                datatype_value = string_substitution_postgres(predicate_object_map.object_map.datatype_map, "{(.+?)}", row,
                                                       row_headers, "object", ignore)
                if "http" in datatype_value:
                   object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(datatype_value)
                else:
                   object = "\"" + object[1:-1] + "\"" + "^^<{}>".format("http://example.com/base/" + datatype_value)
            elif predicate_object_map.object_map.language != None:
                if "spanish" == predicate_object_map.object_map.language or "es" == predicate_object_map.object_map.language:
                    object += "@es"
                elif "english" == predicate_object_map.object_map.language or "en" == predicate_object_map.object_map.language:
                    object += "@en"
                elif len(predicate_object_map.object_map.language) == 2:
                    object += "@" + predicate_object_map.object_map.language
                else:
                    object = None
            elif predicate_object_map.object_map.language_map != None:
                lang = string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object",
                                           ignore, triples_map.iterator)
                if lang != None:
                    object += "@" + string_substitution(predicate_object_map.object_map.language_map, ".+", row,
                                                        "object", ignore, triples_map.iterator)[1:-1]
        elif predicate_object_map.object_map.mapping_type == "template":
            try:
                if predicate_object_map.object_map.term is None:
                    object = "<" + string_substitution_postgres(predicate_object_map.object_map.value, "{(.+?)}", row,
                                                                row_headers, "object", ignore) + ">"
                elif "IRI" in predicate_object_map.object_map.term:
                    object = "<" + string_substitution_postgres(predicate_object_map.object_map.value, "{(.+?)}", row,
                                                                row_headers, "object", ignore) + ">"
                elif "BlankNode" in predicate_object_map.object_map.term:
                    object = "_:" + string_substitution_postgres(predicate_object_map.object_map.value, "{(.+?)}", row,
                                                                 row_headers, "object", ignore)
                    if "/" in object:
                        object = encode_char(object.replace("/", "2F")).replace("%", "")
                        if blank_message:
                            logger.warning("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
                            blank_message = False
                    if "." in object:
                        object = object.replace(".", "2E")
                    object = encode_char(object)
                else:
                    object = "\"" + string_substitution_postgres(predicate_object_map.object_map.value, "{(.+?)}", row,
                                                                 row_headers, "object", ignore) + "\""
                    if predicate_object_map.object_map.datatype != None:
                        object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
                    elif predicate_object_map.object_map.datatype_map != None:
                        datatype_value = string_substitution_postgres(predicate_object_map.object_map.datatype_map, "{(.+?)}", row,
                                                               row_headers, "object", ignore)
                        if "http" in datatype_value:
                           object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(datatype_value)
                        else:
                           object = "\"" + object[1:-1] + "\"" + "^^<{}>".format("http://example.com/base/" + datatype_value)
                    elif predicate_object_map.object_map.language != None:
                        if "spanish" == predicate_object_map.object_map.language or "es" == predicate_object_map.object_map.language:
                            object += "@es"
                        elif "english" == predicate_object_map.object_map.language or "en" == predicate_object_map.object_map.language:
                            object += "@en"
                        elif len(predicate_object_map.object_map.language) == 2:
                            object += "@" + predicate_object_map.object_map.language
                        else:
                            object = None
                    elif predicate_object_map.object_map.language_map != None:
                        lang = string_substitution_postgres(predicate_object_map.object_map.language_map, ".+", row,
                                                            row_headers, "object", ignore)
                        if lang != None:
                            object += "@" + string_substitution_postgres(predicate_object_map.object_map.language_map,
                                                                         ".+", row, row_headers, "object", ignore)[1:-1]
            except TypeError:
                object = None
        elif predicate_object_map.object_map.mapping_type == "reference":
            object = string_substitution_postgres(predicate_object_map.object_map.value, ".+", row, row_headers,
                                                  "object", ignore)
            if object != None:
                if "\\" in object[1:-1]:
                    object = "\"" + object[1:-1].replace("\\", "\\\\") + "\""
                if "'" in object[1:-1]:
                    object = "\"" + object[1:-1].replace("'", "\\\\'") + "\""
                if "\"" in object[1:-1]:
                    object = "\"" + object[1:-1].replace("\"", "\\\"") + "\""
                if "\n" in object:
                    object = object.replace("\n", "\\n")
                if predicate_object_map.object_map.datatype != None:
                    object += "^^<{}>".format(predicate_object_map.object_map.datatype)
                elif predicate_object_map.object_map.datatype_map != None:
                    datatype_value = string_substitution_postgres(predicate_object_map.object_map.datatype_map, "{(.+?)}", row,
                                                           row_headers, "object", ignore)
                    if "http" in datatype_value:
                       object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(datatype_value)
                    else:
                       object = "\"" + object[1:-1] + "\"" + "^^<{}>".format("http://example.com/base/" + datatype_value)
                elif predicate_object_map.object_map.language != None:
                    if "spanish" == predicate_object_map.object_map.language or "es" == predicate_object_map.object_map.language:
                        object += "@es"
                    elif "english" == predicate_object_map.object_map.language or "en" == predicate_object_map.object_map.language:
                        object += "@en"
                    elif len(predicate_object_map.object_map.language) == 2:
                        object += "@" + predicate_object_map.object_map.language
                    else:
                        object = None
                elif predicate_object_map.object_map.language_map != None:
                    lang = string_substitution_postgres(predicate_object_map.object_map.language_map, ".+", row,
                                                        row_headers, "object", ignore)
                    if lang != None:
                        object += "@" + string_substitution_postgres(predicate_object_map.object_map.language_map, ".+",
                                                                     row, row_headers, "object", ignore)[1:-1]
                elif predicate_object_map.object_map.term != None:
                    if "IRI" in predicate_object_map.object_map.term:
                        if " " not in object:
                            object = "\"" + object[1:-1].replace("\\\\'", "'") + "\""
                            object = "<" + encode_char(object[1:-1]) + ">"
                        else:
                            object = None
        elif predicate_object_map.object_map.mapping_type == "parent triples map":
            for triples_map_element in triples_map_list:
                if triples_map_element.triples_map_id == predicate_object_map.object_map.value:
                    if (triples_map_element.data_source != triples_map.data_source) or (
                            triples_map_element.tablename != triples_map.tablename):
                        if triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[
                            0] not in join_table:
                            if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
                                if "http" in triples_map_element.data_source:
                                    if triples_map_element.file_format == "JSONPath":
                                        response = urlopen(triples_map_element.data_source)
                                        data = json.loads(response.read())
                                        if isinstance(data, list):
                                            hash_maker_list(data, triples_map_element, predicate_object_map.object_map)
                                        elif len(data) < 2:
                                            hash_maker_list(data[list(data.keys())[0]], triples_map_element,
                                                            predicate_object_map.object_map)
                                else:
                                    with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
                                        if str(triples_map_element.file_format).lower() == "csv":
                                            data = csv.DictReader(input_file_descriptor, delimiter=",")
                                            hash_maker(data, triples_map_element, predicate_object_map.object_map,"", triples_map_list)
                                        else:
                                            data = json.load(input_file_descriptor)
                                            hash_maker(data[list(data.keys())[0]], triples_map_element,
                                                       predicate_object_map.object_map,"", triples_map_list)

                            elif triples_map_element.file_format == "XPath":
                                with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
                                    child_tree = ET.parse(input_file_descriptor)
                                    child_root = child_tree.getroot()
                                    parent_map = {c: p for p in child_tree.iter() for c in p}
                                    namespace = dict([node for _, node in
                                                      ET.iterparse(str(triples_map_element.data_source),
                                                                   events=['start-ns'])])
                                    hash_maker_xml(child_root, triples_map_element, predicate_object_map.object_map,
                                                   parent_map, namespace)
                            else:
                                db_element = psycopg2.connect(host=host, user=user, password=password, dbname=db)
                                cursor = db_element.cursor()
                                if triples_map_element.query != None and triples_map_element.query != "None":
                                    cursor.execute(triples_map_element.query)
                                else:
                                    database, query_list = translate_postgressql(triples_map_element)
                                    for query in query_list:
                                        temp_query = query.split("FROM")
                                        parent_list = ""
                                        for parent in predicate_object_map.object_map.parent:
                                            if parent not in temp_query[0]:
                                                parent_list += ", \"" + parent + "\""
                                        new_query = temp_query[0] + parent_list + " FROM " + temp_query[1]
                                        cursor.execute(new_query)
                                data = cursor
                                hash_maker_array(cursor, triples_map_element, predicate_object_map.object_map)
                        jt = join_table[
                            triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]]
                        if row[row_headers.index(predicate_object_map.object_map.child[0])] != None and str(
                                row[row_headers.index(predicate_object_map.object_map.child[0])]) in jt:
                            object_list = jt[str(row[row_headers.index(predicate_object_map.object_map.child[0])])]
                        object = None
                    else:
                        if predicate_object_map.object_map.parent != None:
                            if triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[
                                0] not in join_table:
                                db_element = psycopg2.connect(host=host, user=user, password=password, dbname=db)
                                cursor = db_element.cursor()
                                if triples_map_element.tablename != "None":
                                    database, query_list = translate_postgressql(triples_map_element)
                                    for query in query_list:
                                        temp_query = query.split("FROM")
                                        parent_list = ""
                                        for parent in predicate_object_map.object_map.parent:
                                            if parent not in temp_query[0]:
                                                parent_list += ", \"" + parent + "\""
                                        new_query = temp_query[0] + parent_list + " FROM " + temp_query[1]
                                        cursor.execute(new_query)
                                else:
                                    cursor.execute(triples_map_element.query)
                                hash_maker_array(cursor, triples_map_element, predicate_object_map.object_map)
                            jt = join_table[
                                triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]]
                            if row[row_headers.index(predicate_object_map.object_map.child[0])] != None and str(
                                    row[row_headers.index(predicate_object_map.object_map.child[0])]) in jt:
                                object_list = str(jt[row[row_headers.index(predicate_object_map.object_map.child[0])]])
                            object = None
                        else:
                            try:
                                database, query_list = translate_postgressql(triples_map)
                                database2, query_list_origin = translate_postgressql(triples_map_element)
                                db_element = psycopg2.connect(host=host, user=user, password=password, dbname=db)
                                cursor = db_element.cursor()
                                for query in query_list:
                                    for q in query_list_origin:
                                        query_1 = q.split("FROM")
                                        query_2 = query.split("SELECT")[1].split("FROM")[0]
                                        query_new = query_1[0] + ", " + query_2 + " FROM " + query_1[1]
                                        cursor.execute(query_new)
                                        r_h = [x[0] for x in cursor.description]
                                        for r in cursor:
                                            s = string_substitution_postgres(triples_map.subject_map.value, "{(.+?)}",
                                                                             r, r_h, "subject", ignore)
                                            if subject_value == s:
                                                object = "<" + string_substitution_postgres(
                                                    triples_map_element.subject_map.value, "{(.+?)}", r, r_h, "object",
                                                    ignore) + ">"
                            except TypeError:
                                object = None
                    break
                else:
                    continue
        else:
            object = None

        if is_current_output_valid(triples_map.triples_map_id,predicate_object_map,current_logical_dump,logical_dump):
            if predicate in general_predicates:
                dictionary_table_update(predicate + "_" + predicate_object_map.object_map.value)
            else:
                dictionary_table_update(predicate)
            if predicate != None and object != None and subject != None:
                dictionary_table_update(subject)
                dictionary_table_update(object)
                triple = subject + " " + predicate + " " + object + ".\n"
                for graph in triples_map.subject_map.graph:
                    if graph != None and "defaultGraph" not in graph:
                        if "{" in graph:
                            triple = triple[:-2] + " <" + string_substitution_array(graph, "{(.+?)}", row, row_headers,
                                                                                    "subject", ignore) + ">.\n"
                            dictionary_table_update(
                                "<" + string_substitution_array(graph, "{(.+?)}", row, row_headers, "subject",
                                                                ignore) + ">")
                        else:
                            triple = triple[:-2] + " <" + graph + ">.\n"
                            dictionary_table_update("<" + graph + ">")
                    if duplicate == "yes":
                        if predicate in general_predicates:
                            if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                try:
                                    output_file_descriptor.write(triple)
                                except:
                                    output_file_descriptor.write(triple.encode("utf-8"))
                                g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value]: {
                                    dic_table[subject] + "_" + dic_table[object]: ""}})
                                i += 1
                            elif dic_table[subject] + "_" + dic_table[object] not in g_triples[
                                dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                try:
                                    output_file_descriptor.write(triple)
                                except:
                                    output_file_descriptor.write(triple.encode("utf-8"))
                                g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update(
                                    {dic_table[subject] + "_" + dic_table[object]: ""})
                                i += 1
                        else:
                            if dic_table[predicate] not in g_triples:
                                try:
                                    output_file_descriptor.write(triple)
                                except:
                                    output_file_descriptor.write(triple.encode("utf-8"))
                                g_triples.update({dic_table[predicate]: {dic_table[subject] + "_" + dic_table[object]: ""}})
                                i += 1
                            elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
                                try:
                                    output_file_descriptor.write(triple)
                                except:
                                    output_file_descriptor.write(triple.encode("utf-8"))
                                g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[object]: ""})
                                i += 1
                    else:
                        try:
                            output_file_descriptor.write(triple)
                        except:
                            output_file_descriptor.write(triple.encode("utf-8"))
                        i += 1
                if predicate[1:-1] in predicate_object_map.graph:
                    triple = subject + " " + predicate + " " + object + ".\n"
                    if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in \
                            predicate_object_map.graph[predicate[1:-1]]:
                        if "{" in predicate_object_map.graph[predicate[1:-1]]:
                            triple = triple[:-2] + " <" + string_substitution_array(
                                predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, row_headers, "subject",
                                ignore) + ">.\n"
                            dictionary_table_update(
                                "<" + string_substitution_array(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row,
                                                                row_headers, "subject", ignore) + ">")
                        else:
                            triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
                            dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
                        if duplicate == "yes":
                            if predicate in general_predicates:
                                if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                    try:
                                        output_file_descriptor.write(triple)
                                    except:
                                        output_file_descriptor.write(triple.encode("utf-8"))
                                    g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value]: {
                                        dic_table[subject] + "_" + dic_table[object]: ""}})
                                    i += 1
                                elif dic_table[subject] + "_" + dic_table[object] not in g_triples[
                                    predicate + "_" + predicate_object_map.object_map.value]:
                                    try:
                                        output_file_descriptor.write(triple)
                                    except:
                                        output_file_descriptor.write(triple.encode("utf-8"))
                                    g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update(
                                        {dic_table[subject] + "_" + dic_table[object]: ""})
                                    i += 1
                            else:
                                if dic_table[predicate] not in g_triples:
                                    try:
                                        output_file_descriptor.write(triple)
                                    except:
                                        output_file_descriptor.write(triple.encode("utf-8"))
                                    g_triples.update(
                                        {dic_table[predicate]: {dic_table[subject] + "_" + dic_table[object]: ""}})
                                    i += 1
                                elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
                                    try:
                                        output_file_descriptor.write(triple)
                                    except:
                                        output_file_descriptor.write(triple.encode("utf-8"))
                                    g_triples[dic_table[predicate]].update(
                                        {dic_table[subject] + "_" + dic_table[object]: ""})
                                    i += 1
                        else:
                            try:
                                output_file_descriptor.write(triple)
                            except:
                                output_file_descriptor.write(triple.encode("utf-8"))
            elif predicate != None and subject != None and object_list:
                dictionary_table_update(subject)
                for obj in object_list:
                    dictionary_table_update(obj)
                    for graph in triples_map.subject_map.graph:
                        triple = subject + " " + predicate + " " + obj + ".\n"
                        if graph != None and "defaultGraph" not in graph:
                            if "{" in graph:
                                triple = triple[:-2] + " <" + string_substitution_array(graph, "{(.+?)}", row, row_headers,
                                                                                        "subject", ignore) + ">.\n"
                                dictionary_table_update(
                                    "<" + string_substitution_array(graph, "{(.+?)}", row, row_headers, "subject",
                                                                    ignore) + ">")
                            else:
                                triple = triple[:-2] + " <" + graph + ">.\n"
                                dictionary_table_update("<" + graph + ">")
                        if duplicate == "yes":
                            if predicate in general_predicates:
                                if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                    try:
                                        output_file_descriptor.write(triple)
                                    except:
                                        output_file_descriptor.write(triple.encode("utf-8"))
                                    g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value]: {
                                        dic_table[subject] + "_" + dic_table[obj]: ""}})
                                    i += 1
                                elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                    dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                    try:
                                        output_file_descriptor.write(triple)
                                    except:
                                        output_file_descriptor.write(triple.encode("utf-8"))
                                    g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update(
                                        {dic_table[subject] + "_" + dic_table[obj]: ""})
                                    i += 1
                            else:
                                if dic_table[predicate] not in g_triples:
                                    try:
                                        output_file_descriptor.write(triple)
                                    except:
                                        output_file_descriptor.write(triple.encode("utf-8"))
                                    g_triples.update(
                                        {dic_table[predicate]: {dic_table[subject] + "_" + dic_table[obj]: ""}})
                                    i += 1
                                elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
                                    try:
                                        output_file_descriptor.write(triple)
                                    except:
                                        output_file_descriptor.write(triple.encode("utf-8"))
                                    g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
                                    i += 1
                        else:
                            try:
                                output_file_descriptor.write(triple)
                            except:
                                output_file_descriptor.write(triple.encode("utf-8"))
                            i += 1
                    if predicate[1:-1] in predicate_object_map.graph:
                        triple = subject + " " + predicate + " " + obj + ".\n"
                        if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in \
                                predicate_object_map.graph[predicate[1:-1]]:
                            if "{" in predicate_object_map.graph[predicate[1:-1]]:
                                triple = triple[:-2] + " <" + string_substitution_array(
                                    predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, row_headers, "subject",
                                    ignore) + ">.\n"
                                dictionary_table_update(
                                    "<" + string_substitution_array(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}",
                                                                    row, row_headers, "subject", ignore) + ">")
                            else:
                                triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
                                dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
                            if duplicate == "yes":
                                if predicate in general_predicates:
                                    if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
                                        try:
                                            output_file_descriptor.write(triple)
                                        except:
                                            output_file_descriptor.write(triple.encode("utf-8"))
                                        g_triples.update({dic_table[
                                                              predicate + "_" + predicate_object_map.object_map.value]: {
                                            dic_table[subject] + "_" + dic_table[obj]: ""}})
                                        i += 1
                                    elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[
                                        dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
                                        try:
                                            output_file_descriptor.write(triple)
                                        except:
                                            output_file_descriptor.write(triple.encode("utf-8"))
                                        g_triples[
                                            dic_table[predicate + "_" + predicate_object_map.object_map.value]].update(
                                            {dic_table[subject] + "_" + dic_table[obj]: ""})
                                        i += 1
                                else:
                                    if dic_table[predicate] not in g_triples:
                                        try:
                                            output_file_descriptor.write(triple)
                                        except:
                                            output_file_descriptor.write(triple.encode("utf-8"))
                                        g_triples.update(
                                            {dic_table[predicate]: {dic_table[subject] + "_" + dic_table[obj]: ""}})
                                        i += 1
                                    elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
                                        try:
                                            output_file_descriptor.write(triple)
                                        except:
                                            output_file_descriptor.write(triple.encode("utf-8"))
                                        g_triples[dic_table[predicate]].update(
                                            {dic_table[subject] + "_" + dic_table[obj]: ""})
                                        i += 1
                            else:
                                try:
                                    output_file_descriptor.write(triple)
                                except:
                                    output_file_descriptor.write(triple.encode("utf-8"))
                                i += 1
                object_list = []
            else:
                continue
            predicate = None
    return i


def semantify(config_path, log_path='error.log'):
    global logger
    logger = get_logger(log_path)

    config = ConfigParser(interpolation=ExtendedInterpolation())
    if isinstance(config_path, dict):
        config.read_dict(config_path)
    else:
        if not os.path.isfile(config_path):
            logger.error("The configuration file " + config_path + " does not exist. Aborting...")
            sys.exit(1)
        config.read(config_path)

    start_time = time.time()

    global duplicate
    duplicate = config["datasets"]["remove_duplicate"]

    global new_formulation
    if "new_formulation" in config["datasets"]:
        new_formulation = config["datasets"]["new_formulation"]
    else:
        new_formulation = "no"

    global output_format
    if "output_format" in config["datasets"]:
        output_format = config["datasets"]["output_format"]
    else:
        output_format = "n-triples"

    global mapping_partitions
    if "mapping_partitions" in config["datasets"]:
        mapping_partitions = config["datasets"]["mapping_partitions"]
    else:
        mapping_partitions = "no"

    enrichment = config["datasets"]["enrichment"]

    if not os.path.exists(config["datasets"]["output_folder"]):
        os.mkdir(config["datasets"]["output_folder"])

    global number_triple
    global blank_message
    global generated_subjects
    global user, password, port, host, datab
    global current_logical_dump
    global g_triples
    start = time.time()
    if config["datasets"]["all_in_one_file"] == "no":

        with ThreadPoolExecutor(max_workers=10) as executor:
            for dataset_number in range(int(config["datasets"]["number_of_datasets"])):
                dataset_i = "dataset" + str(int(dataset_number) + 1)
                triples_map_list = mapping_parser(config[dataset_i]["mapping"])
                global base
                base = extract_base(config[dataset_i]["mapping"])
                if "n-triples" == output_format.lower():
                    output_file = config["datasets"]["output_folder"] + "/" + config[dataset_i]["name"] + ".nt"
                else:
                    output_file = config["datasets"]["output_folder"] + "/" + config[dataset_i]["name"] + ".ttl"

                if "host" in config[dataset_i]:
                    user = config[dataset_i]["user"]
                    password = config[dataset_i]["password"]
                    port = config[dataset_i]["port"]
                    host = config[dataset_i]["host"]
                    datab = config[dataset_i]["db"]
                logger.info("Semantifying {}...".format(config[dataset_i]["name"]))

                with open(output_file, "w") as output_file_descriptor:
                    if "turtle" == output_format.lower():
                        string_prefixes = prefix_extraction(config[dataset_i]["mapping"])
                        output_file_descriptor.write(string_prefixes)
                    sorted_sources, predicate_list, order_list = files_sort(triples_map_list,
                                                                            config["datasets"]["ordered"], config)
                    if sorted_sources:
                        if order_list:
                            for source_type in order_list:
                                if source_type == "csv":
                                    for source in order_list[source_type]:
                                        if ".nt" in source:
                                            g = rdflib.Graph()
                                            g.parse(source, format="nt")
                                            for triples_map in sorted_sources[source_type][source]:
                                                if (len(sorted_sources[source_type][source][
                                                            triples_map].predicate_object_maps_list) > 0 and
                                                    sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list[
                                                        0].predicate_map.value != "None") or \
                                                        sorted_sources[source_type][source][
                                                            triples_map].subject_map.rdf_class != [None]:
                                                    results = g.query(sorted_sources[source_type][source][triples_map].iterator)
                                                    data = []
                                                    for row in results:
                                                        result_dict = {}
                                                        keys = list(row.__dict__["labels"].keys())
                                                        i = 0
                                                        while i < len(row):
                                                            result_dict[str(keys[i])] = str(row[keys[i]])
                                                            i += 1
                                                        data.append(result_dict)
                                                    blank_message = True
                                                    if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                        for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                            repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                            if repeat_output == "":
                                                                temp_generated = g_triples
                                                                g_triples = {}
                                                                with open(dump_output, "w") as logical_output_descriptor:
                                                                    current_logical_dump = dump_output
                                                                    number_triple += executor.submit(semantify_file,
                                                                                             sorted_sources[source_type][
                                                                                                 source][triples_map],
                                                                                             triples_map_list, ",",
                                                                                             logical_output_descriptor,
                                                                                             data, True).result()
                                                                    current_logical_dump = ""
                                                                g_triples = temp_generated
                                                                temp_generated = {}
                                                                if "jsonld" in dump_output:
                                                                    context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    jsonld_data = g.serialize(format="json-ld", context=context)
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(jsonld_data)
                                                                elif "n3" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    n3_data = g.serialize(format="n3")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(n3_data)
                                                                elif "rdfjson" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    json_data = generate_rdfjson(g)
                                                                    with open(dump_output, "w") as f:
                                                                        json.dump(json_data,f)
                                                                elif "rdfxml" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    xml_data = g.serialize(format="xml")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(xml_data)
                                                                elif "ttl" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    ttl_data = g.serialize(format="ttl")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(ttl_data)
                                                                elif "tar.gz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                    with tarfile.open(dump_output, "w:gz") as tar:
                                                                        tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                elif "tar.xz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                    with tarfile.open(dump_output, "w:xz") as tar:
                                                                        tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                elif ".gz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                    with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                        with gzip.open(dump_output, 'wb') as f_out:
                                                                            f_out.writelines(f_in)
                                                                elif ".zip" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                    zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                    zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                    zip.close()
                                                            else:
                                                                os.system("cp " + repeat_output + " " + dump_output) 
                                                    number_triple += executor.submit(semantify_file,
                                                                                     sorted_sources[source_type][
                                                                                         source][triples_map],
                                                                                     triples_map_list, ",",
                                                                                     output_file_descriptor,
                                                                                     data, True).result()
                                                    if duplicate == "yes":
                                                        predicate_list = release_PTT(
                                                            sorted_sources[source_type][source][triples_map],
                                                            predicate_list)
                                                    if mapping_partitions == "yes":
                                                        generated_subjects = release_subjects(
                                                            sorted_sources[source_type][source][triples_map],
                                                            generated_subjects)
                                        elif "endpoint:" in source:
                                            for triples_map in sorted_sources[source_type][source]:
                                                if (len(sorted_sources[source_type][source][
                                                            triples_map].predicate_object_maps_list) > 0 and
                                                    sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list[
                                                        0].predicate_map.value != "None") or \
                                                        sorted_sources[source_type][source][
                                                            triples_map].subject_map.rdf_class != [None]:
                                                    sparql = SPARQLWrapper(source.replace("endpoint:",""))
                                                    sparql.setQuery(sorted_sources[source_type][source][triples_map].iterator)
                                                    sparql.setReturnFormat(JSON)
                                                    results = sparql.query().convert()
                                                    data = []
                                                    for result in results["results"]["bindings"]:
                                                        result_dict = {}
                                                        for key, value in result.items():
                                                            result_dict[key] = value["value"]
                                                        data.append(result_dict)
                                                    if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                        for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                            repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                            if repeat_output == "":
                                                                temp_generated = g_triples
                                                                g_triples = {}
                                                                with open(dump_output, "w") as logical_output_descriptor:
                                                                    current_logical_dump = dump_output
                                                                    number_triple += executor.submit(semantify_file,
                                                                                             sorted_sources[source_type][
                                                                                                 source][triples_map],
                                                                                             triples_map_list, ",",
                                                                                             logical_output_descriptor,
                                                                                             data).result()
                                                                    current_logical_dump = ""
                                                                g_triples = temp_generated
                                                                temp_generated = {}
                                                                if "jsonld" in dump_output:
                                                                    context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    jsonld_data = g.serialize(format="json-ld", context=context)
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(jsonld_data)
                                                                elif "n3" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    n3_data = g.serialize(format="n3")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(n3_data)
                                                                elif "rdfjson" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    json_data = generate_rdfjson(g)
                                                                    with open(dump_output, "w") as f:
                                                                        json.dump(json_data,f)
                                                                elif "rdfxml" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    xml_data = g.serialize(format="xml")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(xml_data)
                                                                elif "ttl" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    ttl_data = g.serialize(format="ttl")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(ttl_data)
                                                                elif "tar.gz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                    with tarfile.open(dump_output, "w:gz") as tar:
                                                                        tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                elif "tar.xz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                    with tarfile.open(dump_output, "w:xz") as tar:
                                                                        tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                elif ".gz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                    with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                        with gzip.open(dump_output, 'wb') as f_out:
                                                                            f_out.writelines(f_in)
                                                                elif ".zip" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                    zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                    zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                    zip.close()
                                                            else:
                                                                os.system("cp " + repeat_output + " " + dump_output)
                                                    number_triple += executor.submit(semantify_file,
                                                                                     sorted_sources[source_type][
                                                                                         source][triples_map],
                                                                                     triples_map_list, ",",
                                                                                     output_file_descriptor,
                                                                                     data).result()
                                                    if duplicate == "yes":
                                                        predicate_list = release_PTT(
                                                            sorted_sources[source_type][source][triples_map],
                                                            predicate_list)
                                                    if mapping_partitions == "yes":
                                                        generated_subjects = release_subjects(
                                                            sorted_sources[source_type][source][triples_map],
                                                            generated_subjects)
                                        else:
                                            if enrichment == "yes":
                                                if ".csv" in source:
                                                    if source in delimiter:
                                                        reader = pd.read_csv(source, dtype=str, sep=delimiter[source], encoding="latin-1")
                                                    else:
                                                        reader = pd.read_csv(source, dtype=str, encoding="latin-1")
                                                else:
                                                    reader = pd.read_csv(source, dtype=str, sep='\t', encoding="latin-1")
                                                reader = reader.where(pd.notnull(reader), None)
                                                if duplicate == "yes":
                                                    reader = reader.drop_duplicates(keep='first')
                                                data = reader.to_dict(orient='records')
                                                for triples_map in sorted_sources[source_type][source]:
                                                    if "NonAssertedTriplesMap" not in sorted_sources[source_type][source][triples_map].mappings_type:
                                                        if (len(sorted_sources[source_type][source][
                                                                    triples_map].predicate_object_maps_list) > 0 and
                                                            sorted_sources[source_type][source][
                                                                triples_map].predicate_object_maps_list[
                                                                0].predicate_map.value != "None") or \
                                                                sorted_sources[source_type][source][
                                                                    triples_map].subject_map.rdf_class != [None]:
                                                            blank_message = True
                                                            if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                                for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                                    repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                                    if repeat_output == "":
                                                                        temp_generated = g_triples
                                                                        g_triples = {}
                                                                        with open(dump_output, "w") as logical_output_descriptor:
                                                                            current_logical_dump = dump_output
                                                                            number_triple += executor.submit(semantify_file,
                                                                                                     sorted_sources[source_type][
                                                                                                         source][triples_map],
                                                                                                     triples_map_list, ",",
                                                                                                     logical_output_descriptor,
                                                                                                     data, True).result()
                                                                            current_logical_dump = ""
                                                                        g_triples = temp_generated
                                                                        temp_generated = {}
                                                                        if "jsonld" in dump_output:
                                                                            context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            jsonld_data = g.serialize(format="json-ld", context=context)
                                                                            with open(dump_output, "w") as f:
                                                                                f.write(jsonld_data)
                                                                        elif "n3" in dump_output:
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            n3_data = g.serialize(format="n3")
                                                                            with open(dump_output, "w") as f:
                                                                                f.write(n3_data)
                                                                        elif "rdfjson" in dump_output:
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            json_data = generate_rdfjson(g)
                                                                            with open(dump_output, "w") as f:
                                                                                json.dump(json_data,f)
                                                                        elif "rdfxml" in dump_output:
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            xml_data = g.serialize(format="xml")
                                                                            with open(dump_output, "w") as f:
                                                                                f.write(xml_data)
                                                                        elif "ttl" in dump_output:
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            ttl_data = g.serialize(format="ttl")
                                                                            with open(dump_output, "w") as f:
                                                                                f.write(ttl_data)
                                                                        elif "tar.gz" in dump_output:
                                                                            os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                            with tarfile.open(dump_output, "w:gz") as tar:
                                                                                tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                        elif "tar.xz" in dump_output:
                                                                            os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                            with tarfile.open(dump_output, "w:xz") as tar:
                                                                                tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                        elif ".gz" in dump_output:
                                                                            os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                            with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                                with gzip.open(dump_output, 'wb') as f_out:
                                                                                    f_out.writelines(f_in)
                                                                        elif ".zip" in dump_output:
                                                                            os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                            zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                            zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                            zip.close()
                                                                    else:
                                                                        os.system("cp " + repeat_output + " " + dump_output)
                                                            number_triple += executor.submit(semantify_file,
                                                                                             sorted_sources[source_type][
                                                                                                 source][triples_map],
                                                                                             triples_map_list, ",",
                                                                                             output_file_descriptor,
                                                                                             data, True).result()
                                                            if duplicate == "yes":
                                                                predicate_list = release_PTT(
                                                                    sorted_sources[source_type][source][triples_map],
                                                                    predicate_list)
                                                            if mapping_partitions == "yes":
                                                                generated_subjects = release_subjects(
                                                                    sorted_sources[source_type][source][triples_map],
                                                                    generated_subjects)
                                            else:
                                                for triples_map in sorted_sources[source_type][source]:
                                                    if "NonAssertedTriplesMap" not in sorted_sources[source_type][source][triples_map].mappings_type:
                                                        if (len(sorted_sources[source_type][source][
                                                                    triples_map].predicate_object_maps_list) > 0 and
                                                            sorted_sources[source_type][source][
                                                                triples_map].predicate_object_maps_list[
                                                                0].predicate_map.value != "None") or \
                                                                sorted_sources[source_type][source][
                                                                    triples_map].subject_map.rdf_class != [None]:
                                                            with open(source, "r", encoding="latin-1") as input_file_descriptor:
                                                                if ".csv" in source:
                                                                    if source in delimiter:
                                                                        data = csv.DictReader(input_file_descriptor, delimiter=delimiter[source])
                                                                    else:
                                                                        data = csv.DictReader(input_file_descriptor, delimiter=',')
                                                                else:
                                                                    data = csv.DictReader(input_file_descriptor, delimiter='\t')
                                                                blank_message = True
                                                                if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                                    for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                                        repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                                        if repeat_output == "":
                                                                            temp_generated = g_triples
                                                                            g_triples = {}
                                                                            with open(dump_output, "w") as logical_output_descriptor:
                                                                                current_logical_dump = dump_output
                                                                                number_triple += executor.submit(semantify_file,
                                                                                                         sorted_sources[source_type][
                                                                                                             source][triples_map],
                                                                                                         triples_map_list, ",",
                                                                                                         logical_output_descriptor,
                                                                                                         data, True).result()
                                                                                current_logical_dump = ""
                                                                            g_triples = temp_generated
                                                                            temp_generated = {}
                                                                            if "jsonld" in dump_output:
                                                                                context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                                g = rdflib.Graph()
                                                                                g.parse(dump_output, format="nt")
                                                                                jsonld_data = g.serialize(format="json-ld", context=context)
                                                                                with open(dump_output, "w") as f:
                                                                                    f.write(jsonld_data)
                                                                            elif "n3" in dump_output:
                                                                                g = rdflib.Graph()
                                                                                g.parse(dump_output, format="nt")
                                                                                n3_data = g.serialize(format="n3")
                                                                                with open(dump_output, "w") as f:
                                                                                    f.write(n3_data)
                                                                            elif "rdfjson" in dump_output:
                                                                                g = rdflib.Graph()
                                                                                g.parse(dump_output, format="nt")
                                                                                json_data = generate_rdfjson(g)
                                                                                with open(dump_output, "w") as f:
                                                                                    json.dump(json_data,f)
                                                                            elif "rdfxml" in dump_output:
                                                                                g = rdflib.Graph()
                                                                                g.parse(dump_output, format="nt")
                                                                                xml_data = g.serialize(format="xml")
                                                                                with open(dump_output, "w") as f:
                                                                                    f.write(xml_data)
                                                                            elif "ttl" in dump_output:
                                                                                g = rdflib.Graph()
                                                                                g.parse(dump_output, format="nt")
                                                                                ttl_data = g.serialize(format="ttl")
                                                                                with open(dump_output, "w") as f:
                                                                                    f.write(ttl_data)
                                                                            elif "tar.gz" in dump_output:
                                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                                with tarfile.open(dump_output, "w:gz") as tar:
                                                                                    tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                            elif "tar.xz" in dump_output:
                                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                                with tarfile.open(dump_output, "w:xz") as tar:
                                                                                    tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                            elif ".gz" in dump_output:
                                                                                os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                                with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                                    with gzip.open(dump_output, 'wb') as f_out:
                                                                                        f_out.writelines(f_in)
                                                                            elif ".zip" in dump_output:
                                                                                os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                                zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                                zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                                zip.close()
                                                                        else:
                                                                            os.system("cp " + repeat_output + " " + dump_output)
                                                                number_triple += executor.submit(semantify_file,
                                                                                                 sorted_sources[source_type][
                                                                                                     source][triples_map],
                                                                                                 triples_map_list, ",",
                                                                                                 output_file_descriptor,
                                                                                                 data, True).result()
                                                                if duplicate == "yes":
                                                                    predicate_list = release_PTT(
                                                                        sorted_sources[source_type][source][triples_map],
                                                                        predicate_list)
                                                                if mapping_partitions == "yes":
                                                                    generated_subjects = release_subjects(
                                                                        sorted_sources[source_type][source][triples_map],
                                                                        generated_subjects)
                                elif source_type == "JSONPath":
                                    for source in order_list[source_type]:
                                        for triples_map in sorted_sources[source_type][source]:
                                            if (len(sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list) > 0 and
                                                sorted_sources[source_type][source][
                                                    triples_map].predicate_object_maps_list[
                                                    0].predicate_map.value != "None") or \
                                                    sorted_sources[source_type][source][
                                                        triples_map].subject_map.rdf_class != [None]:
                                                if "http" in sorted_sources[source_type][source][
                                                    triples_map].data_source:
                                                    file_source = sorted_sources[source_type][source][triples_map].data_source
                                                    if "#" in file_source:
                                                        file = file_source.split("#")[1]
                                                    else:
                                                        file = file_source.split("/")[len(file_source.split("/"))-1]
                                                    if "gz" in file_source or "zip" in file_source or "tar.xz" in file_source or "tar.gz" in file_source:
                                                        response = requests.get(file_source)
                                                        with open(file, "wb") as f:
                                                            f.write(response.content)
                                                        if "zip" in file_source:
                                                            with zipfile.ZipFile(file, 'r') as zip_ref:
                                                                zip_ref.extractall()
                                                            data = json.load(open(file.replace(".zip","")))
                                                        elif "tar.xz" in file_source or "tar.gz" in file_source:
                                                            with tarfile.open(file, "r") as tar:
                                                                tar.extractall()
                                                            if "tar.xz" in file_source:
                                                                data = json.load(open(file.replace(".tar.xz","")))
                                                            else:
                                                                data = json.load(open(file.replace(".tar.gz","")))
                                                        elif "gz" in file_source:
                                                            with open(file, "rb") as gz_file:
                                                                with open(file.replace(".gz",""), "wb") as txt_file:
                                                                    shutil.copyfileobj(gzip.GzipFile(fileobj=gz_file), txt_file)
                                                            data = json.load(open(file.replace(".gz","")))
                                                    else:
                                                        response = urlopen(file_source)
                                                        data = json.loads(response.read())
                                                else:
                                                    data = json.load(open(source))
                                                blank_message = True
                                                if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                    for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                        repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                        if repeat_output == "":
                                                            temp_generated = g_triples
                                                            g_triples = {}
                                                            with open(dump_output, "w") as logical_output_descriptor:
                                                                current_logical_dump = dump_output
                                                                number_triple += executor.submit(semantify_json,
                                                                                         sorted_sources[source_type][source][
                                                                                             triples_map], triples_map_list,
                                                                                         ",", logical_output_descriptor, data,
                                                                                         sorted_sources[source_type][source][
                                                                                             triples_map].iterator).result()
                                                                current_logical_dump = ""
                                                            g_triples = temp_generated
                                                            temp_generated = {}
                                                            if "jsonld" in dump_output:
                                                                context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                jsonld_data = g.serialize(format="json-ld", context=context)
                                                                with open(dump_output, "w") as f:
                                                                    f.write(jsonld_data)
                                                            elif "n3" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                n3_data = g.serialize(format="n3")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(n3_data)
                                                            elif "rdfjson" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                json_data = generate_rdfjson(g)
                                                                with open(dump_output, "w") as f:
                                                                    json.dump(json_data,f)
                                                            elif "rdfxml" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                xml_data = g.serialize(format="xml")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(xml_data)
                                                            elif "ttl" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                ttl_data = g.serialize(format="ttl")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(ttl_data)
                                                            elif "tar.gz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                with tarfile.open(dump_output, "w:gz") as tar:
                                                                    tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                            elif "tar.xz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                with tarfile.open(dump_output, "w:xz") as tar:
                                                                    tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                            elif ".gz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                    with gzip.open(dump_output, 'wb') as f_out:
                                                                        f_out.writelines(f_in)
                                                            elif ".zip" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                zip.close()
                                                        else:
                                                            os.system("cp " + repeat_output + " " + dump_output)
                                                number_triple += executor.submit(semantify_json,
                                                                                 sorted_sources[source_type][source][
                                                                                     triples_map], triples_map_list,
                                                                                 ",", output_file_descriptor, data,
                                                                                 sorted_sources[source_type][source][
                                                                                     triples_map].iterator).result()
                                                if duplicate == "yes":
                                                    predicate_list = release_PTT(
                                                        sorted_sources[source_type][source][triples_map],
                                                        predicate_list)
                                                if mapping_partitions == "yes":
                                                    generated_subjects = release_subjects(
                                                        sorted_sources[source_type][source][triples_map],
                                                        generated_subjects)
                                elif source_type == "XPath":
                                    for source in order_list[source_type]:
                                        for triples_map in sorted_sources[source_type][source]:
                                            if (len(sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list) > 0 and
                                                sorted_sources[source_type][source][
                                                    triples_map].predicate_object_maps_list[
                                                    0].predicate_map.value != "None") or \
                                                    sorted_sources[source_type][source][
                                                        triples_map].subject_map.rdf_class != [None]:
                                                blank_message = True
                                                if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                    for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                        repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                        if repeat_output == "":
                                                            temp_generated = g_triples
                                                            g_triples = {}
                                                            with open(dump_output, "w") as logical_output_descriptor:
                                                                current_logical_dump = dump_output
                                                                number_triple += executor.submit(semantify_xml,
                                                                                         sorted_sources[source_type][source][
                                                                                             triples_map], triples_map_list,
                                                                                         logical_output_descriptor).result()
                                                                current_logical_dump = ""
                                                            g_triples = temp_generated
                                                            temp_generated = {}
                                                            if "jsonld" in dump_output:
                                                                context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                jsonld_data = g.serialize(format="json-ld", context=context)
                                                                with open(dump_output, "w") as f:
                                                                    f.write(jsonld_data)
                                                            elif "n3" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                n3_data = g.serialize(format="n3")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(n3_data)
                                                            elif "rdfjson" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                json_data = generate_rdfjson(g)
                                                                with open(dump_output, "w") as f:
                                                                    json.dump(json_data,f)
                                                            elif "rdfxml" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                xml_data = g.serialize(format="xml")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(xml_data)
                                                            elif "ttl" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                ttl_data = g.serialize(format="ttl")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(ttl_data)
                                                            elif "tar.gz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                with tarfile.open(dump_output, "w:gz") as tar:
                                                                    tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                            elif "tar.xz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                with tarfile.open(dump_output, "w:xz") as tar:
                                                                    tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                            elif ".gz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                    with gzip.open(dump_output, 'wb') as f_out:
                                                                        f_out.writelines(f_in)
                                                            elif ".zip" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                zip.close()
                                                        else:
                                                            os.system("cp " + repeat_output + " " + dump_output)
                                                number_triple += executor.submit(semantify_xml,
                                                                                 sorted_sources[source_type][source][
                                                                                     triples_map], triples_map_list,
                                                                                 output_file_descriptor).result()
                                                if duplicate == "yes":
                                                    predicate_list = release_PTT(
                                                        sorted_sources[source_type][source][triples_map],
                                                        predicate_list)
                                                if mapping_partitions == "yes":
                                                    generated_subjects = release_subjects(
                                                        sorted_sources[source_type][source][triples_map],
                                                        generated_subjects)
                        else:
                            for source_type in sorted_sources:
                                if source_type == "csv":
                                    for source in sorted_sources[source_type]:
                                        if ".nt" in source:
                                            g = rdflib.Graph()
                                            g.parse(source, format="nt")
                                            for triples_map in sorted_sources[source_type][source]:
                                                if (len(sorted_sources[source_type][source][
                                                            triples_map].predicate_object_maps_list) > 0 and
                                                    sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list[
                                                        0].predicate_map.value != "None") or \
                                                        sorted_sources[source_type][source][
                                                            triples_map].subject_map.rdf_class != [None]:
                                                    results = g.query(sorted_sources[source_type][source][triples_map].iterator)
                                                    data = []
                                                    for row in results:
                                                        result_dict = {}
                                                        keys = list(row.__dict__["labels"].keys())
                                                        i = 0
                                                        while i < len(row):
                                                            result_dict[str(keys[i])] = str(row[keys[i]])
                                                            i += 1
                                                        data.append(result_dict)
                                                    blank_message = True
                                                    if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                        for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                            repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                            if repeat_output == "":    
                                                                temp_generated = g_triples
                                                                g_triples = {}
                                                                with open(dump_output, "w") as logical_output_descriptor:
                                                                    current_logical_dump = dump_output
                                                                    number_triple += executor.submit(semantify_file,
                                                                                             sorted_sources[source_type][
                                                                                                 source][triples_map],
                                                                                             triples_map_list, ",",
                                                                                             logical_output_descriptor,
                                                                                             data, True).result()
                                                                    current_logical_dump = ""
                                                                g_triples = temp_generated
                                                                temp_generated = {}
                                                                if "jsonld" in dump_output:
                                                                    context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    jsonld_data = g.serialize(format="json-ld", context=context)
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(jsonld_data)
                                                                elif "n3" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    n3_data = g.serialize(format="n3")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(n3_data)
                                                                elif "rdfjson" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    json_data = generate_rdfjson(g)
                                                                    with open(dump_output, "w") as f:
                                                                        json.dump(json_data,f)
                                                                elif "rdfxml" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    xml_data = g.serialize(format="xml")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(xml_data)
                                                                elif "ttl" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    ttl_data = g.serialize(format="ttl")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(ttl_data)
                                                                elif "tar.gz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                    with tarfile.open(dump_output, "w:gz") as tar:
                                                                        tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                elif "tar.xz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                    with tarfile.open(dump_output, "w:xz") as tar:
                                                                        tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                elif ".gz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                    with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                        with gzip.open(dump_output, 'wb') as f_out:
                                                                            f_out.writelines(f_in)
                                                                elif ".zip" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                    zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                    zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                    zip.close()
                                                            else:
                                                                os.system("cp " + repeat_output + " " + dump_output)
                                                    number_triple += executor.submit(semantify_file,
                                                                                     sorted_sources[source_type][
                                                                                         source][triples_map],
                                                                                     triples_map_list, ",",
                                                                                     output_file_descriptor,
                                                                                     data, True).result()
                                                    if duplicate == "yes":
                                                        predicate_list = release_PTT(
                                                            sorted_sources[source_type][source][triples_map],
                                                            predicate_list)
                                                    if mapping_partitions == "yes":
                                                        generated_subjects = release_subjects(
                                                            sorted_sources[source_type][source][triples_map],
                                                            generated_subjects)
                                        elif "endpoint:" in source:
                                            for triples_map in sorted_sources[source_type][source]:
                                                if (len(sorted_sources[source_type][source][
                                                            triples_map].predicate_object_maps_list) > 0 and
                                                    sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list[
                                                        0].predicate_map.value != "None") or \
                                                        sorted_sources[source_type][source][
                                                            triples_map].subject_map.rdf_class != [None]:
                                                    sparql = SPARQLWrapper(source.replace("endpoint:",""))
                                                    sparql.setQuery(sorted_sources[source_type][source][triples_map].iterator)
                                                    sparql.setReturnFormat(JSON)
                                                    results = sparql.query().convert()
                                                    data = []
                                                    for result in results["results"]["bindings"]:
                                                        result_dict = {}
                                                        for key, value in result.items():
                                                            result_dict[key] = value["value"]
                                                        data.append(result_dict)
                                                    if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                        for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                            repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                            if repeat_output == "": 
                                                                temp_generated = g_triples
                                                                g_triples = {}   
                                                                with open(dump_output, "w") as logical_output_descriptor:
                                                                    current_logical_dump = dump_output
                                                                    number_triple += executor.submit(semantify_file,
                                                                                             sorted_sources[source_type][
                                                                                                 source][triples_map],
                                                                                             triples_map_list, ",",
                                                                                             logical_output_descriptor,
                                                                                             data, True).result()
                                                                    current_logical_dump = ""
                                                                g_triples = temp_generated
                                                                temp_generated = {}
                                                                if "jsonld" in dump_output:
                                                                    context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    jsonld_data = g.serialize(format="json-ld", context=context)
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(jsonld_data)
                                                                elif "n3" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    n3_data = g.serialize(format="n3")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(n3_data)
                                                                elif "rdfjson" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    json_data = generate_rdfjson(g)
                                                                    with open(dump_output, "w") as f:
                                                                        json.dump(json_data,f)
                                                                elif "rdfxml" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    xml_data = g.serialize(format="xml")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(xml_data)
                                                                elif "ttl" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    ttl_data = g.serialize(format="ttl")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(ttl_data)
                                                                elif "tar.gz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                    with tarfile.open(dump_output, "w:gz") as tar:
                                                                        tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                elif "tar.xz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                    with tarfile.open(dump_output, "w:xz") as tar:
                                                                        tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                elif ".gz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                    with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                        with gzip.open(dump_output, 'wb') as f_out:
                                                                            f_out.writelines(f_in)
                                                                elif ".zip" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                    zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                    zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                    zip.close()
                                                            else:
                                                                os.system("cp " + repeat_output + " " + dump_output)
                                                    number_triple += executor.submit(semantify_file,
                                                                                     sorted_sources[source_type][
                                                                                         source][triples_map],
                                                                                     triples_map_list, ",",
                                                                                     output_file_descriptor,
                                                                                     data, True).result()
                                                    if duplicate == "yes":
                                                        predicate_list = release_PTT(
                                                            sorted_sources[source_type][source][triples_map],
                                                            predicate_list)
                                                    if mapping_partitions == "yes":
                                                        generated_subjects = release_subjects(
                                                            sorted_sources[source_type][source][triples_map],
                                                            generated_subjects)
                                        else:
                                            if enrichment == "yes":
                                                if ".csv" in source:
                                                    if source in delimiter:
                                                        reader = pd.read_csv(source, dtype=str, sep=delimiter[source], encoding="latin-1")
                                                    else:
                                                        reader = pd.read_csv(source, dtype=str, encoding="latin-1")  # latin-1
                                                else:
                                                    reader = pd.read_csv(source, dtype=str, sep="\t", header=0,
                                                                         encoding="latin-1")
                                                reader = reader.where(pd.notnull(reader), None)
                                                if duplicate == "yes":
                                                    reader = reader.drop_duplicates(keep='first')
                                                data = reader.to_dict(orient='records')
                                                for triples_map in sorted_sources[source_type][source]:
                                                    if "NonAssertedTriplesMap" not in sorted_sources[source_type][source][triples_map].mappings_type:
                                                        if (len(sorted_sources[source_type][source][
                                                                    triples_map].predicate_object_maps_list) > 0 and
                                                            sorted_sources[source_type][source][
                                                                triples_map].predicate_object_maps_list[
                                                                0].predicate_map.value != "None") or \
                                                                sorted_sources[source_type][source][
                                                                    triples_map].subject_map.rdf_class != [None]:
                                                            blank_message = True
                                                            if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                                for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                                    repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                                    if repeat_output == "":
                                                                        temp_generated = g_triples
                                                                        g_triples = {}
                                                                        with open(dump_output, "w") as logical_output_descriptor:
                                                                            current_logical_dump = dump_output
                                                                            number_triple += executor.submit(semantify_file,
                                                                                                     sorted_sources[source_type][
                                                                                                         source][triples_map],
                                                                                                     triples_map_list, ",",
                                                                                                     logical_output_descriptor,
                                                                                                     data, True).result()
                                                                            current_logical_dump = ""
                                                                        g_triples = temp_generated
                                                                        temp_generated = {}
                                                                        if "jsonld" in dump_output:
                                                                            context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            jsonld_data = g.serialize(format="json-ld", context=context)
                                                                            with open(dump_output, "w") as f:
                                                                                f.write(jsonld_data)
                                                                        elif "n3" in dump_output:
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            n3_data = g.serialize(format="n3")
                                                                            with open(dump_output, "w") as f:
                                                                                f.write(n3_data)
                                                                        elif "rdfjson" in dump_output:
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            json_data = generate_rdfjson(g)
                                                                            with open(dump_output, "w") as f:
                                                                                json.dump(json_data,f)
                                                                        elif "rdfxml" in dump_output:
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            xml_data = g.serialize(format="xml")
                                                                            with open(dump_output, "w") as f:
                                                                                f.write(xml_data)
                                                                        elif "ttl" in dump_output:
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            ttl_data = g.serialize(format="ttl")
                                                                            with open(dump_output, "w") as f:
                                                                                f.write(ttl_data)
                                                                        elif "tar.gz" in dump_output:
                                                                            os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                            with tarfile.open(dump_output, "w:gz") as tar:
                                                                                tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                        elif "tar.xz" in dump_output:
                                                                            os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                            with tarfile.open(dump_output, "w:xz") as tar:
                                                                                tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                        elif ".gz" in dump_output:
                                                                            os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                            with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                                with gzip.open(dump_output, 'wb') as f_out:
                                                                                    f_out.writelines(f_in)
                                                                        elif ".zip" in dump_output:
                                                                            os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                            zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                            zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                            zip.close()
                                                                    else:
                                                                        os.system("cp " + repeat_output + " " + dump_output)
                                                            number_triple += executor.submit(semantify_file,
                                                                                             sorted_sources[source_type][
                                                                                                 source][triples_map],
                                                                                             triples_map_list, ",",
                                                                                             output_file_descriptor,
                                                                                             data, True).result()
                                                            if duplicate == "yes":
                                                                predicate_list = release_PTT(
                                                                    sorted_sources[source_type][source][triples_map],
                                                                    predicate_list)
                                                            if mapping_partitions == "yes":
                                                                generated_subjects = release_subjects(
                                                                    sorted_sources[source_type][source][triples_map],
                                                                    generated_subjects)
                                            else:
                                                for triples_map in sorted_sources[source_type][source]:
                                                    if "NonAssertedTriplesMap" not in sorted_sources[source_type][source][triples_map].mappings_type:
                                                        if (len(sorted_sources[source_type][source][
                                                                    triples_map].predicate_object_maps_list) > 0 and
                                                            sorted_sources[source_type][source][
                                                                triples_map].predicate_object_maps_list[
                                                                0].predicate_map.value != "None") or \
                                                                sorted_sources[source_type][source][
                                                                    triples_map].subject_map.rdf_class != [None]:
                                                            blank_message = True
                                                            with open(source, "r", encoding="latin-1") as input_file_descriptor:
                                                                if ".csv" in source:
                                                                    if source in delimiter:
                                                                        data = csv.DictReader(input_file_descriptor, delimiter=delimiter[source])
                                                                    else:
                                                                        data = csv.DictReader(input_file_descriptor, delimiter=',')
                                                                else:
                                                                    data = csv.DictReader(input_file_descriptor, delimiter='\t')
                                                                if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                                    for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                                        repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                                        if repeat_output == "":
                                                                            temp_generated = g_triples
                                                                            g_triples = {}
                                                                            with open(dump_output, "w") as logical_output_descriptor:
                                                                                current_logical_dump = dump_output
                                                                                number_triple += executor.submit(semantify_file,
                                                                                                         sorted_sources[source_type][
                                                                                                             source][triples_map],
                                                                                                         triples_map_list, ",",
                                                                                                         logical_output_descriptor,
                                                                                                         data, True).result()
                                                                                current_logical_dump = ""
                                                                            g_triples = temp_generated
                                                                            temp_generated = {}
                                                                            if "jsonld" in dump_output:
                                                                                context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                                g = rdflib.Graph()
                                                                                g.parse(dump_output, format="nt")
                                                                                jsonld_data = g.serialize(format="json-ld", context=context)
                                                                                with open(dump_output, "w") as f:
                                                                                    f.write(jsonld_data)
                                                                            elif "n3" in dump_output:
                                                                                g = rdflib.Graph()
                                                                                g.parse(dump_output, format="nt")
                                                                                n3_data = g.serialize(format="n3")
                                                                                with open(dump_output, "w") as f:
                                                                                    f.write(n3_data)
                                                                            elif "rdfjson" in dump_output:
                                                                                g = rdflib.Graph()
                                                                                g.parse(dump_output, format="nt")
                                                                                json_data = generate_rdfjson(g)
                                                                                with open(dump_output, "w") as f:
                                                                                    json.dump(json_data,f)
                                                                            elif "rdfxml" in dump_output:
                                                                                g = rdflib.Graph()
                                                                                g.parse(dump_output, format="nt")
                                                                                xml_data = g.serialize(format="xml")
                                                                                with open(dump_output, "w") as f:
                                                                                    f.write(xml_data)
                                                                            elif "ttl" in dump_output:
                                                                                g = rdflib.Graph()
                                                                                g.parse(dump_output, format="nt")
                                                                                ttl_data = g.serialize(format="ttl")
                                                                                with open(dump_output, "w") as f:
                                                                                    f.write(ttl_data)
                                                                            elif "tar.gz" in dump_output:
                                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                                with tarfile.open(dump_output, "w:gz") as tar:
                                                                                    tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                            elif "tar.xz" in dump_output:
                                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                                with tarfile.open(dump_output, "w:xz") as tar:
                                                                                    tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                            elif ".gz" in dump_output:
                                                                                os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                                with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                                    with gzip.open(dump_output, 'wb') as f_out:
                                                                                        f_out.writelines(f_in)
                                                                            elif ".zip" in dump_output:
                                                                                os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                                zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                                zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                                zip.close()
                                                                        else:
                                                                            os.system("cp " + repeat_output + " " + dump_output)
                                                                number_triple += executor.submit(semantify_file,
                                                                                                 sorted_sources[source_type][
                                                                                                     source][triples_map],
                                                                                                 triples_map_list, ",",
                                                                                                 output_file_descriptor,
                                                                                                 data, True).result()
                                                                if duplicate == "yes":
                                                                    predicate_list = release_PTT(
                                                                        sorted_sources[source_type][source][triples_map],
                                                                        predicate_list)
                                                                if mapping_partitions == "yes":
                                                                    generated_subjects = release_subjects(
                                                                        sorted_sources[source_type][source][triples_map],
                                                                        generated_subjects)
                                elif source_type == "JSONPath":
                                    for source in sorted_sources[source_type]:
                                        for triples_map in sorted_sources[source_type][source]:
                                            if (len(sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list) > 0 and
                                                sorted_sources[source_type][source][
                                                    triples_map].predicate_object_maps_list[
                                                    0].predicate_map.value != "None") or \
                                                    sorted_sources[source_type][source][
                                                        triples_map].subject_map.rdf_class != [None]:
                                                if "http" in sorted_sources[source_type][source][
                                                    triples_map].data_source:
                                                    file_source = sorted_sources[source_type][source][triples_map].data_source
                                                    if "#" in file_source:
                                                        file = file_source.split("#")[1]
                                                    else:
                                                        file = file_source.split("/")[len(file_source.split("/"))-1]
                                                    if "gz" in file_source or "zip" in file_source or "tar.xz" in file_source or "tar.gz" in file_source:
                                                        response = requests.get(file_source)
                                                        with open(file, "wb") as f:
                                                            f.write(response.content)
                                                        if "zip" in file_source:
                                                            with zipfile.ZipFile(file, 'r') as zip_ref:
                                                                zip_ref.extractall()
                                                            data = json.load(open(file.replace(".zip","")))
                                                        elif "tar.xz" in file_source or "tar.gz" in file_source:
                                                            with tarfile.open(file, "r") as tar:
                                                                tar.extractall()
                                                            if "tar.xz" in file_source:
                                                                data = json.load(open(file.replace(".tar.xz","")))
                                                            else:
                                                                data = json.load(open(file.replace(".tar.gz","")))
                                                        elif "gz" in file_source:
                                                            with open(file, "rb") as gz_file:
                                                                with open(file.replace(".gz",""), "wb") as txt_file:
                                                                    shutil.copyfileobj(gzip.GzipFile(fileobj=gz_file), txt_file)
                                                            data = json.load(open(file.replace(".gz","")))
                                                    else:
                                                        response = urlopen(file_source)
                                                        data = json.loads(response.read())
                                                else:
                                                    data = json.load(open(
                                                        sorted_sources[source_type][source][triples_map].data_source))
                                                blank_message = True
                                                if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                    for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                        repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                        if repeat_output == "":
                                                            temp_generated = g_triples
                                                            g_triples = {}
                                                            with open(dump_output, "w") as logical_output_descriptor:
                                                                current_logical_dump = dump_output
                                                                number_triple += executor.submit(semantify_json,
                                                                                         sorted_sources[source_type][source][
                                                                                             triples_map], triples_map_list,
                                                                                         ",", logical_output_descriptor, data,
                                                                                         sorted_sources[source_type][source][
                                                                                             triples_map].iterator).result()
                                                                current_logical_dump = ""
                                                            g_triples = temp_generated
                                                            temp_generated = {}
                                                            if "jsonld" in dump_output:
                                                                context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                jsonld_data = g.serialize(format="json-ld", context=context)
                                                                with open(dump_output, "w") as f:
                                                                    f.write(jsonld_data)
                                                            elif "n3" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                n3_data = g.serialize(format="n3")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(n3_data)
                                                            elif "rdfjson" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                json_data = generate_rdfjson(g)
                                                                with open(dump_output, "w") as f:
                                                                    json.dump(json_data,f)
                                                            elif "rdfxml" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                xml_data = g.serialize(format="xml")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(xml_data)
                                                            elif "ttl" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                ttl_data = g.serialize(format="ttl")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(ttl_data)
                                                            elif "tar.gz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                with tarfile.open(dump_output, "w:gz") as tar:
                                                                    tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                            elif "tar.xz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                with tarfile.open(dump_output, "w:xz") as tar:
                                                                    tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                            elif ".gz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                    with gzip.open(dump_output, 'wb') as f_out:
                                                                        f_out.writelines(f_in)
                                                            elif ".zip" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                zip.close()
                                                        else:
                                                            os.system("cp " + repeat_output + " " + dump_output)
                                                number_triple += executor.submit(semantify_json,
                                                                                 sorted_sources[source_type][source][
                                                                                     triples_map], triples_map_list,
                                                                                 ",", output_file_descriptor, data,
                                                                                 sorted_sources[source_type][source][
                                                                                     triples_map].iterator).result()
                                                if duplicate == "yes":
                                                    predicate_list = release_PTT(
                                                        sorted_sources[source_type][source][triples_map],
                                                        predicate_list)
                                                if mapping_partitions == "yes":
                                                    generated_subjects = release_subjects(
                                                        sorted_sources[source_type][source][triples_map],
                                                        generated_subjects)
                                elif source_type == "XPath":
                                    for source in sorted_sources[source_type]:
                                        for triples_map in sorted_sources[source_type][source]:
                                            if (len(sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list) > 0 and
                                                sorted_sources[source_type][source][
                                                    triples_map].predicate_object_maps_list[
                                                    0].predicate_map.value != "None") or \
                                                    sorted_sources[source_type][source][
                                                        triples_map].subject_map.rdf_class != [None]:
                                                blank_message = True
                                                if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                    for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                        repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                        if repeat_output == "":
                                                            temp_generated = g_triples
                                                            g_triples = {}
                                                            with open(dump_output, "w") as logical_output_descriptor:
                                                                current_logical_dump = dump_output
                                                                number_triple += executor.submit(semantify_xml,
                                                                                         sorted_sources[source_type][source][
                                                                                             triples_map], triples_map_list,
                                                                                         logical_output_descriptor).result()
                                                                current_logical_dump = ""
                                                            g_triples = temp_generated
                                                            temp_generated = {}
                                                            if "jsonld" in dump_output:
                                                                context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                jsonld_data = g.serialize(format="json-ld", context=context)
                                                                with open(dump_output, "w") as f:
                                                                    f.write(jsonld_data)
                                                            elif "n3" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                n3_data = g.serialize(format="n3")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(n3_data)
                                                            elif "rdfjson" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                json_data = generate_rdfjson(g)
                                                                with open(dump_output, "w") as f:
                                                                    json.dump(json_data,f)
                                                            elif "rdfxml" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                xml_data = g.serialize(format="xml")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(xml_data)
                                                            elif "ttl" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                ttl_data = g.serialize(format="ttl")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(ttl_data)
                                                            elif "tar.gz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                with tarfile.open(dump_output, "w:gz") as tar:
                                                                    tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                            elif "tar.xz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                with tarfile.open(dump_output, "w:xz") as tar:
                                                                    tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                            elif ".gz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                    with gzip.open(dump_output, 'wb') as f_out:
                                                                        f_out.writelines(f_in)
                                                            elif ".zip" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                zip.close()
                                                        else:
                                                            os.system("cp " + repeat_output + " " + dump_output)
                                                number_triple += executor.submit(semantify_xml,
                                                                                 sorted_sources[source_type][source][
                                                                                     triples_map], triples_map_list,
                                                                                 output_file_descriptor).result()
                                                if duplicate == "yes":
                                                    predicate_list = release_PTT(
                                                        sorted_sources[source_type][source][triples_map],
                                                        predicate_list)
                                                if mapping_partitions == "yes":
                                                    generated_subjects = release_subjects(
                                                        sorted_sources[source_type][source][triples_map],
                                                        generated_subjects)
                    if predicate_list:
                        for source_type in sorted_sources:
                            blank_message = True
                            if str(source_type).lower() != "csv" and source_type != "JSONPath" and source_type != "XPath":
                                if source_type == "mysql":
                                    for source in sorted_sources[source_type]:
                                        db = connector.connect(host=config[dataset_i]["host"],
                                                               port=int(config[dataset_i]["port"]),
                                                               user=config[dataset_i]["user"],
                                                               password=config[dataset_i]["password"])
                                        cursor = db.cursor(buffered=True)
                                        if config[dataset_i]["db"].lower() != "none":
                                            cursor.execute("use " + config[dataset_i]["db"])
                                        else:
                                            if database != "None":
                                                cursor.execute("use " + database)
                                        cursor.execute(source)
                                        row_headers = [x[0] for x in cursor.description]
                                        data = []
                                        for triples_map in sorted_sources[source_type][source]:
                                            if (len(sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list) > 0 and
                                                sorted_sources[source_type][source][
                                                    triples_map].predicate_object_maps_list[
                                                    0].predicate_map.value != "None") or \
                                                    sorted_sources[source_type][source][
                                                        triples_map].subject_map.rdf_class != [None]:
                                                logger.info("TM: " + sorted_sources[source_type][source][
                                                    triples_map].triples_map_name)
                                                if mapping_partitions == "yes":
                                                    if sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list[
                                                        0].predicate_map.mapping_type == "constant" or \
                                                            sorted_sources[source_type][source][
                                                                triples_map].predicate_object_maps_list[
                                                                0].predicate_map.mapping_type == "constant shortcut":
                                                        predicate = "<" + sorted_sources[source_type][source][
                                                            triples_map].predicate_object_maps_list[
                                                            0].predicate_map.value + ">"
                                                    else:
                                                        predicate = None
                                                else:
                                                    predicate = None
                                                if data == []:
                                                    if config[dataset_i]["db"].lower() != "none":
                                                        if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                            for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                                repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                                if repeat_output == "":
                                                                    temp_generated = g_triples
                                                                    g_triples = {}    
                                                                    current_logical_dump = dump_output
                                                                    with open(dump_output, "w") as logical_output_descriptor:
                                                                        for row in cursor:
                                                                            number_triple += executor.submit(semantify_mysql, row,
                                                                                                     row_headers,
                                                                                                     sorted_sources[
                                                                                                         source_type][source][
                                                                                                         triples_map],
                                                                                                     triples_map_list,
                                                                                                     logical_output_descriptor,
                                                                                                     config[dataset_i]["host"],
                                                                                                     int(config[dataset_i][
                                                                                                             "port"]),
                                                                                                     config[dataset_i]["user"],
                                                                                                     config[dataset_i][
                                                                                                         "password"],
                                                                                                     config[dataset_i]["db"],
                                                                                                     predicate).result()
                                                                    current_logical_dump = ""
                                                                    cursor.execute(source)
                                                                    g_triples = temp_generated
                                                                    temp_generated = {}
                                                                    if "jsonld" in dump_output:
                                                                        context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        jsonld_data = g.serialize(format="json-ld", context=context)
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(jsonld_data)
                                                                    elif "n3" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        n3_data = g.serialize(format="n3")
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(n3_data)
                                                                    elif "rdfjson" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        json_data = generate_rdfjson(g)
                                                                        with open(dump_output, "w") as f:
                                                                            json.dump(json_data,f)
                                                                    elif "rdfxml" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        xml_data = g.serialize(format="xml")
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(xml_data)
                                                                    elif "ttl" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        ttl_data = g.serialize(format="ttl")
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(ttl_data)
                                                                    elif "tar.gz" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                        with tarfile.open(dump_output, "w:gz") as tar:
                                                                            tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                    elif "tar.xz" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                        with tarfile.open(dump_output, "w:xz") as tar:
                                                                            tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                    elif ".gz" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                        with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                            with gzip.open(dump_output, 'wb') as f_out:
                                                                                f_out.writelines(f_in)
                                                                    elif ".zip" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                        zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                        zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                        zip.close()
                                                                else:
                                                                    os.system("cp " + repeat_output + " " + dump_output)
                                                        for row in cursor:
                                                            number_triple += executor.submit(semantify_mysql, row,
                                                                                             row_headers,
                                                                                             sorted_sources[
                                                                                                 source_type][source][
                                                                                                 triples_map],
                                                                                             triples_map_list,
                                                                                             output_file_descriptor,
                                                                                             config[dataset_i]["host"],
                                                                                             int(config[dataset_i][
                                                                                                     "port"]),
                                                                                             config[dataset_i]["user"],
                                                                                             config[dataset_i][
                                                                                                 "password"],
                                                                                             config[dataset_i]["db"],
                                                                                             predicate).result()
                                                            data.append(row)
                                                    else:
                                                        if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                            for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                                repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                                if repeat_output == "":
                                                                    temp_generated = g_triples
                                                                    g_triples = {}    
                                                                    current_logical_dump = dump_output
                                                                    with open(dump_output, "w") as logical_output_descriptor:
                                                                        for row in cursor:
                                                                            number_triple += executor.submit(semantify_mysql, row,
                                                                                                     row_headers,
                                                                                                     sorted_sources[
                                                                                                         source_type][source][
                                                                                                         triples_map],
                                                                                                     triples_map_list,
                                                                                                     logical_output_descriptor,
                                                                                                     config[dataset_i]["host"],
                                                                                                     int(config[dataset_i][
                                                                                                             "port"]),
                                                                                                     config[dataset_i]["user"],
                                                                                                     config[dataset_i][
                                                                                                         "password"], "None",
                                                                                                     predicate).result()
                                                                    current_logical_dump = ""
                                                                    cursor.execute(source)
                                                                    g_triples = temp_generated
                                                                    temp_generated = {}
                                                                    if "jsonld" in dump_output:
                                                                        context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        jsonld_data = g.serialize(format="json-ld", context=context)
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(jsonld_data)
                                                                    elif "n3" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        n3_data = g.serialize(format="n3")
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(n3_data)
                                                                    elif "rdfjson" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        json_data = generate_rdfjson(g)
                                                                        with open(dump_output, "w") as f:
                                                                            json.dump(json_data,f)
                                                                    elif "rdfxml" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        xml_data = g.serialize(format="xml")
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(xml_data)
                                                                    elif "ttl" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        ttl_data = g.serialize(format="ttl")
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(ttl_data)
                                                                    elif "tar.gz" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                        with tarfile.open(dump_output, "w:gz") as tar:
                                                                            tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                    elif "tar.xz" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                        with tarfile.open(dump_output, "w:xz") as tar:
                                                                            tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                    elif ".gz" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                        with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                            with gzip.open(dump_output, 'wb') as f_out:
                                                                                f_out.writelines(f_in)
                                                                    elif ".zip" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                        zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                        zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                        zip.close()
                                                                else:
                                                                    os.system("cp " + repeat_output + " " + dump_output)
                                                        for row in cursor:
                                                            number_triple += executor.submit(semantify_mysql, row,
                                                                                             row_headers,
                                                                                             sorted_sources[
                                                                                                 source_type][source][
                                                                                                 triples_map],
                                                                                             triples_map_list,
                                                                                             output_file_descriptor,
                                                                                             config[dataset_i]["host"],
                                                                                             int(config[dataset_i][
                                                                                                     "port"]),
                                                                                             config[dataset_i]["user"],
                                                                                             config[dataset_i][
                                                                                                 "password"], "None",
                                                                                             predicate).result()
                                                            data.append(row)
                                                else:
                                                    if config[dataset_i]["db"].lower() != "none":
                                                        if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                            for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                                with open(dump_output, "w") as logical_output_descriptor:
                                                                    repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                                    if repeat_output == "":
                                                                        temp_generated = g_triples
                                                                        g_triples = {}
                                                                        current_logical_dump = dump_output
                                                                        for row in data:
                                                                            number_triple += executor.submit(semantify_mysql, row,
                                                                                                     row_headers,
                                                                                                     sorted_sources[
                                                                                                         source_type][source][
                                                                                                         triples_map],
                                                                                                     triples_map_list,
                                                                                                     logical_output_descriptor,
                                                                                                     config[dataset_i]["host"],
                                                                                                     int(config[dataset_i][
                                                                                                             "port"]),
                                                                                                     config[dataset_i]["user"],
                                                                                                     config[dataset_i][
                                                                                                         "password"],
                                                                                                     config[dataset_i]["db"],
                                                                                                     predicate).result()
                                                                        current_logical_dump = ""
                                                                        g_triples = temp_generated
                                                                        temp_generated = {}
                                                                        if "jsonld" in dump_output:
                                                                            context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            jsonld_data = g.serialize(format="json-ld", context=context)
                                                                            with open(dump_output, "w") as f:
                                                                                f.write(jsonld_data)
                                                                        elif "n3" in dump_output:
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            n3_data = g.serialize(format="n3")
                                                                            with open(dump_output, "w") as f:
                                                                                f.write(n3_data)
                                                                        elif "rdfjson" in dump_output:
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            json_data = generate_rdfjson(g)
                                                                            with open(dump_output, "w") as f:
                                                                                json.dump(json_data,f)
                                                                        elif "rdfxml" in dump_output:
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            xml_data = g.serialize(format="xml")
                                                                            with open(dump_output, "w") as f:
                                                                                f.write(xml_data)
                                                                        elif "ttl" in dump_output:
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            ttl_data = g.serialize(format="ttl")
                                                                            with open(dump_output, "w") as f:
                                                                                f.write(ttl_data)
                                                                        elif "tar.gz" in dump_output:
                                                                            os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                            with tarfile.open(dump_output, "w:gz") as tar:
                                                                                tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                        elif "tar.xz" in dump_output:
                                                                            os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                            with tarfile.open(dump_output, "w:xz") as tar:
                                                                                tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                        elif ".gz" in dump_output:
                                                                            os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                            with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                                with gzip.open(dump_output, 'wb') as f_out:
                                                                                    f_out.writelines(f_in)
                                                                        elif ".zip" in dump_output:
                                                                            os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                            zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                            zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                            zip.close()
                                                                    else:
                                                                        os.system("cp " + repeat_output + " " + dump_output)
                                                        for row in data:
                                                            number_triple += executor.submit(semantify_mysql, row,
                                                                                             row_headers,
                                                                                             sorted_sources[
                                                                                                 source_type][source][
                                                                                                 triples_map],
                                                                                             triples_map_list,
                                                                                             output_file_descriptor,
                                                                                             config[dataset_i]["host"],
                                                                                             int(config[dataset_i][
                                                                                                     "port"]),
                                                                                             config[dataset_i]["user"],
                                                                                             config[dataset_i][
                                                                                                 "password"],
                                                                                             config[dataset_i]["db"],
                                                                                             predicate).result()
                                                    else:
                                                        if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                            for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                                repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                                if repeat_output == "":
                                                                    temp_generated = g_triples
                                                                    g_triples = {}    
                                                                    with open(dump_output, "w") as logical_output_descriptor:
                                                                        current_logical_dump = dump_output
                                                                        for row in data:
                                                                            number_triple += executor.submit(semantify_mysql, row,
                                                                                                     row_headers,
                                                                                                     sorted_sources[
                                                                                                         source_type][source][
                                                                                                         triples_map],
                                                                                                     triples_map_list,
                                                                                                     logical_output_descriptor,
                                                                                                     config[dataset_i]["host"],
                                                                                                     int(config[dataset_i][
                                                                                                             "port"]),
                                                                                                     config[dataset_i]["user"],
                                                                                                     config[dataset_i][
                                                                                                         "password"], "None",
                                                                                                     predicate).result()
                                                                        current_logical_dump = ""
                                                                    g_triples = temp_generated
                                                                    temp_generated = {}
                                                                    if "jsonld" in dump_output:
                                                                        context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        jsonld_data = g.serialize(format="json-ld", context=context)
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(jsonld_data)
                                                                    elif "n3" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        n3_data = g.serialize(format="n3")
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(n3_data)
                                                                    elif "rdfjson" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        json_data = generate_rdfjson(g)
                                                                        with open(dump_output, "w") as f:
                                                                            json.dump(json_data,f)
                                                                    elif "rdfxml" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        xml_data = g.serialize(format="xml")
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(xml_data)
                                                                    elif "ttl" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        ttl_data = g.serialize(format="ttl")
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(ttl_data)
                                                                    elif "tar.gz" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                        with tarfile.open(dump_output, "w:gz") as tar:
                                                                            tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                    elif "tar.xz" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                        with tarfile.open(dump_output, "w:xz") as tar:
                                                                            tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                    elif ".gz" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                        with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                            with gzip.open(dump_output, 'wb') as f_out:
                                                                                f_out.writelines(f_in)
                                                                    elif ".zip" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                        zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                        zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                        zip.close()
                                                                else:
                                                                    os.system("cp " + repeat_output + " " + dump_output)
                                                        for row in data:
                                                            number_triple += executor.submit(semantify_mysql, row,
                                                                                             row_headers,
                                                                                             sorted_sources[
                                                                                                 source_type][source][
                                                                                                 triples_map],
                                                                                             triples_map_list,
                                                                                             output_file_descriptor,
                                                                                             config[dataset_i]["host"],
                                                                                             int(config[dataset_i][
                                                                                                     "port"]),
                                                                                             config[dataset_i]["user"],
                                                                                             config[dataset_i][
                                                                                                 "password"], "None",
                                                                                             predicate).result()
                                                if duplicate == "yes":
                                                    predicate_list = release_PTT(
                                                        sorted_sources[source_type][source][triples_map],
                                                        predicate_list)
                                                if mapping_partitions == "yes":
                                                    generated_subjects = release_subjects(
                                                        sorted_sources[source_type][source][triples_map],
                                                        generated_subjects)
                                elif source_type == "postgres":
                                    for source in sorted_sources[source_type]:
                                        db = psycopg2.connect(host=config[dataset_i]["host"],
                                                              user=config[dataset_i]["user"],
                                                              password=config[dataset_i]["password"],
                                                              dbname=config[dataset_i]["db"])
                                        cursor = db.cursor()
                                        cursor.execute(source)
                                        row_headers = [x[0] for x in cursor.description]
                                        data = []
                                        for triples_map in sorted_sources[source_type][source]:
                                            if (len(sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list) > 0 and
                                                sorted_sources[source_type][source][
                                                    triples_map].predicate_object_maps_list[
                                                    0].predicate_map.value != "None") or \
                                                    sorted_sources[source_type][source][
                                                        triples_map].subject_map.rdf_class != [None]:
                                                logger.info("TM: " + sorted_sources[source_type][source][
                                                    triples_map].triples_map_name)
                                                if mapping_partitions == "yes":
                                                    if sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list[
                                                        0].predicate_map.mapping_type == "constant" or \
                                                            sorted_sources[source_type][source][
                                                                triples_map].predicate_object_maps_list[
                                                                0].predicate_map.mapping_type == "constant shortcut":
                                                        predicate = "<" + sorted_sources[source_type][source][
                                                            triples_map].predicate_object_maps_list[
                                                            0].predicate_map.value + ">"
                                                    else:
                                                        predicate = None
                                                else:
                                                    predicate = None
                                                if data == []:
                                                    if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                            for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                                repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                                if repeat_output == "":
                                                                    temp_generated = g_triples
                                                                    g_triples = {}
                                                                    current_logical_dump = dump_output
                                                                    with open(dump_output, "w") as logical_output_descriptor:
                                                                        for row in cursor:
                                                                            number_triple += executor.submit(semantify_postgres, row,
                                                                                                 row_headers,
                                                                                                 sorted_sources[source_type][
                                                                                                     source][triples_map],
                                                                                                 triples_map_list,
                                                                                                 logical_output_descriptor,
                                                                                                 config[dataset_i]["user"],
                                                                                                 config[dataset_i]["password"],
                                                                                                 config[dataset_i]["db"],
                                                                                                 config[dataset_i]["host"],
                                                                                                 predicate).result()
                                                                    cursor.execute(source)
                                                                    current_logical_dump = ""
                                                                    g_triples = temp_generated
                                                                    temp_generated = {}
                                                                    if "jsonld" in dump_output:
                                                                        context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        jsonld_data = g.serialize(format="json-ld", context=context)
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(jsonld_data)
                                                                    elif "n3" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        n3_data = g.serialize(format="n3")
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(n3_data)
                                                                    elif "rdfjson" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        json_data = generate_rdfjson(g)
                                                                        with open(dump_output, "w") as f:
                                                                            json.dump(json_data,f)
                                                                    elif "rdfxml" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        xml_data = g.serialize(format="xml")
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(xml_data)
                                                                    elif "ttl" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        ttl_data = g.serialize(format="ttl")
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(ttl_data)
                                                                    elif "tar.gz" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                        with tarfile.open(dump_output, "w:gz") as tar:
                                                                            tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                    elif "tar.xz" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                        with tarfile.open(dump_output, "w:xz") as tar:
                                                                            tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                    elif ".gz" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                        with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                            with gzip.open(dump_output, 'wb') as f_out:
                                                                                f_out.writelines(f_in)
                                                                    elif ".zip" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                        zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                        zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                        zip.close()
                                                                else:
                                                                    os.system("cp " + repeat_output + " " + dump_output)
                                                    for row in cursor:
                                                        number_triple += executor.submit(semantify_postgres, row,
                                                                                         row_headers,
                                                                                         sorted_sources[source_type][
                                                                                             source][triples_map],
                                                                                         triples_map_list,
                                                                                         output_file_descriptor,
                                                                                         config[dataset_i]["user"],
                                                                                         config[dataset_i]["password"],
                                                                                         config[dataset_i]["db"],
                                                                                         config[dataset_i]["host"],
                                                                                         predicate).result()
                                                        data.append(row)
                                                else:
                                                    if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                            for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                                repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                                if repeat_output == "":
                                                                    temp_generated = g_triples
                                                                    g_triples = {}
                                                                    with open(dump_output, "w") as logical_output_descriptor:
                                                                        current_logical_dump = dump_output
                                                                        for row in data:
                                                                            number_triple += executor.submit(semantify_postgres, row,
                                                                                                             row_headers,
                                                                                                             sorted_sources[source_type][
                                                                                                                 source][triples_map],
                                                                                                             triples_map_list,
                                                                                                             logical_output_descriptor,
                                                                                                             config[dataset_i]["user"],
                                                                                                             config[dataset_i]["password"],
                                                                                                             config[dataset_i]["db"],
                                                                                                             config[dataset_i]["host"],
                                                                                                             predicate).result()
                                                                        current_logical_dump = ""
                                                                    g_triples = temp_generated
                                                                    temp_generated = {}
                                                                    if "jsonld" in dump_output:
                                                                        context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        jsonld_data = g.serialize(format="json-ld", context=context)
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(jsonld_data)
                                                                    elif "n3" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        n3_data = g.serialize(format="n3")
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(n3_data)
                                                                    elif "rdfjson" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        json_data = generate_rdfjson(g)
                                                                        with open(dump_output, "w") as f:
                                                                            json.dump(json_data,f)
                                                                    elif "rdfxml" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        xml_data = g.serialize(format="xml")
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(xml_data)
                                                                    elif "ttl" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        ttl_data = g.serialize(format="ttl")
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(ttl_data)
                                                                    elif "tar.gz" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                        with tarfile.open(dump_output, "w:gz") as tar:
                                                                            tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                    elif "tar.xz" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                        with tarfile.open(dump_output, "w:xz") as tar:
                                                                            tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                    elif ".gz" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                        with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                            with gzip.open(dump_output, 'wb') as f_out:
                                                                                f_out.writelines(f_in)
                                                                    elif ".zip" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                        zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                        zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                        zip.close()
                                                                else:
                                                                    os.system("cp " + repeat_output + " " + dump_output)
                                                    for row in data:
                                                        number_triple += executor.submit(semantify_postgres, row,
                                                                                         row_headers,
                                                                                         sorted_sources[source_type][
                                                                                             source][triples_map],
                                                                                         triples_map_list,
                                                                                         output_file_descriptor,
                                                                                         config[dataset_i]["user"],
                                                                                         config[dataset_i]["password"],
                                                                                         config[dataset_i]["db"],
                                                                                         config[dataset_i]["host"],
                                                                                         predicate).result()
                                                if duplicate == "yes":
                                                    predicate_list = release_PTT(
                                                        sorted_sources[source_type][source][triples_map],
                                                        predicate_list)
                                                if mapping_partitions == "yes":
                                                    generated_subjects = release_subjects(
                                                        sorted_sources[source_type][source][triples_map],
                                                        generated_subjects)
                                else:
                                    logger.error("Invalid reference formulation or format. Aborting...")
                                    sys.exit(1)
                logger.info("Successfully semantified {}.\n\n".format(config[dataset_i]["name"]))
    else:
        if "turtle" == output_format.lower():
            output_file = config["datasets"]["output_folder"] + "/" + config["datasets"]["name"] + ".ttl"
        else:
            output_file = config["datasets"]["output_folder"] + "/" + config["datasets"]["name"] + ".nt"

        with ThreadPoolExecutor(max_workers=10) as executor:
            with open(output_file, "w") as output_file_descriptor:
                for dataset_number in range(int(config["datasets"]["number_of_datasets"])):
                    dataset_i = "dataset" + str(int(dataset_number) + 1)
                    if "host" in config[dataset_i]:
                        user = config[dataset_i]["user"]
                        password = config[dataset_i]["password"]
                        port = config[dataset_i]["port"]
                        host = config[dataset_i]["host"]
                        datab = config[dataset_i]["db"]
                    triples_map_list = mapping_parser(config[dataset_i]["mapping"])
                    base = extract_base(config[dataset_i]["mapping"])
                    if "turtle" == output_format.lower():
                        string_prefixes = prefix_extraction(config[dataset_i]["mapping"])
                        output_file_descriptor.write(string_prefixes)
                    logger.info("Semantifying {}...".format(config[dataset_i]["name"]))
                    sorted_sources, predicate_list, order_list = files_sort(triples_map_list,
                                                                            config["datasets"]["ordered"], config)
                    if sorted_sources:
                        if order_list:
                            for source_type in order_list:
                                if source_type == "csv":
                                    for source in order_list[source_type]:
                                        if ".nt" in source:
                                            g = rdflib.Graph()
                                            g.parse(source, format="nt")
                                            for triples_map in sorted_sources[source_type][source]:
                                                if (len(sorted_sources[source_type][source][
                                                            triples_map].predicate_object_maps_list) > 0 and
                                                    sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list[
                                                        0].predicate_map.value != "None") or \
                                                        sorted_sources[source_type][source][
                                                            triples_map].subject_map.rdf_class != [None]:
                                                    results = g.query(sorted_sources[source_type][source][triples_map].iterator)
                                                    data = []
                                                    for row in results:
                                                        result_dict = {}
                                                        keys = list(row.__dict__["labels"].keys())
                                                        i = 0
                                                        while i < len(row):
                                                            result_dict[str(keys[i])] = str(row[keys[i]])
                                                            i += 1
                                                        data.append(result_dict)
                                                    blank_message = True
                                                    if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                            for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                                repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                                if repeat_output == "":
                                                                    temp_generated = g_triples
                                                                    g_triples = {}
                                                                    with open(dump_output, "w") as logical_output_descriptor:
                                                                        current_logical_dump = dump_output
                                                                        number_triple += executor.submit(semantify_file,
                                                                                             sorted_sources[source_type][
                                                                                                 source][triples_map],
                                                                                             triples_map_list, ",",
                                                                                             logical_output_descriptor,
                                                                                             data, True).result()
                                                                        current_logical_dump = ""
                                                                    g_triples = temp_generated
                                                                    temp_generated = {}
                                                                    if "jsonld" in dump_output:
                                                                        context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        jsonld_data = g.serialize(format="json-ld", context=context)
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(jsonld_data)
                                                                    elif "n3" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        n3_data = g.serialize(format="n3")
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(n3_data)
                                                                    elif "rdfjson" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        json_data = generate_rdfjson(g)
                                                                        with open(dump_output, "w") as f:
                                                                            json.dump(json_data,f)
                                                                    elif "rdfxml" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        xml_data = g.serialize(format="xml")
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(xml_data)
                                                                    elif "ttl" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        ttl_data = g.serialize(format="ttl")
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(ttl_data)
                                                                    elif "tar.gz" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                        with tarfile.open(dump_output, "w:gz") as tar:
                                                                            tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                    elif "tar.xz" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                        with tarfile.open(dump_output, "w:xz") as tar:
                                                                            tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                    elif ".gz" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                        with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                            with gzip.open(dump_output, 'wb') as f_out:
                                                                                f_out.writelines(f_in)
                                                                    elif ".zip" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                        zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                        zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                        zip.close()
                                                                else:
                                                                    os.system("cp " + repeat_output + " " + dump_output)
                                                    number_triple += executor.submit(semantify_file,
                                                                                     sorted_sources[source_type][
                                                                                         source][triples_map],
                                                                                     triples_map_list, ",",
                                                                                     output_file_descriptor,
                                                                                     data, True).result()
                                                    if duplicate == "yes":
                                                        predicate_list = release_PTT(
                                                            sorted_sources[source_type][source][triples_map],
                                                            predicate_list)
                                                    if mapping_partitions == "yes":
                                                        generated_subjects = release_subjects(
                                                            sorted_sources[source_type][source][triples_map],
                                                            generated_subjects)
                                        elif "endpoint:" in source:
                                            for triples_map in sorted_sources[source_type][source]:
                                                if (len(sorted_sources[source_type][source][
                                                            triples_map].predicate_object_maps_list) > 0 and
                                                    sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list[
                                                        0].predicate_map.value != "None") or \
                                                        sorted_sources[source_type][source][
                                                            triples_map].subject_map.rdf_class != [None]:
                                                    sparql = SPARQLWrapper(source.replace("endpoint:",""))
                                                    sparql.setQuery(sorted_sources[source_type][source][triples_map].iterator)
                                                    sparql.setReturnFormat(JSON)
                                                    results = sparql.query().convert()
                                                    data = []
                                                    for result in results["results"]["bindings"]:
                                                        result_dict = {}
                                                        for key, value in result.items():
                                                            result_dict[key] = value["value"]
                                                        data.append(result_dict)
                                                    if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                            for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                                repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                                if repeat_output == "":
                                                                    temp_generated = g_triples
                                                                    g_triples = {}
                                                                    with open(dump_output, "w") as logical_output_descriptor:
                                                                        current_logical_dump = dump_output
                                                                        number_triple += executor.submit(semantify_file,
                                                                                             sorted_sources[source_type][
                                                                                                 source][triples_map],
                                                                                             triples_map_list, ",",
                                                                                             logical_output_descriptor,
                                                                                             data, True).result()
                                                                        current_logical_dump = ""
                                                                    g_triples = temp_generated
                                                                    temp_generated = {}
                                                                    if "jsonld" in dump_output:
                                                                        context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        jsonld_data = g.serialize(format="json-ld", context=context)
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(jsonld_data)
                                                                    elif "n3" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        n3_data = g.serialize(format="n3")
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(n3_data)
                                                                    elif "rdfjson" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        json_data = generate_rdfjson(g)
                                                                        with open(dump_output, "w") as f:
                                                                            json.dump(json_data,f)
                                                                    elif "rdfxml" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        xml_data = g.serialize(format="xml")
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(xml_data)
                                                                    elif "ttl" in dump_output:
                                                                        g = rdflib.Graph()
                                                                        g.parse(dump_output, format="nt")
                                                                        ttl_data = g.serialize(format="ttl")
                                                                        with open(dump_output, "w") as f:
                                                                            f.write(ttl_data)
                                                                    elif "tar.gz" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                        with tarfile.open(dump_output, "w:gz") as tar:
                                                                            tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                    elif "tar.xz" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                        with tarfile.open(dump_output, "w:xz") as tar:
                                                                            tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                    elif ".gz" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                        with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                            with gzip.open(dump_output, 'wb') as f_out:
                                                                                f_out.writelines(f_in)
                                                                    elif ".zip" in dump_output:
                                                                        os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                        zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                        zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                        zip.close()
                                                                else:
                                                                    os.system("cp " + repeat_output + " " + dump_output)
                                                    number_triple += executor.submit(semantify_file,
                                                                                     sorted_sources[source_type][
                                                                                         source][triples_map],
                                                                                     triples_map_list, ",",
                                                                                     output_file_descriptor,
                                                                                     data, True).result()
                                                    if duplicate == "yes":
                                                        predicate_list = release_PTT(
                                                            sorted_sources[source_type][source][triples_map],
                                                            predicate_list)
                                                    if mapping_partitions == "yes":
                                                        generated_subjects = release_subjects(
                                                            sorted_sources[source_type][source][triples_map],
                                                            generated_subjects)
                                        else:
                                            if enrichment == "yes":
                                                reader = pd.read_csv(source, encoding="latin-1")
                                                reader = reader.where(pd.notnull(reader), None)
                                                if duplicate == "yes":
                                                    reader = reader.drop_duplicates(keep='first')
                                                data = reader.to_dict(orient='records')
                                                for triples_map in sorted_sources[source_type][source]:
                                                    if "NonAssertedTriplesMap" not in sorted_sources[source_type][source][triples_map].mappings_type:
                                                        if (len(sorted_sources[source_type][source][
                                                                    triples_map].predicate_object_maps_list) > 0 and
                                                            sorted_sources[source_type][source][
                                                                triples_map].predicate_object_maps_list[
                                                                0].predicate_map.value != "None") or \
                                                                sorted_sources[source_type][source][
                                                                    triples_map].subject_map.rdf_class != [None]:
                                                            blank_message = True
                                                            if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                                for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                                    repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                                    if repeat_output == "":
                                                                        temp_generated = g_triples
                                                                        g_triples = {}
                                                                        with open(dump_output, "w") as logical_output_descriptor:
                                                                            current_logical_dump = dump_output
                                                                            number_triple += executor.submit(semantify_file,
                                                                                                     sorted_sources[source_type][
                                                                                                         source][triples_map],
                                                                                                     triples_map_list, ",",
                                                                                                     logical_output_descriptor,
                                                                                                     data, True).result()
                                                                            current_logical_dump = ""
                                                                        g_triples = temp_generated
                                                                        temp_generated = {}
                                                                        if "jsonld" in dump_output:
                                                                            context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            jsonld_data = g.serialize(format="json-ld", context=context)
                                                                            with open(dump_output, "w") as f:
                                                                                f.write(jsonld_data)
                                                                        elif "n3" in dump_output:
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            n3_data = g.serialize(format="n3")
                                                                            with open(dump_output, "w") as f:
                                                                                f.write(n3_data)
                                                                        elif "rdfjson" in dump_output:
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            json_data = generate_rdfjson(g)
                                                                            with open(dump_output, "w") as f:
                                                                                json.dump(json_data,f)
                                                                        elif "rdfxml" in dump_output:
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            xml_data = g.serialize(format="xml")
                                                                            with open(dump_output, "w") as f:
                                                                                f.write(xml_data)
                                                                        elif "ttl" in dump_output:
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            ttl_data = g.serialize(format="ttl")
                                                                            with open(dump_output, "w") as f:
                                                                                f.write(ttl_data)
                                                                        elif "tar.gz" in dump_output:
                                                                            os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                            with tarfile.open(dump_output, "w:gz") as tar:
                                                                                tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                        elif "tar.xz" in dump_output:
                                                                            os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                            with tarfile.open(dump_output, "w:xz") as tar:
                                                                                tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                        elif ".gz" in dump_output:
                                                                            os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                            with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                                with gzip.open(dump_output, 'wb') as f_out:
                                                                                    f_out.writelines(f_in)
                                                                        elif ".zip" in dump_output:
                                                                            os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                            zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                            zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                            zip.close()
                                                                    else:
                                                                        os.system("cp " + repeat_output + " " + dump_output)
                                                            number_triple += executor.submit(semantify_file,
                                                                                             sorted_sources[source_type][
                                                                                                 source][triples_map],
                                                                                             triples_map_list, ",",
                                                                                             output_file_descriptor,
                                                                                             data, True).result()
                                                            if duplicate == "yes":
                                                                predicate_list = release_PTT(
                                                                    sorted_sources[source_type][source][triples_map],
                                                                    predicate_list)
                                                            if mapping_partitions == "yes":
                                                                generated_subjects = release_subjects(
                                                                    sorted_sources[source_type][source][triples_map],
                                                                    generated_subjects)
                                            else:
                                                for triples_map in sorted_sources[source_type][source]:
                                                    if "NonAssertedTriplesMap" not in sorted_sources[source_type][source][triples_map].mappings_type:
                                                        if (len(sorted_sources[source_type][source][
                                                                    triples_map].predicate_object_maps_list) > 0 and
                                                            sorted_sources[source_type][source][
                                                                triples_map].predicate_object_maps_list[
                                                                0].predicate_map.value != "None") or \
                                                                sorted_sources[source_type][source][
                                                                    triples_map].subject_map.rdf_class != [None]:
                                                            blank_message = True
                                                            with open(source, "r", encoding="latin-1") as input_file_descriptor:
                                                                data = csv.DictReader(input_file_descriptor, delimiter=',')
                                                                if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                                    for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                                        repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                                        if repeat_output == "":
                                                                            temp_generated = g_triples
                                                                            g_triples = {}
                                                                            with open(dump_output, "w") as logical_output_descriptor:
                                                                                current_logical_dump = dump_output
                                                                                number_triple += executor.submit(semantify_file,
                                                                                                         sorted_sources[source_type][
                                                                                                             source][triples_map],
                                                                                                         triples_map_list, ",",
                                                                                                         logical_output_descriptor,
                                                                                                         data, True).result()
                                                                                current_logical_dump = ""
                                                                            g_triples = temp_generated
                                                                            temp_generated = {}
                                                                            if "jsonld" in dump_output:
                                                                                context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                                g = rdflib.Graph()
                                                                                g.parse(dump_output, format="nt")
                                                                                jsonld_data = g.serialize(format="json-ld", context=context)
                                                                                with open(dump_output, "w") as f:
                                                                                    f.write(jsonld_data)
                                                                            elif "n3" in dump_output:
                                                                                g = rdflib.Graph()
                                                                                g.parse(dump_output, format="nt")
                                                                                n3_data = g.serialize(format="n3")
                                                                                with open(dump_output, "w") as f:
                                                                                    f.write(n3_data)
                                                                            elif "rdfjson" in dump_output:
                                                                                g = rdflib.Graph()
                                                                                g.parse(dump_output, format="nt")
                                                                                json_data = generate_rdfjson(g)
                                                                                with open(dump_output, "w") as f:
                                                                                    json.dump(json_data,f)
                                                                            elif "rdfxml" in dump_output:
                                                                                g = rdflib.Graph()
                                                                                g.parse(dump_output, format="nt")
                                                                                xml_data = g.serialize(format="xml")
                                                                                with open(dump_output, "w") as f:
                                                                                    f.write(xml_data)
                                                                            elif "ttl" in dump_output:
                                                                                g = rdflib.Graph()
                                                                                g.parse(dump_output, format="nt")
                                                                                ttl_data = g.serialize(format="ttl")
                                                                                with open(dump_output, "w") as f:
                                                                                    f.write(ttl_data)
                                                                            elif "tar.gz" in dump_output:
                                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                                with tarfile.open(dump_output, "w:gz") as tar:
                                                                                    tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                            elif "tar.xz" in dump_output:
                                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                                with tarfile.open(dump_output, "w:xz") as tar:
                                                                                    tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                            elif ".gz" in dump_output:
                                                                                os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                                with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                                    with gzip.open(dump_output, 'wb') as f_out:
                                                                                        f_out.writelines(f_in)
                                                                            elif ".zip" in dump_output:
                                                                                os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                                zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                                zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                                zip.close()
                                                                        else:
                                                                            os.system("cp " + repeat_output + " " + dump_output)
                                                                number_triple += executor.submit(semantify_file,
                                                                                                 sorted_sources[source_type][
                                                                                                     source][triples_map],
                                                                                                 triples_map_list, ",",
                                                                                                 output_file_descriptor,
                                                                                                 data, True).result()
                                                                if duplicate == "yes":
                                                                    predicate_list = release_PTT(
                                                                        sorted_sources[source_type][source][triples_map],
                                                                        predicate_list)
                                                                if mapping_partitions == "yes":
                                                                    generated_subjects = release_subjects(
                                                                        sorted_sources[source_type][source][triples_map],
                                                                        generated_subjects)
                                elif source_type == "JSONPath":
                                    for source in order_list[source_type]:
                                        for triples_map in sorted_sources[source_type][source]:
                                            if (len(sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list) > 0 and
                                                sorted_sources[source_type][source][
                                                    triples_map].predicate_object_maps_list[
                                                    0].predicate_map.value != "None") or \
                                                    sorted_sources[source_type][source][
                                                        triples_map].subject_map.rdf_class != [None]:
                                                if "http" in sorted_sources[source_type][source][
                                                    triples_map].data_source:
                                                    file_source = sorted_sources[source_type][source][triples_map].data_source
                                                    if "#" in file_source:
                                                        file = file_source.split("#")[1]
                                                    else:
                                                        file = file_source.split("/")[len(file_source.split("/"))-1]
                                                    if "gz" in file_source or "zip" in file_source or "tar.xz" in file_source or "tar.gz" in file_source:
                                                        response = requests.get(file_source)
                                                        with open(file, "wb") as f:
                                                            f.write(response.content)
                                                        if "zip" in file_source:
                                                            with zipfile.ZipFile(file, 'r') as zip_ref:
                                                                zip_ref.extractall()
                                                            data = json.load(open(file.replace(".zip","")))
                                                        elif "tar.xz" in file_source or "tar.gz" in file_source:
                                                            with tarfile.open(file, "r") as tar:
                                                                tar.extractall()
                                                            if "tar.xz" in file_source:
                                                                data = json.load(open(file.replace(".tar.xz","")))
                                                            else:
                                                                data = json.load(open(file.replace(".tar.gz","")))
                                                        elif "gz" in file_source:
                                                            with open(file, "rb") as gz_file:
                                                                with open(file.replace(".gz",""), "wb") as txt_file:
                                                                    shutil.copyfileobj(gzip.GzipFile(fileobj=gz_file), txt_file)
                                                            data = json.load(open(file.replace(".gz","")))
                                                    else:
                                                        response = urlopen(file_source)
                                                        data = json.loads(response.read())
                                                else:
                                                    data = json.load(
                                                        sorted_sources[source_type][source][triples_map].data_source)
                                                blank_message = True
                                                if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                    for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                        repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                        if repeat_output == "":
                                                            temp_generated = g_triples
                                                            g_triples = {}
                                                            with open(dump_output, "w") as logical_output_descriptor:
                                                                current_logical_dump = dump_output
                                                                number_triple += executor.submit(semantify_json,
                                                                                         sorted_sources[source_type][source][
                                                                                             triples_map], triples_map_list,
                                                                                         ",", logical_output_descriptor, data,
                                                                                         sorted_sources[source_type][source][
                                                                                             triples_map].iterator).result()
                                                                current_logical_dump = ""
                                                            g_triples = temp_generated
                                                            temp_generated = {}
                                                            if "jsonld" in dump_output:
                                                                context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                jsonld_data = g.serialize(format="json-ld", context=context)
                                                                with open(dump_output, "w") as f:
                                                                    f.write(jsonld_data)
                                                            elif "n3" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                n3_data = g.serialize(format="n3")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(n3_data)
                                                            elif "rdfjson" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                json_data = generate_rdfjson(g)
                                                                with open(dump_output, "w") as f:
                                                                    json.dump(json_data,f)
                                                            elif "rdfxml" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                xml_data = g.serialize(format="xml")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(xml_data)
                                                            elif "ttl" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                ttl_data = g.serialize(format="ttl")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(ttl_data)
                                                            elif "tar.gz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                with tarfile.open(dump_output, "w:gz") as tar:
                                                                    tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                            elif "tar.xz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                with tarfile.open(dump_output, "w:xz") as tar:
                                                                    tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                            elif ".gz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                    with gzip.open(dump_output, 'wb') as f_out:
                                                                        f_out.writelines(f_in)
                                                            elif ".zip" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                zip.close()
                                                        else:
                                                            os.system("cp " + repeat_output + " " + dump_output)
                                                number_triple += executor.submit(semantify_json,
                                                                                 sorted_sources[source_type][source][
                                                                                     triples_map], triples_map_list,
                                                                                 ",", output_file_descriptor, data,
                                                                                 sorted_sources[source_type][source][
                                                                                     triples_map].iterator).result()
                                                if duplicate == "yes":
                                                    predicate_list = release_PTT(
                                                        sorted_sources[source_type][source][triples_map],
                                                        predicate_list)
                                                if mapping_partitions == "yes":
                                                    generated_subjects = release_subjects(
                                                        sorted_sources[source_type][source][triples_map],
                                                        generated_subjects)
                                elif source_type == "XPath":
                                    for source in order_list[source_type]:
                                        for triples_map in sorted_sources[source_type][source]:
                                            if (len(sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list) > 0 and
                                                sorted_sources[source_type][source][
                                                    triples_map].predicate_object_maps_list[
                                                    0].predicate_map.value != "None") or \
                                                    sorted_sources[source_type][source][
                                                        triples_map].subject_map.rdf_class != [None]:
                                                blank_message = True
                                                if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                    for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                        repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                        if repeat_output == "":
                                                            temp_generated = g_triples
                                                            g_triples = {}
                                                            with open(dump_output, "w") as logical_output_descriptor:
                                                                current_logical_dump = dump_output
                                                                number_triple += executor.submit(semantify_xml,
                                                                                         sorted_sources[source_type][source][
                                                                                             triples_map], triples_map_list,
                                                                                         logical_output_descriptor).result()
                                                                current_logical_dump = ""
                                                            g_triples = temp_generated
                                                            temp_generated = {}
                                                            if "jsonld" in dump_output:
                                                                context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                jsonld_data = g.serialize(format="json-ld", context=context)
                                                                with open(dump_output, "w") as f:
                                                                    f.write(jsonld_data)
                                                            elif "n3" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                n3_data = g.serialize(format="n3")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(n3_data)
                                                            elif "rdfjson" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                json_data = generate_rdfjson(g)
                                                                with open(dump_output, "w") as f:
                                                                    json.dump(json_data,f)
                                                            elif "rdfxml" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                xml_data = g.serialize(format="xml")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(xml_data)
                                                            elif "ttl" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                ttl_data = g.serialize(format="ttl")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(ttl_data)
                                                            elif "tar.gz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                with tarfile.open(dump_output, "w:gz") as tar:
                                                                    tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                            elif "tar.xz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                with tarfile.open(dump_output, "w:xz") as tar:
                                                                    tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                            elif ".gz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                    with gzip.open(dump_output, 'wb') as f_out:
                                                                        f_out.writelines(f_in)
                                                            elif ".zip" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                zip.close()
                                                        else:
                                                            os.system("cp " + repeat_output + " " + dump_output)
                                                number_triple += executor.submit(semantify_xml,
                                                                                 sorted_sources[source_type][source][
                                                                                     triples_map], triples_map_list,
                                                                                 output_file_descriptor).result()
                                                if duplicate == "yes":
                                                    predicate_list = release_PTT(
                                                        sorted_sources[source_type][source][triples_map],
                                                        predicate_list)
                                                if mapping_partitions == "yes":
                                                    generated_subjects = release_subjects(
                                                        sorted_sources[source_type][source][triples_map],
                                                        generated_subjects)
                        else:
                            for source_type in sorted_sources:
                                if source_type == "csv":
                                    for source in sorted_sources[source_type]:
                                        if ".nt" in source:
                                            g = rdflib.Graph()
                                            g.parse(source, format="nt")
                                            for triples_map in sorted_sources[source_type][source]:
                                                if (len(sorted_sources[source_type][source][
                                                            triples_map].predicate_object_maps_list) > 0 and
                                                    sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list[
                                                        0].predicate_map.value != "None") or \
                                                        sorted_sources[source_type][source][
                                                            triples_map].subject_map.rdf_class != [None]:
                                                    results = g.query(sorted_sources[source_type][source][triples_map].iterator)
                                                    data = []
                                                    for row in results:
                                                        result_dict = {}
                                                        keys = list(row.__dict__["labels"].keys())
                                                        i = 0
                                                        while i < len(row):
                                                            result_dict[str(keys[i])] = str(row[keys[i]])
                                                            i += 1
                                                        data.append(result_dict)
                                                    blank_message = True
                                                    if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                        for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                            repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                            if repeat_output == "":
                                                                temp_generated = g_triples
                                                                g_triples = {}
                                                                with open(dump_output, "w") as logical_output_descriptor:
                                                                    current_logical_dump = dump_output
                                                                    number_triple += executor.submit(semantify_file,
                                                                                             sorted_sources[source_type][
                                                                                                 source][triples_map],
                                                                                             triples_map_list, ",",
                                                                                             logical_output_descriptor,
                                                                                             data, True).result()
                                                                    current_logical_dump = ""
                                                                g_triples = temp_generated
                                                                temp_generated = {}
                                                                if "jsonld" in dump_output:
                                                                    context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    jsonld_data = g.serialize(format="json-ld", context=context)
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(jsonld_data)
                                                                elif "n3" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    n3_data = g.serialize(format="n3")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(n3_data)
                                                                elif "rdfjson" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    json_data = generate_rdfjson(g)
                                                                    with open(dump_output, "w") as f:
                                                                        json.dump(json_data,f)
                                                                elif "rdfxml" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    xml_data = g.serialize(format="xml")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(xml_data)
                                                                elif "ttl" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    ttl_data = g.serialize(format="ttl")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(ttl_data)
                                                                elif "tar.gz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                    with tarfile.open(dump_output, "w:gz") as tar:
                                                                        tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                elif "tar.xz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                    with tarfile.open(dump_output, "w:xz") as tar:
                                                                        tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                elif ".gz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                    with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                        with gzip.open(dump_output, 'wb') as f_out:
                                                                            f_out.writelines(f_in)
                                                                elif ".zip" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                    zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                    zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                    zip.close()
                                                            else:
                                                                os.system("cp " + repeat_output + " " + dump_output)
                                                    number_triple += executor.submit(semantify_file,
                                                                                     sorted_sources[source_type][
                                                                                         source][triples_map],
                                                                                     triples_map_list, ",",
                                                                                     output_file_descriptor,
                                                                                     data, True).result()
                                                    if duplicate == "yes":
                                                        predicate_list = release_PTT(
                                                            sorted_sources[source_type][source][triples_map],
                                                            predicate_list)
                                                    if mapping_partitions == "yes":
                                                        generated_subjects = release_subjects(
                                                            sorted_sources[source_type][source][triples_map],
                                                            generated_subjects)
                                        elif "endpoint:" in source:
                                            for triples_map in sorted_sources[source_type][source]:
                                                if (len(sorted_sources[source_type][source][
                                                            triples_map].predicate_object_maps_list) > 0 and
                                                    sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list[
                                                        0].predicate_map.value != "None") or \
                                                        sorted_sources[source_type][source][
                                                            triples_map].subject_map.rdf_class != [None]:
                                                    sparql = SPARQLWrapper(source.replace("endpoint:",""))
                                                    sparql.setQuery(sorted_sources[source_type][source][triples_map].iterator)
                                                    sparql.setReturnFormat(JSON)
                                                    results = sparql.query().convert()
                                                    data = []
                                                    for result in results["results"]["bindings"]:
                                                        result_dict = {}
                                                        for key, value in result.items():
                                                            result_dict[key] = value["value"]
                                                        data.append(result_dict)
                                                    if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                        for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                            repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                            if repeat_output == "":
                                                                temp_generated = g_triples
                                                                g_triples = {}
                                                                with open(dump_output, "w") as logical_output_descriptor:
                                                                    current_logical_dump = dump_output
                                                                    number_triple += executor.submit(semantify_file,
                                                                                             sorted_sources[source_type][
                                                                                                 source][triples_map],
                                                                                             triples_map_list, ",",
                                                                                             logical_output_descriptor,
                                                                                             data, True).result()
                                                                    current_logical_dump = ""
                                                                g_triples = temp_generated
                                                                temp_generated = {}
                                                                if "jsonld" in dump_output:
                                                                    context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    jsonld_data = g.serialize(format="json-ld", context=context)
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(jsonld_data)
                                                                elif "n3" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    n3_data = g.serialize(format="n3")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(n3_data)
                                                                elif "rdfjson" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    json_data = generate_rdfjson(g)
                                                                    with open(dump_output, "w") as f:
                                                                        json.dump(json_data,f)
                                                                elif "rdfxml" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    xml_data = g.serialize(format="xml")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(xml_data)
                                                                elif "ttl" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    ttl_data = g.serialize(format="ttl")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(ttl_data)
                                                                elif "tar.gz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                    with tarfile.open(dump_output, "w:gz") as tar:
                                                                        tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                elif "tar.xz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                    with tarfile.open(dump_output, "w:xz") as tar:
                                                                        tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                elif ".gz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                    with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                        with gzip.open(dump_output, 'wb') as f_out:
                                                                            f_out.writelines(f_in)
                                                                elif ".zip" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                    zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                    zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                    zip.close()
                                                            else:
                                                                os.system("cp " + repeat_output + " " + dump_output)
                                                    number_triple += executor.submit(semantify_file,
                                                                                     sorted_sources[source_type][
                                                                                         source][triples_map],
                                                                                     triples_map_list, ",",
                                                                                     output_file_descriptor,
                                                                                     data, True).result()
                                                    if duplicate == "yes":
                                                        predicate_list = release_PTT(
                                                            sorted_sources[source_type][source][triples_map],
                                                            predicate_list)
                                                    if mapping_partitions == "yes":
                                                        generated_subjects = release_subjects(
                                                            sorted_sources[source_type][source][triples_map],
                                                            generated_subjects)
                                        else:
                                            if enrichment == "yes":
                                                reader = pd.read_csv(source, encoding="latin-1")
                                                reader = reader.where(pd.notnull(reader), None)
                                                if duplicate == "yes":
                                                    reader = reader.drop_duplicates(keep='first')
                                                data = reader.to_dict(orient='records')
                                                for triples_map in sorted_sources[source_type][source]:
                                                    if "NonAssertedTriplesMap" not in sorted_sources[source_type][source][triples_map].mappings_type:
                                                        if (len(sorted_sources[source_type][source][
                                                                    triples_map].predicate_object_maps_list) > 0 and
                                                            sorted_sources[source_type][source][
                                                                triples_map].predicate_object_maps_list[
                                                                0].predicate_map.value != "None") or \
                                                                sorted_sources[source_type][source][
                                                                    triples_map].subject_map.rdf_class != [None]:
                                                            blank_message = True
                                                            if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                                for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                                    repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                                    if repeat_output == "":
                                                                        temp_generated = g_triples
                                                                        g_triples = {}
                                                                        with open(dump_output, "w") as logical_output_descriptor:
                                                                            current_logical_dump = dump_output
                                                                            number_triple += executor.submit(semantify_file,
                                                                                                     sorted_sources[source_type][
                                                                                                         source][triples_map],
                                                                                                     triples_map_list, ",",
                                                                                                     logical_output_descriptor,
                                                                                                     data, True).result()
                                                                            current_logical_dump = ""
                                                                        g_triples = temp_generated
                                                                        temp_generated = {}
                                                                        if "jsonld" in dump_output:
                                                                            context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            jsonld_data = g.serialize(format="json-ld", context=context)
                                                                            with open(dump_output, "w") as f:
                                                                                f.write(jsonld_data)
                                                                        elif "n3" in dump_output:
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            n3_data = g.serialize(format="n3")
                                                                            with open(dump_output, "w") as f:
                                                                                f.write(n3_data)
                                                                        elif "rdfjson" in dump_output:
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            json_data = generate_rdfjson(g)
                                                                            with open(dump_output, "w") as f:
                                                                                json.dump(json_data,f)
                                                                        elif "rdfxml" in dump_output:
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            xml_data = g.serialize(format="xml")
                                                                            with open(dump_output, "w") as f:
                                                                                f.write(xml_data)
                                                                        elif "ttl" in dump_output:
                                                                            g = rdflib.Graph()
                                                                            g.parse(dump_output, format="nt")
                                                                            ttl_data = g.serialize(format="ttl")
                                                                            with open(dump_output, "w") as f:
                                                                                f.write(ttl_data)
                                                                        elif "tar.gz" in dump_output:
                                                                            os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                            with tarfile.open(dump_output, "w:gz") as tar:
                                                                                tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                        elif "tar.xz" in dump_output:
                                                                            os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                            with tarfile.open(dump_output, "w:xz") as tar:
                                                                                tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                        elif ".gz" in dump_output:
                                                                            os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                            with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                                with gzip.open(dump_output, 'wb') as f_out:
                                                                                    f_out.writelines(f_in)
                                                                        elif ".zip" in dump_output:
                                                                            os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                            zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                            zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                            zip.close()
                                                                    else:
                                                                        os.system("cp " + repeat_output + " " + dump_output)
                                                            number_triple += executor.submit(semantify_file,
                                                                                             sorted_sources[source_type][
                                                                                                 source][triples_map],
                                                                                             triples_map_list, ",",
                                                                                             output_file_descriptor,
                                                                                             data, True).result()
                                                            if duplicate == "yes":
                                                                predicate_list = release_PTT(
                                                                    sorted_sources[source_type][source][triples_map],
                                                                    predicate_list)
                                                            if mapping_partitions == "yes":
                                                                generated_subjects = release_subjects(
                                                                    sorted_sources[source_type][source][triples_map],
                                                                    generated_subjects)
                                            else:
                                                with open(source, "r", encoding="latin-1") as input_file_descriptor:
                                                    data = csv.DictReader(input_file_descriptor, delimiter=',')
                                                    for triples_map in sorted_sources[source_type][source]:
                                                        if "NonAssertedTriplesMap" not in sorted_sources[source_type][source][triples_map].mappings_type:
                                                            if (len(sorted_sources[source_type][source][
                                                                        triples_map].predicate_object_maps_list) > 0 and
                                                                sorted_sources[source_type][source][
                                                                    triples_map].predicate_object_maps_list[
                                                                    0].predicate_map.value != "None") or \
                                                                    sorted_sources[source_type][source][
                                                                        triples_map].subject_map.rdf_class != [None]:
                                                                blank_message = True
                                                                if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                                    for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                                        repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                                        if repeat_output == "":
                                                                            temp_generated = g_triples
                                                                            g_triples = {}
                                                                            with open(dump_output, "w") as logical_output_descriptor:
                                                                                current_logical_dump = dump_output
                                                                                number_triple += executor.submit(semantify_file,
                                                                                                         sorted_sources[source_type][
                                                                                                             source][triples_map],
                                                                                                         triples_map_list, ",",
                                                                                                         logical_output_descriptor,
                                                                                                         data, True).result()
                                                                                current_logical_dump = ""
                                                                            g_triples = temp_generated
                                                                            temp_generated = {}
                                                                            if "jsonld" in dump_output:
                                                                                context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                                g = rdflib.Graph()
                                                                                g.parse(dump_output, format="nt")
                                                                                jsonld_data = g.serialize(format="json-ld", context=context)
                                                                                with open(dump_output, "w") as f:
                                                                                    f.write(jsonld_data)
                                                                            elif "n3" in dump_output:
                                                                                g = rdflib.Graph()
                                                                                g.parse(dump_output, format="nt")
                                                                                n3_data = g.serialize(format="n3")
                                                                                with open(dump_output, "w") as f:
                                                                                    f.write(n3_data)
                                                                            elif "rdfjson" in dump_output:
                                                                                g = rdflib.Graph()
                                                                                g.parse(dump_output, format="nt")
                                                                                json_data = generate_rdfjson(g)
                                                                                with open(dump_output, "w") as f:
                                                                                    json.dump(json_data,f)
                                                                            elif "rdfxml" in dump_output:
                                                                                g = rdflib.Graph()
                                                                                g.parse(dump_output, format="nt")
                                                                                xml_data = g.serialize(format="xml")
                                                                                with open(dump_output, "w") as f:
                                                                                    f.write(xml_data)
                                                                            elif "ttl" in dump_output:
                                                                                g = rdflib.Graph()
                                                                                g.parse(dump_output, format="nt")
                                                                                ttl_data = g.serialize(format="ttl")
                                                                                with open(dump_output, "w") as f:
                                                                                    f.write(ttl_data)
                                                                            elif "tar.gz" in dump_output:
                                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                                with tarfile.open(dump_output, "w:gz") as tar:
                                                                                    tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                            elif "tar.xz" in dump_output:
                                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                                with tarfile.open(dump_output, "w:xz") as tar:
                                                                                    tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                            elif ".gz" in dump_output:
                                                                                os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                                with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                                    with gzip.open(dump_output, 'wb') as f_out:
                                                                                        f_out.writelines(f_in)
                                                                            elif ".zip" in dump_output:
                                                                                os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                                zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                                zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                                zip.close()
                                                                        else:
                                                                            os.system("cp " + repeat_output + " " + dump_output)
                                                                number_triple += executor.submit(semantify_file,
                                                                                                 sorted_sources[source_type][
                                                                                                     source][triples_map],
                                                                                                 triples_map_list, ",",
                                                                                                 output_file_descriptor,
                                                                                                 data, True).result()
                                                                if duplicate == "yes":
                                                                    predicate_list = release_PTT(
                                                                        sorted_sources[source_type][source][triples_map],
                                                                        predicate_list)
                                                                if mapping_partitions == "yes":
                                                                    generated_subjects = release_subjects(
                                                                        sorted_sources[source_type][source][triples_map],
                                                                        generated_subjects)
                                elif source_type == "JSONPath":
                                    for source in sorted_sources[source_type]:
                                        for triples_map in sorted_sources[source_type][source]:
                                            if (len(sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list) > 0 and
                                                sorted_sources[source_type][source][
                                                    triples_map].predicate_object_maps_list[
                                                    0].predicate_map.value != "None") or \
                                                    sorted_sources[source_type][source][
                                                        triples_map].subject_map.rdf_class != [None]:
                                                if "http" in sorted_sources[source_type][source][
                                                    triples_map].data_source:
                                                    file_source = sorted_sources[source_type][source][triples_map].data_source
                                                    if "#" in file_source:
                                                        file = file_source.split("#")[1]
                                                    else:
                                                        file = file_source.split("/")[len(file_source.split("/"))-1]
                                                    if "gz" in file_source or "zip" in file_source or "tar.xz" in file_source or "tar.gz" in file_source:
                                                        response = requests.get(file_source)
                                                        with open(file, "wb") as f:
                                                            f.write(response.content)
                                                        if "zip" in file_source:
                                                            with zipfile.ZipFile(file, 'r') as zip_ref:
                                                                zip_ref.extractall()
                                                            data = json.load(open(file.replace(".zip","")))
                                                        elif "tar.xz" in file_source or "tar.gz" in file_source:
                                                            with tarfile.open(file, "r") as tar:
                                                                tar.extractall()
                                                            if "tar.xz" in file_source:
                                                                data = json.load(open(file.replace(".tar.xz","")))
                                                            else:
                                                                data = json.load(open(file.replace(".tar.gz","")))
                                                        elif "gz" in file_source:
                                                            with open(file, "rb") as gz_file:
                                                                with open(file.replace(".gz",""), "wb") as txt_file:
                                                                    shutil.copyfileobj(gzip.GzipFile(fileobj=gz_file), txt_file)
                                                            data = json.load(open(file.replace(".gz","")))
                                                    else:
                                                        response = urlopen(file_source)
                                                        data = json.loads(response.read())
                                                else:
                                                    data = json.load(open(
                                                        sorted_sources[source_type][source][triples_map].data_source))
                                                blank_message = True
                                                if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                    for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                        repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                        if repeat_output == "":
                                                            temp_generated = g_triples
                                                            g_triples = {}
                                                            with open(dump_output, "w") as logical_output_descriptor:
                                                                current_logical_dump = dump_output
                                                                number_triple += executor.submit(semantify_json,
                                                                                         sorted_sources[source_type][source][
                                                                                             triples_map], triples_map_list,
                                                                                         ",", logical_output_descriptor, data,
                                                                                         sorted_sources[source_type][source][
                                                                                             triples_map].iterator).result()
                                                                current_logical_dump = ""
                                                            g_triples = temp_generated
                                                            temp_generated = {}
                                                            if "jsonld" in dump_output:
                                                                context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                jsonld_data = g.serialize(format="json-ld", context=context)
                                                                with open(dump_output, "w") as f:
                                                                    f.write(jsonld_data)
                                                            elif "n3" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                n3_data = g.serialize(format="n3")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(n3_data)
                                                            elif "rdfjson" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                json_data = generate_rdfjson(g)
                                                                with open(dump_output, "w") as f:
                                                                    json.dump(json_data,f)
                                                            elif "rdfxml" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                xml_data = g.serialize(format="xml")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(xml_data)
                                                            elif "ttl" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                ttl_data = g.serialize(format="ttl")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(ttl_data)
                                                            elif "tar.gz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                with tarfile.open(dump_output, "w:gz") as tar:
                                                                    tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                            elif "tar.xz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                with tarfile.open(dump_output, "w:xz") as tar:
                                                                    tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                            elif ".gz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                    with gzip.open(dump_output, 'wb') as f_out:
                                                                        f_out.writelines(f_in)
                                                            elif ".zip" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                zip.close()
                                                        else:
                                                            os.system("cp " + repeat_output + " " + dump_output)
                                                number_triple += executor.submit(semantify_json,
                                                                                 sorted_sources[source_type][source][
                                                                                     triples_map], triples_map_list,
                                                                                 ",", output_file_descriptor, data,
                                                                                 sorted_sources[source_type][source][
                                                                                     triples_map].iterator).result()
                                                if duplicate == "yes":
                                                    predicate_list = release_PTT(
                                                        sorted_sources[source_type][source][triples_map],
                                                        predicate_list)
                                                if mapping_partitions == "yes":
                                                    generated_subjects = release_subjects(
                                                        sorted_sources[source_type][source][triples_map],
                                                        generated_subjects)
                                elif source_type == "XPath":
                                    for source in sorted_sources[source_type]:
                                        for triples_map in sorted_sources[source_type][source]:
                                            if (len(sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list) > 0 and
                                                sorted_sources[source_type][source][
                                                    triples_map].predicate_object_maps_list[
                                                    0].predicate_map.value != "None") or \
                                                    sorted_sources[source_type][source][
                                                        triples_map].subject_map.rdf_class != [None]:
                                                blank_message = True
                                                if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                    for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                        repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                        if repeat_output == "":
                                                            temp_generated = g_triples
                                                            g_triples = {}
                                                            with open(dump_output, "w") as logical_output_descriptor:
                                                                current_logical_dump = dump_output
                                                                number_triple += executor.submit(semantify_xml,
                                                                                         sorted_sources[source_type][source][
                                                                                             triples_map], triples_map_list,
                                                                                         logical_output_descriptor).result()
                                                                current_logical_dump = ""
                                                            g_triples = temp_generated
                                                            temp_generated = {}
                                                            if "jsonld" in dump_output:
                                                                context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                jsonld_data = g.serialize(format="json-ld", context=context)
                                                                with open(dump_output, "w") as f:
                                                                    f.write(jsonld_data)
                                                            elif "n3" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                n3_data = g.serialize(format="n3")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(n3_data)
                                                            elif "rdfjson" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                json_data = generate_rdfjson(g)
                                                                with open(dump_output, "w") as f:
                                                                    json.dump(json_data,f)
                                                            elif "rdfxml" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                xml_data = g.serialize(format="xml")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(xml_data)
                                                            elif "ttl" in dump_output:
                                                                g = rdflib.Graph()
                                                                g.parse(dump_output, format="nt")
                                                                ttl_data = g.serialize(format="ttl")
                                                                with open(dump_output, "w") as f:
                                                                    f.write(ttl_data)
                                                            elif "tar.gz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                with tarfile.open(dump_output, "w:gz") as tar:
                                                                    tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                            elif "tar.xz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                with tarfile.open(dump_output, "w:xz") as tar:
                                                                    tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                            elif ".gz" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                    with gzip.open(dump_output, 'wb') as f_out:
                                                                        f_out.writelines(f_in)
                                                            elif ".zip" in dump_output:
                                                                os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                zip.close()
                                                        else:
                                                            os.system("cp " + repeat_output + " " + dump_output)
                                                number_triple += executor.submit(semantify_xml,
                                                                                 sorted_sources[source_type][source][
                                                                                     triples_map], triples_map_list,
                                                                                 output_file_descriptor).result()
                                                if duplicate == "yes":
                                                    predicate_list = release_PTT(
                                                        sorted_sources[source_type][source][triples_map],
                                                        predicate_list)
                                                if mapping_partitions == "yes":
                                                    generated_subjects = release_subjects(
                                                        sorted_sources[source_type][source][triples_map],
                                                        generated_subjects)

                    if predicate_list:
                        for source_type in sorted_sources:
                            blank_message = True
                            if str(source_type).lower() != "csv" and source_type != "JSONPath" and source_type != "XPath":
                                if source_type == "mysql":
                                    for source in sorted_sources[source_type]:
                                        db = connector.connect(host=config[dataset_i]["host"],
                                                               port=int(config[dataset_i]["port"]),
                                                               user=config[dataset_i]["user"],
                                                               password=config[dataset_i]["password"])
                                        cursor = db.cursor(buffered=True)
                                        if config[dataset_i]["db"].lower() != "none":
                                            cursor.execute("use " + config[dataset_i]["db"])
                                        else:
                                            if database != "None":
                                                cursor.execute("use " + database)
                                        cursor.execute(source)
                                        row_headers = [x[0] for x in cursor.description]
                                        data = []
                                        for triples_map in sorted_sources[source_type][source]:
                                            if (len(sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list) > 0 and
                                                sorted_sources[source_type][source][
                                                    triples_map].predicate_object_maps_list[
                                                    0].predicate_map.value != "None") or \
                                                    sorted_sources[source_type][source][
                                                        triples_map].subject_map.rdf_class != [None]:
                                                logger.info("TM: " + sorted_sources[source_type][source][
                                                    triples_map].triples_map_id)
                                                if mapping_partitions == "yes":
                                                    if sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list[
                                                        0].predicate_map.mapping_type == "constant" or \
                                                            sorted_sources[source_type][source][
                                                                triples_map].predicate_object_maps_list[
                                                                0].predicate_map.mapping_type == "constant shortcut":
                                                        predicate = "<" + sorted_sources[source_type][source][
                                                            triples_map].predicate_object_maps_list[
                                                            0].predicate_map.value + ">"
                                                    else:
                                                        predicate = None
                                                else:
                                                    predicate = None
                                                if data == []:
                                                    if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                        for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                            current_logical_dump = dump_output
                                                            repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                            if repeat_output == "":
                                                                temp_generated = g_triples
                                                                g_triples = {}
                                                                with open(dump_output, "w") as logical_output_descriptor:
                                                                    for row in cursor:
                                                                        if config[dataset_i]["db"].lower() != "none":
                                                                            number_triple += executor.submit(semantify_mysql, row,
                                                                                                             row_headers,
                                                                                                             sorted_sources[
                                                                                                                 source_type][source][
                                                                                                                 triples_map],
                                                                                                             triples_map_list,
                                                                                                             logical_output_descriptor,
                                                                                                             config[dataset_i]["host"],
                                                                                                             int(config[dataset_i][
                                                                                                                     "port"]),
                                                                                                             config[dataset_i]["user"],
                                                                                                             config[dataset_i][
                                                                                                                 "password"],
                                                                                                             config[dataset_i]["db"],
                                                                                                             predicate).result()
                                                                        else:
                                                                            number_triple += executor.submit(semantify_mysql, row,
                                                                                                             row_headers,
                                                                                                             sorted_sources[
                                                                                                                 source_type][source][
                                                                                                                 triples_map],
                                                                                                             triples_map_list,
                                                                                                             logical_output_descriptor,
                                                                                                             config[dataset_i]["host"],
                                                                                                             int(config[dataset_i][
                                                                                                                     "port"]),
                                                                                                             config[dataset_i]["user"],
                                                                                                             config[dataset_i][
                                                                                                                 "password"], "None",
                                                                                                             predicate).result()
                                                                current_logical_dump = ""
                                                                cursor.execute(source)
                                                                g_triples = temp_generated
                                                                temp_generated = {}
                                                                if "jsonld" in dump_output:
                                                                    context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    jsonld_data = g.serialize(format="json-ld", context=context)
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(jsonld_data)
                                                                elif "n3" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    n3_data = g.serialize(format="n3")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(n3_data)
                                                                elif "rdfjson" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    json_data = generate_rdfjson(g)
                                                                    with open(dump_output, "w") as f:
                                                                        json.dump(json_data,f)
                                                                elif "rdfxml" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    xml_data = g.serialize(format="xml")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(xml_data)
                                                                elif "ttl" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    ttl_data = g.serialize(format="ttl")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(ttl_data)
                                                                elif "tar.gz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                    with tarfile.open(dump_output, "w:gz") as tar:
                                                                        tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                elif "tar.xz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                    with tarfile.open(dump_output, "w:xz") as tar:
                                                                        tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                elif ".gz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                    with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                        with gzip.open(dump_output, 'wb') as f_out:
                                                                            f_out.writelines(f_in)
                                                                elif ".zip" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                    zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                    zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                    zip.close()
                                                            else:
                                                                os.system("cp " + repeat_output + " " + dump_output)
                                                    for row in cursor:
                                                        if config[dataset_i]["db"].lower() != "none":
                                                            number_triple += executor.submit(semantify_mysql, row,
                                                                                             row_headers,
                                                                                             sorted_sources[
                                                                                                 source_type][source][
                                                                                                 triples_map],
                                                                                             triples_map_list,
                                                                                             output_file_descriptor,
                                                                                             config[dataset_i]["host"],
                                                                                             int(config[dataset_i][
                                                                                                     "port"]),
                                                                                             config[dataset_i]["user"],
                                                                                             config[dataset_i][
                                                                                                 "password"],
                                                                                             config[dataset_i]["db"],
                                                                                             predicate).result()
                                                        else:
                                                            number_triple += executor.submit(semantify_mysql, row,
                                                                                             row_headers,
                                                                                             sorted_sources[
                                                                                                 source_type][source][
                                                                                                 triples_map],
                                                                                             triples_map_list,
                                                                                             output_file_descriptor,
                                                                                             config[dataset_i]["host"],
                                                                                             int(config[dataset_i][
                                                                                                     "port"]),
                                                                                             config[dataset_i]["user"],
                                                                                             config[dataset_i][
                                                                                                 "password"], "None",
                                                                                             predicate).result()
                                                        data.append(row)
                                                else:
                                                    if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                        for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                            repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                            if repeat_output == "":
                                                                temp_generated = g_triples
                                                                g_triples = {}
                                                                with open(dump_output, "w") as logical_output_descriptor:
                                                                    current_logical_dump = dump_output
                                                                    for row in data:
                                                                        if config[dataset_i]["db"].lower() != "none":
                                                                            number_triple += executor.submit(semantify_mysql, row,
                                                                                                             row_headers,
                                                                                                             sorted_sources[
                                                                                                                 source_type][source][
                                                                                                                 triples_map],
                                                                                                             triples_map_list,
                                                                                                             logical_output_descriptor,
                                                                                                             config[dataset_i]["host"],
                                                                                                             int(config[dataset_i][
                                                                                                                     "port"]),
                                                                                                             config[dataset_i]["user"],
                                                                                                             config[dataset_i][
                                                                                                                 "password"],
                                                                                                             config[dataset_i]["db"],
                                                                                                             predicate).result()
                                                                        else:
                                                                            number_triple += executor.submit(semantify_mysql, row,
                                                                                                             row_headers,
                                                                                                             sorted_sources[
                                                                                                                 source_type][source][
                                                                                                                 triples_map],
                                                                                                             triples_map_list,
                                                                                                             logical_output_descriptor,
                                                                                                             config[dataset_i]["host"],
                                                                                                             int(config[dataset_i][
                                                                                                                     "port"]),
                                                                                                             config[dataset_i]["user"],
                                                                                                             config[dataset_i][
                                                                                                                 "password"], "None",
                                                                                                             predicate).result()
                                                                    current_logical_dump = ""
                                                                g_triples = temp_generated
                                                                temp_generated = {}
                                                                if "jsonld" in dump_output:
                                                                    context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    jsonld_data = g.serialize(format="json-ld", context=context)
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(jsonld_data)
                                                                elif "n3" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    n3_data = g.serialize(format="n3")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(n3_data)
                                                                elif "rdfjson" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    json_data = generate_rdfjson(g)
                                                                    with open(dump_output, "w") as f:
                                                                        json.dump(json_data,f)
                                                                elif "rdfxml" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    xml_data = g.serialize(format="xml")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(xml_data)
                                                                elif "ttl" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    ttl_data = g.serialize(format="ttl")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(ttl_data)
                                                                elif "tar.gz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                    with tarfile.open(dump_output, "w:gz") as tar:
                                                                        tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                elif "tar.xz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                    with tarfile.open(dump_output, "w:xz") as tar:
                                                                        tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                elif ".gz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                    with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                        with gzip.open(dump_output, 'wb') as f_out:
                                                                            f_out.writelines(f_in)
                                                                elif ".zip" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                    zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                    zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                    zip.close()
                                                            else:
                                                                os.system("cp " + repeat_output + " " + dump_output)
                                                    else:
                                                        for row in data:
                                                            if config[dataset_i]["db"].lower() != "none":
                                                                number_triple += executor.submit(semantify_mysql, row,
                                                                                                 row_headers,
                                                                                                 sorted_sources[
                                                                                                     source_type][source][
                                                                                                     triples_map],
                                                                                                 triples_map_list,
                                                                                                 output_file_descriptor,
                                                                                                 config[dataset_i]["host"],
                                                                                                 int(config[dataset_i][
                                                                                                         "port"]),
                                                                                                 config[dataset_i]["user"],
                                                                                                 config[dataset_i][
                                                                                                     "password"],
                                                                                                 config[dataset_i]["db"],
                                                                                                 predicate).result()
                                                            else:
                                                                number_triple += executor.submit(semantify_mysql, row,
                                                                                                 row_headers,
                                                                                                 sorted_sources[
                                                                                                     source_type][source][
                                                                                                     triples_map],
                                                                                                 triples_map_list,
                                                                                                 output_file_descriptor,
                                                                                                 config[dataset_i]["host"],
                                                                                                 int(config[dataset_i][
                                                                                                         "port"]),
                                                                                                 config[dataset_i]["user"],
                                                                                                 config[dataset_i][
                                                                                                     "password"], "None",
                                                                                                 predicate).result()
                                                if duplicate == "yes":
                                                    predicate_list = release_PTT(
                                                        sorted_sources[source_type][source][triples_map],
                                                        predicate_list)
                                                if mapping_partitions == "yes":
                                                    generated_subjects = release_subjects(
                                                        sorted_sources[source_type][source][triples_map],
                                                        generated_subjects)
                                        data = []
                                elif source_type == "postgres":
                                    for source in sorted_sources[source_type]:
                                        db = psycopg2.connect(host=config[dataset_i]["host"],
                                                              user=config[dataset_i]["user"],
                                                              password=config[dataset_i]["password"],
                                                              dbname=config[dataset_i]["db"])
                                        cursor = db.cursor()
                                        cursor.execute(source)
                                        row_headers = [x[0] for x in cursor.description]
                                        data = []
                                        for triples_map in sorted_sources[source_type][source]:
                                            if (len(sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list) > 0 and
                                                sorted_sources[source_type][source][
                                                    triples_map].predicate_object_maps_list[
                                                    0].predicate_map.value != "None") or \
                                                    sorted_sources[source_type][source][
                                                        triples_map].subject_map.rdf_class != [None]:
                                                logger.info("TM: " + sorted_sources[source_type][source][
                                                    triples_map].triples_map_id)
                                                if mapping_partitions == "yes":
                                                    if sorted_sources[source_type][source][
                                                        triples_map].predicate_object_maps_list[
                                                        0].predicate_map.mapping_type == "constant" or \
                                                            sorted_sources[source_type][source][
                                                                triples_map].predicate_object_maps_list[
                                                                0].predicate_map.mapping_type == "constant shortcut":
                                                        predicate = "<" + sorted_sources[source_type][source][
                                                            triples_map].predicate_object_maps_list[
                                                            0].predicate_map.value + ">"
                                                    else:
                                                        predicate = None
                                                else:
                                                    predicate = None
                                                if data == []:
                                                    if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                        for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                            repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                            if repeat_output == "":
                                                                temp_generated = g_triples
                                                                g_triples = {}
                                                                current_logical_dump = dump_output
                                                                with open(dump_output, "w") as logical_output_descriptor:
                                                                    for row in cursor:
                                                                        number_triple += executor.submit(semantify_postgres, row,
                                                                                                         row_headers,
                                                                                                         sorted_sources[source_type][
                                                                                                             source][triples_map],
                                                                                                         triples_map_list,
                                                                                                         logical_output_descriptor,
                                                                                                         config[dataset_i]["user"],
                                                                                                         config[dataset_i]["password"],
                                                                                                         config[dataset_i]["db"],
                                                                                                         config[dataset_i]["host"],
                                                                                                         predicate).result()
                                                                current_logical_dump = ""
                                                                cursor.execute(source)
                                                                g_triples = temp_generated
                                                                temp_generated = {}
                                                                if "jsonld" in dump_output:
                                                                    context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    jsonld_data = g.serialize(format="json-ld", context=context)
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(jsonld_data)
                                                                elif "n3" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    n3_data = g.serialize(format="n3")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(n3_data)
                                                                elif "rdfjson" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    json_data = generate_rdfjson(g)
                                                                    with open(dump_output, "w") as f:
                                                                        json.dump(json_data,f)
                                                                elif "rdfxml" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    xml_data = g.serialize(format="xml")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(xml_data)
                                                                elif "ttl" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    ttl_data = g.serialize(format="ttl")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(ttl_data)
                                                                elif "tar.gz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                    with tarfile.open(dump_output, "w:gz") as tar:
                                                                        tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                elif "tar.xz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                    with tarfile.open(dump_output, "w:xz") as tar:
                                                                        tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                elif ".gz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                    with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                        with gzip.open(dump_output, 'wb') as f_out:
                                                                            f_out.writelines(f_in)
                                                                elif ".zip" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                    zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                    zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                    zip.close()
                                                            else:
                                                                os.system("cp " + repeat_output + " " + dump_output)
                                                    for row in cursor:
                                                        number_triple += executor.submit(semantify_postgres, row,
                                                                                         row_headers,
                                                                                         sorted_sources[source_type][
                                                                                             source][triples_map],
                                                                                         triples_map_list,
                                                                                         output_file_descriptor,
                                                                                         config[dataset_i]["user"],
                                                                                         config[dataset_i]["password"],
                                                                                         config[dataset_i]["db"],
                                                                                         config[dataset_i]["host"],
                                                                                         predicate).result()
                                                        data.append(row)
                                                else:
                                                    if sorted_sources[source_type][source][triples_map].triples_map_id in logical_dump:
                                                        for dump_output in logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id]:
                                                            repeat_output = is_repeat_output(dump_output,logical_dump[sorted_sources[source_type][source][triples_map].triples_map_id])
                                                            if repeat_output == "":
                                                                temp_generated = g_triples
                                                                g_triples = {}
                                                                current_logical_dump = dump_output
                                                                with open(dump_output, "w") as logical_output_descriptor:
                                                                    for row in data:
                                                                        number_triple += executor.submit(semantify_postgres, row,
                                                                                                         row_headers,
                                                                                                         sorted_sources[source_type][
                                                                                                             source][triples_map],
                                                                                                         triples_map_list,
                                                                                                         logical_output_descriptor,
                                                                                                         config[dataset_i]["user"],
                                                                                                         config[dataset_i]["password"],
                                                                                                         config[dataset_i]["db"],
                                                                                                         config[dataset_i]["host"],
                                                                                                         predicate).result()
                                                                current_logical_dump = ""
                                                                g_triples = temp_generated
                                                                temp_generated = {}
                                                                if "jsonld" in dump_output:
                                                                    context = extract_prefixes_from_ttl(config[dataset_i]["mapping"])
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    jsonld_data = g.serialize(format="json-ld", context=context)
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(jsonld_data)
                                                                elif "n3" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    n3_data = g.serialize(format="n3")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(n3_data)
                                                                elif "rdfjson" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    json_data = generate_rdfjson(g)
                                                                    with open(dump_output, "w") as f:
                                                                        json.dump(json_data,f)
                                                                elif "rdfxml" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    xml_data = g.serialize(format="xml")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(xml_data)
                                                                elif "ttl" in dump_output:
                                                                    g = rdflib.Graph()
                                                                    g.parse(dump_output, format="nt")
                                                                    ttl_data = g.serialize(format="ttl")
                                                                    with open(dump_output, "w") as f:
                                                                        f.write(ttl_data)
                                                                elif "tar.gz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".tar.gz",""))
                                                                    with tarfile.open(dump_output, "w:gz") as tar:
                                                                        tar.add(dump_output.replace(".tar.gz",""), arcname=dump_output.replace(".tar.gz",""))
                                                                elif "tar.xz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".tar.xz",""))
                                                                    with tarfile.open(dump_output, "w:xz") as tar:
                                                                        tar.add(dump_output.replace(".tar.xz",""), arcname=dump_output.replace(".tar.xz",""))
                                                                elif ".gz" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".gz",""))
                                                                    with open(dump_output.replace(".gz",""), 'rb') as f_in:
                                                                        with gzip.open(dump_output, 'wb') as f_out:
                                                                            f_out.writelines(f_in)
                                                                elif ".zip" in dump_output:
                                                                    os.system("mv " + dump_output + " " + dump_output.replace(".zip",""))
                                                                    zip = zipfile.ZipFile(dump_output, "w", zipfile.ZIP_DEFLATED)
                                                                    zip.write(dump_output.replace(".zip",""), os.path.basename(dump_output.replace(".zip","")))
                                                                    zip.close()
                                                            else:
                                                                os.system("cp " + repeat_output + " " + dump_output)
                                                    for row in data:
                                                        number_triple += executor.submit(semantify_postgres, row,
                                                                                         row_headers,
                                                                                         sorted_sources[source_type][
                                                                                             source][triples_map],
                                                                                         triples_map_list,
                                                                                         output_file_descriptor,
                                                                                         config[dataset_i]["user"],
                                                                                         config[dataset_i]["password"],
                                                                                         config[dataset_i]["db"],
                                                                                         config[dataset_i]["host"],
                                                                                         predicate).result()
                                                if duplicate == "yes":
                                                    predicate_list = release_PTT(
                                                        sorted_sources[source_type][source][triples_map],
                                                        predicate_list)
                                                if mapping_partitions == "yes":
                                                    generated_subjects = release_subjects(
                                                        sorted_sources[source_type][source][triples_map],
                                                        generated_subjects)
                                        data = []
                                else:
                                    logger.error("Invalid reference formulation or format. Aborting...")
                                    sys.exit(1)
                    logger.info("Successfully semantified {}.\n\n".format(config[dataset_i]["name"]))

    duration = time.time() - start_time

    logger.info("Successfully semantified all datasets in {:.3f} seconds.".format(duration))