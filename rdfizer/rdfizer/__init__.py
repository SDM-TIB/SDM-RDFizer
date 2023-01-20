import os
import re
import csv
import sys
import rdflib
from rdflib.plugins.sparql import prepareQuery
from configparser import ConfigParser, ExtendedInterpolation
import traceback
from mysql import connector
from concurrent.futures import ThreadPoolExecutor
import time
import json
import xml.etree.ElementTree as ET
import psycopg2
import pandas as pd
from .functions import *

try:
	from triples_map import TriplesMap as tm
except:
	from .triples_map import TriplesMap as tm	

import tracemalloc
# Work in the rr:sqlQuery (change mapping parser query, add sqlite3 support, etc)
# Work in the "when subject is empty" thing (uuid.uuid4(), dependency graph over the ) 

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
global user, password, port, host
user, password, port, host = "", "", "", ""
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
global dic_table
dic_table = {}
global base
base = ""
global blank_message
blank_message = True
global general_predicates
general_predicates = {"http://www.w3.org/2000/01/rdf-schema#subClassOf":"",
						"http://www.w3.org/2002/07/owl#sameAs":"",
						"http://www.w3.org/2000/01/rdf-schema#seeAlso":"",
						"http://www.w3.org/2000/01/rdf-schema#subPropertyOf":""}

def prefix_extraction(original):
	string_prefixes = ""
	f = open(original,"r")
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
		else:
			break
	string_prefixes += "\n"
	f.close()
	return string_prefixes

def determine_prefix(uri):
	url = ""
	value = ""
	if "#" in uri:
		url, value = uri.split("#")[0]+"#", uri.split("#")[1]
	else:
		value = uri.split("/")[len(uri.split("/"))-1]
		char = ""
		temp = ""
		temp_string = uri
		while char != "/":
			temp = temp_string
			temp_string = temp_string[:-1]
			char = temp[len(temp)-1]
		url = temp
	if  "<" in url:
		url = url[1:]
	if ">" in value:
		value = value[:-1]
	return prefixes[url] + ":" + value

def release_PTT(triples_map,predicate_list):
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
	if triples_map.subject_map.rdf_class != None:
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

def join_iterator(data, iterator, parent, child):
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
							if isinstance(row,list):
								for sub_row in row:
									join_iterator(sub_row, iterator, parent, child)
								executed = False
								break
							elif isinstance(row,str):
								row = []
								break
						else:
							join_iterator(row[list(row.keys())[0]], "", parent, child)
				else:
					path = jsonpath_find(temp_keys[len(temp_keys)-1],row,"",[])
					for key in path[0].split("."):
						if key in temp_keys:
							join_iterator(row[key], "", parent, child)
						elif key in row:
							row = row[key]
							if isinstance(row,list):
								for sub_row in row:
									join_iterator(sub_row, iterator, parent, child)
								executed = False
								break
							elif isinstance(row,dict):
								join_iterator(row, iterator, parent, child)
								executed = False
								break
							elif isinstance(row,str):
								row = []
								break 
			if new_iterator != ".":
				if "*" == new_iterator[-2]:
					for sub_row in row:
						join_iterator(sub_row, iterator.replace(new_iterator[:-1],""), parent, child)
					executed = False
					break
				if "[*][*]" in new_iterator:
					for sub_row in row:
						for sub_sub_row in row[sub_row]:
							join_iterator(sub_sub_row, iterator.replace(new_iterator[:-1],""), parent, child)
					executed = False
					break
				if isinstance(row,list):
					for sub_row in row:
						join_iterator(sub_row, iterator.replace(new_iterator[:-1],""), parent, child)
					executed = False
					break
	else:
		if parent.triples_map_id + "_" + child.child[0] not in join_table:
			hash_maker([data], parent, child)
		else:
			hash_update([data], parent, child, parent.triples_map_id + "_" + child.child[0])

def hash_update(parent_data, parent_subject, child_object,join_id):
	hash_table = {}
	for row in parent_data:
		if child_object.parent[0] in row.keys():
			if row[child_object.parent[0]] in hash_table:
				if duplicate == "yes":
					if parent_subject.subject_map.subject_mapping_type == "reference":
						value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
						if value != None:
							if "http" in value and "<" not in value:
								value = "<" + value[1:-1] + ">"
							elif "http" in value and "<" in value:
								value = value[1:-1] 
						if value not in hash_table[row[child_object.parent[0]]]:
							hash_table[row[child_object.parent[0]]].update({value : "object"})
					else:
						if string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) is not None:
							if "<" + string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) + ">" not in hash_table[row[child_object.parent[0]]]:
								hash_table[row[child_object.parent[0]]].update({"<" + string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) + ">" : "object"}) 
				else:
					if parent_subject.subject_map.subject_mapping_type == "reference":
						value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore)
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1] 
						hash_table[row[child_object.parent[0]]].update({value : "object"})
					else:
						if string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) is not None:
							hash_table[row[child_object.parent[0]]].update({"<" + string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) + ">" : "object"})

			else:
				if parent_subject.subject_map.subject_mapping_type == "reference":
					value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
					if value != None:
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1] 
					hash_table.update({row[child_object.parent[0]] : {value : "object"}}) 
				else:
					if string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) is not None:
						hash_table.update({row[child_object.parent[0]] : {"<" + string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) + ">" : "object"}})
	join_table[join_id].update(hash_table)

def hash_maker(parent_data, parent_subject, child_object):
	global blank_message
	hash_table = {}
	for row in parent_data:
		if child_object.parent[0] in row.keys():
			if row[child_object.parent[0]] in hash_table:
				if duplicate == "yes":
					if parent_subject.subject_map.subject_mapping_type == "reference":
						value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
						if value != None:
							if "http" in value and "<" not in value:
								value = "<" + value[1:-1] + ">"
							elif "http" in value and "<" in value:
								value = value[1:-1] 
						if value not in hash_table[row[child_object.parent[0]]]:
							hash_table[row[child_object.parent[0]]].update({value : "object"})
					else:
						if string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator) != None:
							value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator)
							if value != None:
								if parent_subject.subject_map.term_type != None:
									if "BlankNode" in parent_subject.subject_map.term_type:
										if "/" in value:
											value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
											if "." in value:
												value = value.replace(".","2E")
											if blank_message:
												print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
												blank_message = False
										else:
											value = "_:" + encode_char(value).replace("%","")
											if "." in value:
												value = value.replace(".","2E")
								else:
									value = "<" + value + ">"
								hash_table[row[child_object.parent[0]]].update({value : "object"})
				else:
					if parent_subject.subject_map.subject_mapping_type == "reference":
						value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1] 
						hash_table[row[child_object.parent[0]]].update({value : "object"})
					else:
						value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator)
						if value != None:
							if parent_subject.subject_map.term_type != None:
								if "BlankNode" in parent_subject.subject_map.term_type:
									if "/" in value:
										value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
										if "." in value:
											value = value.replace(".","2E")
										if blank_message:
											print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
											blank_message = False
									else:
										value = "_:" + encode_char(value).replace("%","")
										if "." in value:
											value = value.replace(".","2E")
							else:
								value = "<" + value + ">"
							hash_table[row[child_object.parent[0]]].update({value : "object"})

			else:
				if parent_subject.subject_map.subject_mapping_type == "reference":
					value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
					if value != None:
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1] 
					hash_table.update({row[child_object.parent[0]] : {value : "object"}}) 
				else:
					value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator)
					if value != None:
						if parent_subject.subject_map.term_type != None:
							if "BlankNode" in parent_subject.subject_map.term_type:
								if "/" in value:
									value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
									if "." in value:
										value = value.replace(".","2E")
									if blank_message:
										print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
										blank_message = False
								else:
									value = "_:" + encode_char(value).replace("%","")
									if "." in value:
										value = value.replace(".","2E")
						else:
							value = "<" + value + ">"
						hash_table.update({row[child_object.parent[0]] : {value : "object"}})
	join_table.update({parent_subject.triples_map_id + "_" + child_object.child[0] : hash_table})

def hash_maker_list(parent_data, parent_subject, child_object):
	hash_table = {}
	global blank_message
	for row in parent_data:
		if sublist(child_object.parent,row.keys()):
			if child_list_value(child_object.parent,row) in hash_table:
				if duplicate == "yes":
					if parent_subject.subject_map.subject_mapping_type == "reference":
						value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
						if value != None:
							if "http" in value and "<" not in value:
								value = "<" + value[1:-1] + ">"
							elif "http" in value and "<" in value:
								value = value[1:-1] 
						hash_table[child_list_value(child_object.parent,row)].update({value : "object"})
					else:
						value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator)
						if value != None:
							if parent_subject.subject_map.term_type != None:
								if "BlankNode" in parent_subject.subject_map.term_type:
									if "/" in value:
										value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
										if "." in value:
											value = value.replace(".","2E")
										if blank_message:
											print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
											blank_message = False
									else:
										value = "_:" + encode_char(value).replace("%","")
										if "." in value:
											value = value.replace(".","2E")
							else:
								value = "<" + value + ">"
							hash_table[child_list_value(child_object.parent,row)].update({value: "object"})


				else:
					if parent_subject.subject_map.subject_mapping_type == "reference":
						value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1] 
						hash_table[child_list_value(child_object.parent,row)].update({value : "object"})
					else:
						value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator)
						if value != None:
							if parent_subject.subject_map.term_type != None:
								if "BlankNode" in parent_subject.subject_map.term_type:
									if "/" in value:
										value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
										if "." in value:
											value = value.replace(".","2E")
										if blank_message:
											print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
											blank_message = False
									else:
										value = "_:" + encode_char(value).replace("%","")
										if "." in value:
											value = value.replace(".","2E")
							else:
								value = "<" + value + ">"
							hash_table[child_list_value(child_object.parent,row)].update({value: "object"})

			else:
				if parent_subject.subject_map.subject_mapping_type == "reference":
					value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore, parent_subject.iterator)
					if value != None:
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1] 
					hash_table.update({child_list_value(child_object.parent,row) : {value : "object"}}) 
				else:
					value = string_substitution(parent_subject.subject_map.value, "{(.+?)}", row, "object", ignore, parent_subject.iterator)
					if value != None:
						if parent_subject.subject_map.term_type != None:
							if "BlankNode" in parent_subject.subject_map.term_type:
								if "/" in value:
									value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
									if "." in value:
										value = value.replace(".","2E")
									if blank_message:
										print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
										blank_message = False
								else:
									value = "_:" + encode_char(value).replace("%","")
									if "." in value:
										value = value.replace(".","2E")
						else:
							value = "<" + value + ">"
						hash_table.update({child_list_value(child_object.parent,row) : {value : "object"}})
	join_table.update({parent_subject.triples_map_id + "_" + child_list(child_object.child) : hash_table})

def hash_maker_xml(parent_data, parent_subject, child_object, parent_map, namespace):
	hash_table = {}
	global blank_message
	for row in parent_data:
		if row.find(child_object.parent[0]).text in hash_table:
			if duplicate == "yes":
				if parent_subject.subject_map.subject_mapping_type == "reference":
					value = string_substitution_xml(parent_subject.subject_map.value, ".+", row, "object", parent_subject.iterator, parent_map, namespace)
					if value[0] != None:
						if "http" in value[0]:
							value[0] = "<" + value[0][1:-1] + ">"
					if value [0]not in hash_table[row.find(child_object.parent[0]).text]:
						hash_table[row.find(child_object.parent[0]).text].update({value[0] : "object"})
				else:
					value = string_substitution_xml(parent_subject.subject_map.value, "{(.+?)}", row, "subject", parent_subject.iterator, parent_map, namespace)
					if value != None:
						if parent_subject.subject_map.term_type != None:
							if "BlankNode" in parent_subject.subject_map.term_type:
								if "/" in value:
									value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
									if "." in value:
										value = value.replace(".","2E")
									if blank_message:
										print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
										blank_message = False
								else:
									value = "_:" + encode_char(value).replace("%","")
									if "." in value:
										value = value.replace(".","2E")
						else:
							value = "<" + value + ">"
						if value not in hash_table[row.find(child_object.parent[0]).text]:
							hash_table[row.find(child_object.parent[0]).text].update({value : "object"})
			else:
				if parent_subject.subject_map.subject_mapping_type == "reference":
					value = string_substitution_xml(parent_subject.subject_map.value, ".+", row, "object", parent_subject.iterator, parent_map, namespace)
					if value[0] != None:
						if "http" in value:
							value[0] = "<" + value[0][1:-1] + ">"
					hash_table[row.find(child_object.parent[0]).text].update({value[0] : "object"})
				else:
					value = string_substitution_xml(parent_subject.subject_map.value, "{(.+?)}", row, "subject", parent_subject.iterator, parent_map, namespace)
					if value != None:
						if parent_subject.subject_map.term_type != None:
							if "BlankNode" in parent_subject.subject_map.term_type:
								if "/" in value:
									value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
									if "." in value:
										value = value.replace(".","2E")
									if blank_message:
										print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
										blank_message = False
								else:
									value = "_:" + encode_char(value).replace("%","")
									if "." in value:
										value = value.replace(".","2E")
						else:
							value = "<" + value + ">"
						hash_table[row.find(child_object.parent[0]).text].update({value : "object"})

		else:
			if parent_subject.subject_map.subject_mapping_type == "reference":
				value = string_substitution_xml(parent_subject.subject_map.value, ".+", row, "object", parent_subject.iterator, parent_map, namespace)
				if value[0] != None:
					if "http" in value[0]:
						value[0] = "<" + value[0][1:-1] + ">"
				hash_table.update({row.find(child_object.parent[0]).text : {value[0] : "object"}}) 
			else:
				value = string_substitution_xml(parent_subject.subject_map.value, "{(.+?)}", row, "subject", parent_subject.iterator, parent_map, namespace)
				if value != None:
					if parent_subject.subject_map.term_type != None:
						if "BlankNode" in parent_subject.subject_map.term_type:
							if "/" in value:
								value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
								if "." in value:
									value = value.replace(".","2E")
								if blank_message:
									print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
									blank_message = False
							else:
								value = "_:" + encode_char(value).replace("%","")
								if "." in value:
									value = value.replace(".","2E")
					else:
						value = "<" + value + ">"	
					hash_table.update({row.find(child_object.parent[0]).text : {value : "object"}}) 
	join_table.update({parent_subject.triples_map_id + "_" + child_object.child[0] : hash_table})


def hash_maker_array(parent_data, parent_subject, child_object):
	hash_table = {}
	row_headers=[x[0] for x in parent_data.description]
	for row in parent_data:	
		element =row[row_headers.index(child_object.parent[0])]
		if type(element) is int:
			element = str(element)
		if row[row_headers.index(child_object.parent[0])] in hash_table:
			if duplicate == "yes":
				if "<" + string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers,"object",ignore) + ">" not in hash_table[row[row_headers.index(child_object.parent[0])]]:
					hash_table[element].update({"<" + string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers,"object",ignore) + ">" : "object"})
			else:
				hash_table[element].update({"<" + string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers, "object",ignore) + ">" : "object"})
			
		else:
			hash_table.update({element : {"<" + string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers, "object",ignore) + ">" : "object"}}) 
	join_table.update({parent_subject.triples_map_id + "_" + child_object.child[0]  : hash_table})

def hash_maker_array_list(parent_data, parent_subject, child_object, r_w):
	hash_table = {}
	global blank_message
	row_headers=[x[0] for x in parent_data.description]
	for row in parent_data:
		if child_list_value_array(child_object.parent,row,row_headers) in hash_table:
			if duplicate == "yes":
				if parent_subject.subject_map.subject_mapping_type == "reference":
					value = string_substitution_array(parent_subject.subject_map.value, ".+", row, row_headers,"object",ignore)
					if value != None:
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1] 
					if value not in hash_table[child_list_value_array(child_object.parent,row,row_headers)]:
						hash_table[child_list_value_array(child_object.parent,row,row_headers)].update({value + ">" : "object"})

				else:
					value = string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers,"object",ignore)
					if value != None:
						if parent_subject.subject_map.term_type != None:
							if "BlankNode" in parent_subject.subject_map.term_type:
								if "/" in value:
									value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
									if "." in value:
										value = value.replace(".","2E")
									if blank_message:
										print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
										blank_message = False
								else:
									value = "_:" + encode_char(value).replace("%","")
									if "." in value:
										value = value.replace(".","2E")
						else:
							value = "<" + value + ">"
						if value not in hash_table[child_list_value_array(child_object.parent,row,row_headers)]:
							hash_table[child_list_value_array(child_object.parent,row,row_headers)].update({value : "object"})
			else:
				if parent_subject.subject_map.subject_mapping_type == "reference":
					value = string_substitution_array(parent_subject.subject_map.value, ".+", row, row_headers,"object",ignore)
					if value != None:
						if "http" in value and "<" not in value:
							value = "<" + value[1:-1] + ">"
						elif "http" in value and "<" in value:
							value = value[1:-1] 
					hash_table[child_list_value_array(child_object.parent,row,row_headers)].update({value : "object"})
				else:
					value = string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers,"object",ignore)
					if value != None:
						if parent_subject.subject_map.term_type != None:
							if "BlankNode" in parent_subject.subject_map.term_type:
								if "/" in value:
									value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
									if blank_message:
										print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
										blank_message = False
								else:
									value = "_:" + encode_char(value).replace("%","")
								if "." in value:
									value = value.replace(".","2E")
						else:
							value = "<" + value + ">"
						hash_table[child_list_value_array(child_object.parent,row,row_headers)].update({value : "object"})
			
		else:
			if parent_subject.subject_map.subject_mapping_type == "reference":
				value = string_substitution_array(parent_subject.subject_map.value, ".+", row, row_headers,"object",ignore)
				if value != None:
					if "http" in value and "<" not in value:
						value = "<" + value[1:-1] + ">"
					elif "http" in value and "<" in value:
							value = value[1:-1]
				hash_table.update({child_list_value_array(child_object.parent,row,row_headers):{value : "object"}})
			else:
				value = string_substitution_array(parent_subject.subject_map.value, "{(.+?)}", row, row_headers,"object",ignore)
				if value != None:
					if parent_subject.subject_map.term_type != None:
						if "BlankNode" in parent_subject.subject_map.term_type:
							if "/" in value:
								value = "_:" + encode_char(value.replace("/","2F")).replace("%","")
								if blank_message:
									print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
									blank_message = False
							else:
								value = "_:" + encode_char(value).replace("%","")
							if "." in value:
								value = value.replace(".","2E")
					else:
						value = "<" + value + ">"
					hash_table.update({child_list_value_array(child_object.parent,row,row_headers) : {value : "object"}}) 
	join_table.update({parent_subject.triples_map_id + "_" + child_list(child_object.child)  : hash_table})

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
		print(n3_mapping_parse_exception)
		print('Could not parse {} as a mapping file'.format(mapping_file))
		print('Aborting...')
		sys.exit(1)

	mapping_query = """
		prefix rr: <http://www.w3.org/ns/r2rml#> 
		prefix rml: <http://semweb.mmlab.be/ns/rml#> 
		prefix ql: <http://semweb.mmlab.be/ns/ql#> 
		prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> 
		SELECT DISTINCT *
		WHERE {

	# Subject -------------------------------------------------------------------------
			?triples_map_id rml:logicalSource ?_source .
			OPTIONAL{?_source rml:source ?data_source .}
			OPTIONAL {?_source rml:referenceFormulation ?ref_form .}
			OPTIONAL { ?_source rml:iterator ?iterator . }
			OPTIONAL { ?_source rr:tableName ?tablename .}
			OPTIONAL { ?_source rml:query ?query .}

			?triples_map_id rr:subjectMap ?_subject_map .
			OPTIONAL {?_subject_map rr:template ?subject_template .}
			OPTIONAL {?_subject_map rml:reference ?subject_reference .}
			OPTIONAL {?_subject_map rr:constant ?subject_constant}
			OPTIONAL { ?_subject_map rr:class ?rdf_class . }
			OPTIONAL { ?_subject_map rr:termType ?termtype . }
			OPTIONAL { ?_subject_map rr:graph ?graph . }
			OPTIONAL { ?_subject_map rr:graphMap ?_graph_structure .
					   ?_graph_structure rr:constant ?graph . }
			OPTIONAL { ?_subject_map rr:graphMap ?_graph_structure .
					   ?_graph_structure rr:template ?graph . }		   

	# Predicate -----------------------------------------------------------------------
			OPTIONAL {
			?triples_map_id rr:predicateObjectMap ?_predicate_object_map .
			
			OPTIONAL {
				?triples_map_id rr:predicateObjectMap ?_predicate_object_map .
				?_predicate_object_map rr:predicateMap ?_predicate_map .
				?_predicate_map rr:constant ?predicate_constant .
			}
			OPTIONAL {
				?_predicate_object_map rr:predicateMap ?_predicate_map .
				?_predicate_map rr:template ?predicate_template .
			}
			OPTIONAL {
				?_predicate_object_map rr:predicateMap ?_predicate_map .
				?_predicate_map rml:reference ?predicate_reference .
			}
			OPTIONAL {
				?_predicate_object_map rr:predicate ?predicate_constant_shortcut .
			 }
			

	# Object --------------------------------------------------------------------------
			OPTIONAL {
				?_predicate_object_map rr:objectMap ?_object_map .
				?_object_map rr:constant ?object_constant .
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
			}
			OPTIONAL {
				?_predicate_object_map rr:objectMap ?_object_map .
				?_object_map rr:template ?object_template .
				OPTIONAL {?_object_map rr:termType ?term .}
				OPTIONAL {?_object_map rml:languageMap ?language_map.
						  ?language_map rml:reference ?language_value.}
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
			}
			OPTIONAL {
				?_predicate_object_map rr:objectMap ?_object_map .
				?_object_map rml:reference ?object_reference .
				OPTIONAL { ?_object_map rr:language ?language .}
				OPTIONAL {?_object_map rml:languageMap ?language_map.
						  ?language_map rml:reference ?language_value.}
				OPTIONAL {?_object_map rr:termType ?term .}
				OPTIONAL {
					?_object_map rr:datatype ?object_datatype .
				}
			}
			OPTIONAL {
				?_predicate_object_map rr:objectMap ?_object_map .
				?_object_map rr:parentTriplesMap ?object_parent_triples_map .
				OPTIONAL {
					?_object_map rr:joinCondition ?join_condition .
					?join_condition rr:child ?child_value;
								 rr:parent ?parent_value.
					OPTIONAL {?_object_map rr:termType ?term .}
				}
			}
			OPTIONAL {
				?_predicate_object_map rr:object ?object_constant_shortcut .
			}
			OPTIONAL {?_predicate_object_map rr:graph ?predicate_object_graph .}
			OPTIONAL { ?_predicate_object_map  rr:graphMap ?_graph_structure .
					   ?_graph_structure rr:constant ?predicate_object_graph  . }
			OPTIONAL { ?_predicate_object_map  rr:graphMap ?_graph_structure .
					   ?_graph_structure rr:template ?predicate_object_graph  . }	
			}
			OPTIONAL {
				?_source a d2rq:Database;
  				d2rq:jdbcDSN ?jdbcDSN; 
  				d2rq:jdbcDriver ?jdbcDriver; 
			    d2rq:username ?user;
			    d2rq:password ?password .
			}
		} """

	mapping_query_results = mapping_graph.query(mapping_query)
	triples_map_list = []


	for result_triples_map in mapping_query_results:
		triples_map_exists = False
		for triples_map in triples_map_list:
			triples_map_exists = triples_map_exists or (str(triples_map.triples_map_id) == str(result_triples_map.triples_map_id))
		
		if not triples_map_exists:
			if result_triples_map.subject_template != None:
				if result_triples_map.rdf_class is None:
					reference, condition = string_separetion(str(result_triples_map.subject_template))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_template), condition, "template", [result_triples_map.rdf_class], result_triples_map.termtype, [result_triples_map.graph])
				else:
					reference, condition = string_separetion(str(result_triples_map.subject_template))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_template), condition, "template", [str(result_triples_map.rdf_class)], result_triples_map.termtype, [result_triples_map.graph])
			elif result_triples_map.subject_reference != None:
				if result_triples_map.rdf_class is None:
					reference, condition = string_separetion(str(result_triples_map.subject_reference))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_reference), condition, "reference", [result_triples_map.rdf_class], result_triples_map.termtype, [result_triples_map.graph])
				else:
					reference, condition = string_separetion(str(result_triples_map.subject_reference))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_reference), condition, "reference", [str(result_triples_map.rdf_class)], result_triples_map.termtype, [result_triples_map.graph])
			elif result_triples_map.subject_constant != None:
				if result_triples_map.rdf_class is None:
					reference, condition = string_separetion(str(result_triples_map.subject_constant))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_constant), condition, "constant", [result_triples_map.rdf_class], result_triples_map.termtype, [result_triples_map.graph])
				else:
					reference, condition = string_separetion(str(result_triples_map.subject_constant))
					subject_map = tm.SubjectMap(str(result_triples_map.subject_constant), condition, "constant", [str(result_triples_map.rdf_class)], result_triples_map.termtype, [result_triples_map.graph])
				
			mapping_query_prepared = prepareQuery(mapping_query)


			mapping_query_prepared_results = mapping_graph.query(mapping_query_prepared, initBindings={'triples_map_id': result_triples_map.triples_map_id})

			join_predicate = {}
			predicate_object_maps_list = []
			predicate_object_graph = {}
			for result_predicate_object_map in mapping_query_prepared_results:
				join = True
				if result_predicate_object_map.predicate_constant != None:
					predicate_map = tm.PredicateMap("constant", str(result_predicate_object_map.predicate_constant), "")
					predicate_object_graph[str(result_predicate_object_map.predicate_constant)] = result_triples_map.predicate_object_graph
				elif result_predicate_object_map.predicate_constant_shortcut != None:
					predicate_map = tm.PredicateMap("constant shortcut", str(result_predicate_object_map.predicate_constant_shortcut), "")
					predicate_object_graph[str(result_predicate_object_map.predicate_constant_shortcut)] = result_triples_map.predicate_object_graph
				elif result_predicate_object_map.predicate_template != None:
					template, condition = string_separetion(str(result_predicate_object_map.predicate_template))
					predicate_map = tm.PredicateMap("template", template, condition)
				elif result_predicate_object_map.predicate_reference != None:
					reference, condition = string_separetion(str(result_predicate_object_map.predicate_reference))
					predicate_map = tm.PredicateMap("reference", reference, condition)
				else:
					predicate_map = tm.PredicateMap("None", "None", "None")

				if result_predicate_object_map.object_constant != None:
					object_map = tm.ObjectMap("constant", str(result_predicate_object_map.object_constant), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language,result_predicate_object_map.language_value)
				elif result_predicate_object_map.object_template != None:
					object_map = tm.ObjectMap("template", str(result_predicate_object_map.object_template), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language,result_predicate_object_map.language_value)
				elif result_predicate_object_map.object_reference != None:
					object_map = tm.ObjectMap("reference", str(result_predicate_object_map.object_reference), str(result_predicate_object_map.object_datatype), "None", "None", result_predicate_object_map.term, result_predicate_object_map.language,result_predicate_object_map.language_value)
				elif result_predicate_object_map.object_parent_triples_map != None:
					if predicate_map.value + " " + str(result_predicate_object_map.object_parent_triples_map) not in join_predicate:
						join_predicate[predicate_map.value + " " + str(result_predicate_object_map.object_parent_triples_map)] = {"predicate":predicate_map, "childs":[str(result_predicate_object_map.child_value)], "parents":[str(result_predicate_object_map.parent_value)], "triples_map":str(result_predicate_object_map.object_parent_triples_map)}
					else:
						join_predicate[predicate_map.value + " " + str(result_predicate_object_map.object_parent_triples_map)]["childs"].append(str(result_predicate_object_map.child_value))
						join_predicate[predicate_map.value + " " + str(result_predicate_object_map.object_parent_triples_map)]["parents"].append(str(result_predicate_object_map.parent_value))
					join = False
				elif result_predicate_object_map.object_constant_shortcut != None:
					object_map = tm.ObjectMap("constant shortcut", str(result_predicate_object_map.object_constant_shortcut), "None", "None", "None", result_predicate_object_map.term, result_predicate_object_map.language,result_predicate_object_map.language_value)
				else:
					object_map = tm.ObjectMap("None", "None", "None", "None", "None", "None", "None", "None")
				if join:
					predicate_object_maps_list += [tm.PredicateObjectMap(predicate_map, object_map,predicate_object_graph)]
				join = True
			if join_predicate:
				for jp in join_predicate.keys():
					object_map = tm.ObjectMap("parent triples map", join_predicate[jp]["triples_map"], str(result_predicate_object_map.object_datatype), join_predicate[jp]["childs"], join_predicate[jp]["parents"],result_predicate_object_map.term, result_predicate_object_map.language,result_predicate_object_map.language_value)
					predicate_object_maps_list += [tm.PredicateObjectMap(join_predicate[jp]["predicate"], object_map,predicate_object_graph)]

			current_triples_map = tm.TriplesMap(str(result_triples_map.triples_map_id), str(result_triples_map.data_source), subject_map, predicate_object_maps_list, ref_form=str(result_triples_map.ref_form), iterator=str(result_triples_map.iterator), tablename=str(result_triples_map.tablename), query=str(result_triples_map.query))
			triples_map_list += [current_triples_map]

		else:
			for triples_map in triples_map_list:
				if str(triples_map.triples_map_id) == str(result_triples_map.triples_map_id):
					if result_triples_map.rdf_class not in triples_map.subject_map.rdf_class:
						triples_map.subject_map.rdf_class.append(result_triples_map.rdf_class)
					if result_triples_map.graph not in triples_map.subject_map.graph:
						triples_map.graph.append(result_triples_map.graph)

	return triples_map_list


def semantify_xml(triples_map, triples_map_list, output_file_descriptor):
	print("TM:", triples_map.triples_map_name)
	i = 0
	triples_map_triples = {}
	generated_triples = {}
	object_list = []
	global blank_message
	with open(str(triples_map.data_source), "r") as input_file_descriptor:
		tree = ET.parse(input_file_descriptor)
		root = tree.getroot()

		if "[" not in triples_map.iterator:
			level = triples_map.iterator.split("/")[len(triples_map.iterator.split("/"))-1]
		else:
			temp = triples_map.iterator.split("[")[0]
			level = temp.split("/")[len(temp.split("/"))-1]
		parent_map = {c: p for p in tree.iter() for c in p}
		namespace = dict([node for _, node in ET.iterparse(str(triples_map.data_source),events=['start-ns'])])
		if namespace:
			for name in namespace:
				ET.register_namespace(name, namespace[name])
		for child in root.iterfind(level, namespace):
			subject_value = string_substitution_xml(triples_map.subject_map.value, "{(.+?)}", child, "subject", triples_map.iterator, parent_map, namespace)
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
							subject = "<" + subject_value  + ">"
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
									subject = "<" + base + subject_value + ">"
								else:
									subject = "<" + subject_value + ">"
							except:
								subject = None
						
					elif "BlankNode" in triples_map.subject_map.term_type:
						if triples_map.subject_map.condition == "":

							try:
								if "/" in subject_value:
									subject  = "_:" + encode_char(subject_value.replace("/","2F")).replace("%","")
									if blank_message:
										print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
										blank_message = False
								else:
									subject = "_:" + encode_char(subject_value).replace("%","") 
								if "." in subject:
									subject = subject.replace(".","2E")
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
					subject_value = string_substitution_xml(triples_map.subject_map.value, ".+", child, "subject", triples_map.iterator, parent_map, namespace)
					subject_value = subject_value[0][1:-1]
					try:
						if " " not in subject_value:
							if "http" not in subject_value:
								subject = "<" + base + subject_value + ">"
							else:
								subject = "<" + subject_value + ">"
						else:
							print("<http://example.com/base/" + subject_value + "> is an invalid URL")
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
							subject = "<" + base + subject_value + ">"
						else:
							subject = "<" + subject_value + ">"
					except:
						subject = None

			elif "constant" in triples_map.subject_map.subject_mapping_type:
				subject = "<" + subject_value + ">"

			else:
				if triples_map.subject_map.condition == "":

					try:
						subject =  "\"" + triples_map.subject_map.value + "\"" 
					except:
						subject = None

				else:
				#	field, condition = condition_separetor(triples_map.subject_map.condition)
				#	if row[field] == condition:
					try:
						subject =  "\"" + triples_map.subject_map.value + "\""
					except:
						subject = None

			if triples_map.subject_map.rdf_class != None and subject != None:
				predicate = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
				for rdf_class in triples_map.subject_map.rdf_class:
					if rdf_class != None:
						obj = "<{}>".format(rdf_class)
						dictionary_table_update(subject)
						dictionary_table_update(obj)
						dictionary_table_update(predicate + "_" + obj)
						rdf_type = subject + " " + predicate + " " + obj + " .\n"
						for graph in triples_map.subject_map.graph:	
							if graph != None and "defaultGraph" not in graph:
								if "{" in graph:	
									rdf_type = rdf_type[:-2] + " <" + string_substitution_xml(graph, "{(.+?)}", child, "subject", triples_map.iterator, parent_map, namespace) + "> .\n"
									dictionary_table_update("<" + string_substitution_xml(graph, "{(.+?)}", child, "subject", triples_map.iterator, parent_map, namespace) + ">")
								else:
									rdf_type = rdf_type[:-2] + " <" + graph + "> .\n"
									dictionary_table_update("<" + graph + ">")
							if duplicate == "yes":
								if dic_table[predicate + "_" + obj] not in g_triples:
									output_file_descriptor.write(rdf_type)
									g_triples.update({dic_table[predicate  + "_" + obj ] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
									i += 1
								elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + obj]]:
									output_file_descriptor.write(rdf_type)
									g_triples[dic_table[predicate + "_" + obj]].update({dic_table[subject] + "_" + dic_table[obj] : ""})
									i += 1
							else:
								output_file_descriptor.write(rdf_type)
								i += 1


			for predicate_object_map in triples_map.predicate_object_maps_list:
				if predicate_object_map.predicate_map.mapping_type == "constant" or predicate_object_map.predicate_map.mapping_type == "constant shortcut":
					predicate = "<" + predicate_object_map.predicate_map.value + ">"
				elif predicate_object_map.predicate_map.mapping_type == "template":
					if predicate_object_map.predicate_map.condition != "":
							#field, condition = condition_separetor(predicate_object_map.predicate_map.condition)
							#if row[field] == condition:
							try:
								predicate = "<" + string_substitution_xml(predicate_object_map.predicate_map.value, "{(.+?)}", child, "predicate", triples_map.iterator, parent_map, namespace) + ">"
							except:
								predicate = None
							#else:
							#	predicate = None
					else:
						try:
							predicate = "<" + string_substitution_xml(predicate_object_map.predicate_map.value, "{(.+?)}", child, "predicate", triples_map.iterator, parent_map, namespace) + ">"
						except:
							predicate = None
				elif predicate_object_map.predicate_map.mapping_type == "reference":
					if predicate_object_map.predicate_map.condition != "":
						#field, condition = condition_separetor(predicate_object_map.predicate_map.condition)
						#if row[field] == condition:
						predicate = string_substitution_xml(predicate_object_map.predicate_map.value, ".+", child, "predicate", triples_map.iterator, parent_map, namespace)
						#else:
						#	predicate = None
					else:
						predicate = string_substitution_xml(predicate_object_map.predicate_map.value, ".+", child, "predicate", triples_map.iterator, parent_map, namespace)
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
				elif predicate_object_map.object_map.mapping_type == "template":
					object = string_substitution_xml(predicate_object_map.object_map.value, "{(.+?)}", child, "object", triples_map.iterator, parent_map, namespace)
					if isinstance(object,list):
						for i in range(len(object)):
							if predicate_object_map.object_map.term is None:
								object[i] = "<" + object[i] + ">"
							elif "IRI" in predicate_object_map.object_map.term:
								object[i] = "<" + object[i] + ">"
							else:
								object[i] = "\"" + object[i] + "\""
								if predicate_object_map.object_map.datatype != None:
									object[i] = "\"" + object[i][1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
								elif predicate_object_map.object_map.language != None:
									if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
										object[i] += "@es"
									elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
										object[i] += "@en"
									elif len(predicate_object_map.object_map.language) == 2:
										object[i] += "@"+predicate_object_map.object_map.language
								elif predicate_object_map.object_map.language_map != None:
									lang = string_substitution_xml(predicate_object_map.object_map.language_map, ".+", child, "object", triples_map.iterator, parent_map, namespace)
									if lang != None:
										object[i] += "@" + string_substitution_xml(predicate_object_map.object_map.language_map, ".+", child, "object", triples_map.iterator, parent_map, namespace)[1:-1]
					else:
						if predicate_object_map.object_map.term is None:
							object = "<" + object + ">"
						elif "IRI" in predicate_object_map.object_map.term:
							object = "<" + object + ">"
						else:
							object = "\"" + object + "\""
							if predicate_object_map.object_map.datatype != None:
								object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
							elif predicate_object_map.object_map.language != None:
								if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
									object += "@es"
								elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
									object += "@en"
								elif len(predicate_object_map.object_map.language) == 2:
									object += "@"+predicate_object_map.object_map.language
							elif predicate_object_map.object_map.language_map != None:
								lang = string_substitution_xml(predicate_object_map.object_map.language_map, ".+", child, "object", triples_map.iterator, parent_map, namespace)
								if lang != None:
									object += "@" + string_substitution_xml(predicate_object_map.object_map.language_map, ".+", child, "object", triples_map.iterator, parent_map, namespace)[1:-1]
				elif predicate_object_map.object_map.mapping_type == "reference":
					object = string_substitution_xml(predicate_object_map.object_map.value, ".+", child, "object", triples_map.iterator, parent_map, namespace)
					if object != None:
						if isinstance(object,list):
							for i in range(len(object)):
								if "\\" in object[i][1:-1]:
									object = "\"" + object[1:-1].replace("\\","\\\\") + "\""
								if "'" in object[i][1:-1]:
									object = "\"" + object[1:-1].replace("'","\\\\'") + "\""
								if "\n" in object[i]:
									object[i] = object[i].replace("\n","\\n") 
								if predicate_object_map.object_map.datatype != None:
									object[i] += "^^<{}>".format(predicate_object_map.object_map.datatype)
								elif predicate_object_map.object_map.language != None:
									if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
										object[i] += "@es"
									elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
										object[i] += "@en"
									elif len(predicate_object_map.object_map.language) == 2:
										object[i] += "@"+predicate_object_map.object_map.language
								elif predicate_object_map.object_map.language_map != None:
									lang = string_substitution_xml(predicate_object_map.object_map.language_map, ".+", child, "object", triples_map.iterator, parent_map, namespace)
									if lang != None:
										object[i] += "@"+ string_substitution_xml(predicate_object_map.object_map.language_map, ".+", child, "object", triples_map.iterator, parent_map, namespace)[1:-1]
								elif predicate_object_map.object_map.term != None:
									if "IRI" in predicate_object_map.object_map.term:
										if " " not in object:
											object[i] = "\"" + object[i][1:-1].replace("\\\\'","'") + "\""
											object[i] = "<" + encode_char(object[i][1:-1]) + ">"
										else:
											object[i] = None
						else:
							if "\\" in object[1:-1]:
								object = "\"" + object[1:-1].replace("\\","\\\\") + "\""
							if "'" in object[1:-1]:
								object = "\"" + object[1:-1].replace("'","\\\\'") + "\""
							if "\n" in object:
								object = object.replace("\n","\\n") 
							if predicate_object_map.object_map.datatype != None:
								object += "^^<{}>".format(predicate_object_map.object_map.datatype)
							elif predicate_object_map.object_map.language != None:
								if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
									object += "@es"
								elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
									object += "@en"
								elif len(predicate_object_map.object_map.language) == 2:
									object += "@"+predicate_object_map.object_map.language
							elif predicate_object_map.object_map.language_map != None:
								lang = string_substitution_xml(predicate_object_map.object_map.language_map, ".+", child, "object", triples_map.iterator, parent_map, namespace)
								if lang != None:
									object += "@"+ string_substitution_xml(predicate_object_map.object_map.language_map, ".+", child, "object", triples_map.iterator, parent_map, namespace)[1:-1]
							elif predicate_object_map.object_map.term != None:
								if "IRI" in predicate_object_map.object_map.term:
									if " " not in object:
										object = "\"" + object[1:-1].replace("\\\\'","'") + "\""
										object = "<" + encode_char(object[1:-1]) + ">"
									else:
										object = None
				elif predicate_object_map.object_map.mapping_type == "parent triples map":
					if subject != None:
						for triples_map_element in triples_map_list:
							if triples_map_element.triples_map_id == predicate_object_map.object_map.value:
								if triples_map_element.data_source != triples_map.data_source:
									if triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0] not in join_table:
										if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
											with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
												if str(triples_map_element.file_format).lower() == "csv":
													data = csv.DictReader(input_file_descriptor, delimiter=delimiter)
													hash_maker(data, triples_map_element, predicate_object_map.object_map)
												else:
													data = json.load(input_file_descriptor)
													if isinstance(data, list):
														hash_maker(data, triples_map_element, predicate_object_map.object_map)
													elif len(data) < 2:
														hash_maker(data[list(data.keys())[0]], triples_map_element, predicate_object_map.object_map)

										elif triples_map_element.file_format == "XPath":
											with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
												child_tree = ET.parse(input_file_descriptor)
												child_root = child_tree.getroot()
												hash_maker_xml(child_root, triples_map_element, predicate_object_map.object_map, parent_map, namespace)							
										else:
											database, query_list = translate_sql(triples_map)
											db = connector.connect(host=host, port=int(port), user=user, password=password)
											cursor = db.cursor(buffered=True)
											cursor.execute("use " + database)
											for query in query_list:
												cursor.execute(query)
											hash_maker_array(cursor, triples_map_element, predicate_object_map.object_map)

									if child.find(predicate_object_map.object_map.child[0]) != None:
										if child.find(predicate_object_map.object_map.child[0]).text in join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]]:
											object_list = join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]][child.find(predicate_object_map.object_map.child[0]).text]
										else:
											object_list = []
									object = None
								else:
									if predicate_object_map.object_map.parent != None:
										if triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0] not in join_table:
											with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
												child_tree = ET.parse(input_file_descriptor)
												child_root = child_tree.getroot()
												hash_maker_xml(child_root, triples_map_element, predicate_object_map.object_map, parent_map, namespace)

										if child.find(predicate_object_map.object_map.child[0]) != None:
											if child.find(predicate_object_map.object_map.child[0]).text in join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]]:
												object_list = join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]][child.find(predicate_object_map.object_map.child[0]).text]
											else:
												object_list = []
										object = None
									else:
										try:
											object = "<" + string_substitution_xml(triples_map_element.subject_map.value, "{(.+?)}", child, "subject", triples_map.iterator, parent_map, namespace) + ">"
										except TypeError:
											object = None
								break
							else:
								continue
					else:
						object = None
				else:
					object = None

				if predicate in general_predicates:
					dictionary_table_update(predicate + "_" + predicate_object_map.object_map.value)
				else:
					dictionary_table_update(predicate)
				if predicate != None and (object != None or object) and subject != None:
					for graph in triples_map.subject_map.graph:
						dictionary_table_update(subject)
						if isinstance(object,list):
							for obj in object:
								dictionary_table_update(obj)
								triple = subject + " " + predicate + " " + obj + ".\n"
								if graph != None and "defaultGraph" not in graph:
									if "{" in graph:
										triple = triple[:-2] + " <" + string_substitution_xml(graph, "{(.+?)}", child, "subject", triples_map.iterator, parent_map, namespace) + ">.\n"
										dictionary_table_update("<" + string_substitution_xml(graph, "{(.+?)}", child, "subject", triples_map.iterator, parent_map, namespace) + ">")
									else:
										triple = triple[:-2] + " <" + graph + ">.\n"
										dictionary_table_update("<" + graph + ">")
								if duplicate == "yes":
									if predicate in general_predicates:
										if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
											output_file_descriptor.write(triple)
											g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
											i += 1
										elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
											output_file_descriptor.write(triple)
											g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
											i += 1
									else:
										if dic_table[predicate] not in g_triples:					
											output_file_descriptor.write(triple)
											g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
											i += 1
										elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
											output_file_descriptor.write(triple)
											g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
											i += 1
								else:
									output_file_descriptor.write(triple)
									i += 1
						else:
							dictionary_table_update(object)
							triple = subject + " " + predicate + " " + object + ".\n"
							if graph != None and "defaultGraph" not in graph:
								if "{" in graph:
									triple = triple[:-2] + " <" + string_substitution_xml(graph, "{(.+?)}", child, "subject", triples_map.iterator, parent_map, namespace) + ">.\n"
									dictionary_table_update("<" + string_substitution_xml(graph, "{(.+?)}", child, "subject", triples_map.iterator, parent_map, namespace) + ">")
								else:
									triple = triple[:-2] + " <" + graph + ">.\n"
									dictionary_table_update("<" + graph + ">")
							if duplicate == "yes":
								if predicate in general_predicates:
									if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
										output_file_descriptor.write(triple)
										g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[object]: ""}})
										i += 1
									elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
										output_file_descriptor.write(triple)
										g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[object]: ""})
										i += 1
								else:
									if dic_table[predicate] not in g_triples:					
										output_file_descriptor.write(triple)
										g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[object]: ""}})
										i += 1
									elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
										output_file_descriptor.write(triple)
										g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[object]: ""})
										i += 1
							else:
								output_file_descriptor.write(triple)
								i += 1
					if predicate[1:-1] in predicate_object_map.graph:
						if isinstance(object,list):
							for obj in object:
								triple = subject + " " + predicate + " " + obj + ".\n"
								if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in predicate_object_map.graph[predicate[1:-1]]:
									if "{" in predicate_object_map.graph[predicate[1:-1]]:
										triple = triple[:-2] + " <" + string_substitution_xml(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", child, "subject", triples_map.iterator, parent_map, namespace) + ">.\n"
										dictionary_table_update("<" + string_substitution_xml(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", child, "subject", triples_map.iterator, parent_map, namespace) + ">")
									else:
										triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
										dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
									if duplicate == "yes":
										if predicate in general_predicates:
											if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
												output_file_descriptor.write(triple)
												g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
												i += 1
											elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[predicate + "_" + predicate_object_map.object_map.value]:
												output_file_descriptor.write(triple)
												g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
												i += 1
										else:
											if dic_table[predicate] not in g_triples:					
												output_file_descriptor.write(triple)
												g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
												i += 1
											elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
												output_file_descriptor.write(triple)
												g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
												i += 1
									else:
										output_file_descriptor.write(triple)
						else:
							triple = subject + " " + predicate + " " + object + ".\n"
							if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in predicate_object_map.graph[predicate[1:-1]]:
								if "{" in predicate_object_map.graph[predicate[1:-1]]:
									triple = triple[:-2] + " <" + string_substitution_xml(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", child, "subject", triples_map.iterator, parent_map, namespace) + ">.\n"
								else:
									triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
								if duplicate == "yes":
									if predicate in general_predicates:
										if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
											output_file_descriptor.write(triple)
											g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[object]: ""}})
											i += 1
										elif dic_table[subject] + "_" + dic_table[object] not in g_triples[predicate + "_" + predicate_object_map.object_map.value]:
											output_file_descriptor.write(triple)
											g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[object]: ""})
											i += 1
									else:
										if dic_table[predicate] not in g_triples:					
											output_file_descriptor.write(triple)
											g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[object]: ""}})
											i += 1
										elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
											output_file_descriptor.write(triple)
											g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[object]: ""})
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
									triple = triple[:-2] + " <" + string_substitution_xml(graph, "{(.+?)}", child, "subject", triples_map.iterator, parent_map, namespace) + ">.\n"
									dictionary_table_update("<" + string_substitution_xml(graph, "{(.+?)}", child, "subject", triples_map.iterator, parent_map, namespace) + ">")
								else:
									triple = triple[:-2] + " <" + graph + ">.\n"
									dictionary_table_update("<" + graph + ">")

							if duplicate == "yes":
								if predicate in general_predicates:
									if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
										output_file_descriptor.write(triple)
										g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
										i += 1
									elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
										output_file_descriptor.write(triple)
										g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
										i += 1
								else:
									if dic_table[predicate] not in g_triples:
										output_file_descriptor.write(triple)
										g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
										i += 1
									elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
										output_file_descriptor.write(triple)
										g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
										i += 1
							else:
								output_file_descriptor.write(triple)
								i += 1
						if predicate[1:-1] in predicate_object_map.graph:
							triple = subject + " " + predicate + " " + obj + ".\n"
							if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in predicate_object_map.graph[predicate[1:-1]]:
								if "{" in predicate_object_map.graph[predicate[1:-1]]:
									triple = triple[:-2] + " <" + string_substitution_xml(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", child, "subject", triples_map.iterator, parent_map, namespace) + ">.\n"
									dictionary_table_update("<" + string_substitution_xml(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", child, "subject", triples_map.iterator, parent_map, namespace) + ">")
								else:
									triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
									dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
								if duplicate == "yes":
									if predicate in general_predicates:
										if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
											output_file_descriptor.write(triple)
											g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
											i += 1
										elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
											output_file_descriptor.write(triple)
											g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
											i += 1
									else:
										if dic_table[predicate] not in g_triples:					
											output_file_descriptor.write(triple)
											g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
											i += 1
										elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
											output_file_descriptor.write(triple)
											g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
											i += 1
								else:
									output_file_descriptor.write(triple)
									i += 1
					object_list = []
				else:
					continue
	return i

def semantify_json(triples_map, triples_map_list, delimiter, output_file_descriptor, data, iterator):
	print("TM:", triples_map.triples_map_name)

	triples_map_triples = {}
	generated_triples = {}
	object_list = []
	global blank_message
	i = 0
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
			elif "" == tp and isinstance(row,dict):
				if len(row.keys()) == 1:
					while list(row.keys())[0] not in temp_keys:
						new_iterator += "."
						row = row[list(row.keys())[0]]
						if isinstance(row,list):
							for sub_row in row:
								i += semantify_json(triples_map, triples_map_list, delimiter, output_file_descriptor, sub_row, iterator.replace(new_iterator[:-1],""))
							executed = False
							break
						if isinstance(row,str):
							row = []
							break			
				if "*" == new_iterator[-2]:
					for sub_row in row:
						i += semantify_json(triples_map, triples_map_list, delimiter, output_file_descriptor, row[sub_row], iterator.replace(new_iterator[:-1],""))
					executed = False
					break
				if "[*][*]" in new_iterator:
					for sub_row in row:
						for sub_sub_row in row[sub_row]:
							i += semantify_json(triples_map, triples_map_list, delimiter, output_file_descriptor, sub_sub_row, iterator.replace(new_iterator[:-1],""))
					executed = False
					break
				if isinstance(row,list):
					for sub_row in row:
						i += semantify_json(triples_map, triples_map_list, delimiter, output_file_descriptor, sub_row, iterator.replace(new_iterator[:-1],""))
					executed = False
					break
		if executed:
			if isinstance(row,list):
				for sub_row in row:
					i += semantify_json(triples_map, triples_map_list, delimiter, output_file_descriptor, sub_row, iterator.replace(new_iterator[:-1],""))
			else:
				i += semantify_json(triples_map, triples_map_list, delimiter, output_file_descriptor, row, iterator.replace(new_iterator[:-1],""))
	else:
		subject_value = string_substitution_json(triples_map.subject_map.value, "{(.+?)}", data, "subject",ignore,iterator) 		
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
						subject = "<" + subject_value  + ">"
					except:
						subject = None
			else:
				if "IRI" in triples_map.subject_map.term_type:
					if triples_map.subject_map.condition == "":

						try:
							if "http" not in subject_value:
								subject = "<" + base + subject_value + ">"
							else:
								subject = "<" + encode_char(subject_value) + ">"
						except:
							subject = None

					else:
					#	field, condition = condition_separetor(triples_map.subject_map.condition)
					#	if row[field] == condition:
						try:
							if "http" not in subject_value:
								subject = "<" + base + subject_value + ">"
							else:
								subject = "<" + subject_value + ">" 
						except:
							subject = None
					
				elif "BlankNode" in triples_map.subject_map.term_type:
					if triples_map.subject_map.condition == "":

						try:
							if "/" in subject_value:
								subject  = "_:" + encode_char(subject_value.replace("/","2F")).replace("%","")
								if blank_message:
									print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
									blank_message = False
							else:
								subject = "_:" + encode_char(subject_value).replace("%","")
							if "." in subject:
								subject = subject.replace(".","2E")
							 
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
				subject_value = string_substitution_json(triples_map.subject_map.value, ".+", data, "subject",ignore,iterator)
				subject_value = subject_value[1:-1]
				try:
					if " " not in subject_value:
						if "http" not in subject_value:
							subject = "<" + base + subject_value + ">"
						else:
							subject = "<" + subject_value + ">"
					else:
						print("<http://example.com/base/" + subject_value + "> is an invalid URL")
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
						subject = "<" + base + subject_value + ">"
					else:
						subject = "<" + subject_value + ">"
				except:
					subject = None

		elif "constant" in triples_map.subject_map.subject_mapping_type:
			subject = "<" + subject_value + ">"
		elif "Literal" in triples_map.subject_map.term_type:
			subject = None
		else:
			if triples_map.subject_map.condition == "":

				try:
					subject =  "\"" + triples_map.subject_map.value + "\""
				except:
					subject = None

			else:
			#	field, condition = condition_separetor(triples_map.subject_map.condition)
			#	if row[field] == condition:
				try:
					subject =  "\"" + triples_map.subject_map.value + "\""
				except:
					subject = None

		if triples_map.subject_map.rdf_class != None and subject != None:
			predicate = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
			for rdf_class in triples_map.subject_map.rdf_class:
				if rdf_class != None:
					for graph in triples_map.subject_map.graph:
						obj = "<{}>".format(rdf_class)
						dictionary_table_update(subject)
						dictionary_table_update(obj)
						dictionary_table_update(predicate + "_" + obj)
						rdf_type = subject + " " + predicate + " " + obj + ".\n"
						if graph != None and "defaultGraph" not in graph:
							if "{" in graph:	
								rdf_type = rdf_type[:-2] + " <" + string_substitution_json(graph, "{(.+?)}", data, "subject",ignore,iterator) + "> .\n"
								dictionary_table_update("<" + string_substitution_json(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">")
							else:
								rdf_type = rdf_type[:-2] + " <" + graph + "> .\n"
								dictionary_table_update("<" + graph + ">")
						if duplicate == "yes":
							if dic_table[predicate + "_" + obj] not in g_triples:
								output_file_descriptor.write(rdf_type)
								g_triples.update({dic_table[predicate  + "_" + obj ] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
								i += 1
							elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + obj]]:
								output_file_descriptor.write(rdf_type)
								g_triples[dic_table[predicate + "_" + obj]].update({dic_table[subject] + "_" + dic_table[obj] : ""})
								i += 1
						else:
							output_file_descriptor.write(rdf_type)
							i += 1
		
		for predicate_object_map in triples_map.predicate_object_maps_list:
			if predicate_object_map.predicate_map.mapping_type == "constant" or predicate_object_map.predicate_map.mapping_type == "constant shortcut":
				predicate = "<" + predicate_object_map.predicate_map.value + ">"
			elif predicate_object_map.predicate_map.mapping_type == "template":
				if predicate_object_map.predicate_map.condition != "":
						#field, condition = condition_separetor(predicate_object_map.predicate_map.condition)
						#if row[field] == condition:
						try:
							predicate = "<" + string_substitution_json(predicate_object_map.predicate_map.value, "{(.+?)}", data, "predicate",ignore, iterator) + ">"
						except:
							predicate = None
						#else:
						#	predicate = None
				else:
					try:
						predicate = "<" + string_substitution_json(predicate_object_map.predicate_map.value, "{(.+?)}", data, "predicate",ignore, iterator) + ">"
					except:
						predicate = None
			elif predicate_object_map.predicate_map.mapping_type == "reference":
				if predicate_object_map.predicate_map.condition != "":
					#field, condition = condition_separetor(predicate_object_map.predicate_map.condition)
					#if row[field] == condition:
					predicate = string_substitution_json(predicate_object_map.predicate_map.value, ".+", data, "predicate",ignore, iterator)
					#else:
					#	predicate = None
				else:
					predicate = string_substitution_json(predicate_object_map.predicate_map.value, ".+", data, "predicate",ignore, iterator)
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
			elif predicate_object_map.object_map.mapping_type == "template":
				try:
					if predicate_object_map.object_map.term is None:
						object = "<" + string_substitution_json(predicate_object_map.object_map.value, "{(.+?)}", data, "object",ignore, iterator) + ">"
					elif "IRI" in predicate_object_map.object_map.term:
						object = "<" + string_substitution_json(predicate_object_map.object_map.value, "{(.+?)}", data, "object",ignore, iterator) + ">"
					elif "BlankNode" in predicate_object_map.object_map.term:
						object = "_:" + string_substitution_json(predicate_object_map.object_map.value, "{(.+?)}", data, "object",ignore, iterator)
						if "/" in object:
							object  = object.replace("/","2F")
							print("Incorrect format for Blank Nodes. \"/\" will be replace with \"-\".")
						if "." in object:
							object = object.replace(".","2E")
						object = encode_char(object)
					else:
						object = "\"" + string_substitution_json(predicate_object_map.object_map.value, "{(.+?)}", data, "object",ignore, iterator) + "\""
						if predicate_object_map.object_map.datatype != None:
							object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
						elif predicate_object_map.object_map.language != None:
							if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
								object += "@es"
							elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
								object += "@en"
							elif len(predicate_object_map.object_map.language) == 2:
								object += "@"+predicate_object_map.object_map.language
						elif predicate_object_map.object_map.language_map != None:
							lang = string_substitution_json(predicate_object_map.object_map.language_map, ".+", data, "object",ignore, iterator)
							if lang != None:
								object += "@" + string_substitution_json(predicate_object_map.object_map.language_map, ".+", data, "object",ignore, iterator)[1:-1]
				except TypeError:
					object = None
			elif predicate_object_map.object_map.mapping_type == "reference":
				object = string_substitution_json(predicate_object_map.object_map.value, ".+", data, "object", ignore, iterator)
				if isinstance(object,list):
					object_list = object
					object = None
					if object_list:
						i = 0
						while i < len(object_list):
							if "\\" in object[i][1:-1]:
								object = "\"" + object[i][1:-1].replace("\\","\\\\") + "\""
							if "'" in object_list[i][1:-1]:
								object_list[i] = "\"" + object_list[i][1:-1].replace("'","\\\\'") + "\""
							if "\n" in object_list[i]:
								object_list[i] = object_list[i].replace("\n","\\n")
							if predicate_object_map.object_map.datatype != None:
								object_list[i] = "\"" + object_list[i][1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
							elif predicate_object_map.object_map.language != None:
								if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
									object_list[i] += "@es"
								elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
									object_list[i] += "@en"
								elif len(predicate_object_map.object_map.language) == 2:
									object_list[i] += "@"+predicate_object_map.object_map.language
							elif predicate_object_map.object_map.language_map != None:
									object_list[i] += "@"+ string_substitution_json(predicate_object_map.object_map.language_map, ".+", data, "object",ignore, iterator)[1:-1]
							elif predicate_object_map.object_map.term != None:
								if "IRI" in predicate_object_map.object_map.term:
									if " " not in object_list[i]:
										object_list[i] = "\"" + object_list[i][1:-1].replace("\\\\'","'") + "\""
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
							object = "\"" + object[1:-1].replace("\\","\\\\") + "\""
						if "'" in object[1:-1]:
							object = "\"" + object[1:-1].replace("'","\\\\'") + "\""
						if "\n" in object:
							object = object.replace("\n","\\n")
						if predicate_object_map.object_map.datatype != None:
							object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
						elif predicate_object_map.object_map.language != None:
							if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
								object += "@es"
							elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
								object += "@en"
							elif len(predicate_object_map.object_map.language) == 2:
								object += "@"+predicate_object_map.object_map.language
						elif predicate_object_map.object_map.language_map != None:
							lang = string_substitution_json(predicate_object_map.object_map.language_map, ".+", data, "object",ignore, iterator)
							if lang != None:
								object += "@"+ string_substitution_json(predicate_object_map.object_map.language_map, ".+", data, "object",ignore, iterator)[1:-1]
						elif predicate_object_map.object_map.term != None:
							if "IRI" in predicate_object_map.object_map.term:
								if " " not in object:
									object = "\"" + object[1:-1].replace("\\\\'","'") + "\""
									object = "<" + encode_char(object[1:-1]) + ">"
								else:
									object = None
			elif predicate_object_map.object_map.mapping_type == "parent triples map":
				if subject != None:
					for triples_map_element in triples_map_list:
						if triples_map_element.triples_map_id == predicate_object_map.object_map.value:
							if triples_map_element.data_source != triples_map.data_source:
								if triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0] not in join_table:
									if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
										with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
											if str(triples_map_element.file_format).lower() == "csv":
												data_element = csv.DictReader(input_file_descriptor, delimiter=delimiter)
												hash_maker(data_element, triples_map_element, predicate_object_map.object_map)
											else:
												data_element = json.load(input_file_descriptor)
												if triples_map_element.iterator != "None" and triples_map_element.iterator != "$.[*]":
													join_iterator(data_element, triples_map_element.iterator, triples_map_element, predicate_object_map.object_map)
												else:
													hash_maker(element[list(element.keys())[0]], triples_map_element, predicate_object_map.object_map)

									elif triples_map_element.file_format == "XPath":
										with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
											child_tree = ET.parse(input_file_descriptor)
											child_root = child_tree.getroot()
											hash_maker_xml(child_root, triples_map_element, predicate_object_map.object_map)							
									else:
										database, query_list = translate_sql(triples_map)
										db = connector.connect(host=host, port=int(port), user=user, password=password)
										cursor = db.cursor(buffered=True)
										cursor.execute("use " + database)
										for query in query_list:
											cursor.execute(query)
										hash_maker_array(cursor, triples_map_element, predicate_object_map.object_map)
								if sublist(predicate_object_map.object_map.child,data.keys()):
									if child_list_value(predicate_object_map.object_map.child,data) in join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)]:
										object_list = join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)][child_list_value(predicate_object_map.object_map.child,data)]
									else:
										object_list = []
								object = None
							else:
								if predicate_object_map.object_map.parent != None:
									if triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0] not in join_table:
										with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
											if str(triples_map_element.file_format).lower() == "csv":
												data = csv.DictReader(input_file_descriptor, delimiter=delimiter)
												hash_maker(data, triples_map_element, predicate_object_map.object_map)
											else:
												parent_data = json.load(input_file_descriptor)
												if triples_map_element.iterator != "None":
													join_iterator(parent_data, triples_map_element.iterator, triples_map_element, predicate_object_map.object_map)
												else:
													hash_maker(parent_data[list(parent_data.keys())[0]], triples_map_element, predicate_object_map.object_map)
									if "." in predicate_object_map.object_map.child[0]:
										temp_keys = predicate_object_map.object_map.child[0].split(".")
										temp_data = data
										for temp in temp_keys:
											if temp in temp_data:
												temp_data = temp_data[temp]
											else:
												temp_data = ""
												break
										if temp_data in join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]] and temp_data != "":
											object_list = join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]][temp_data]
										else:
											object_list = []
									else:
										if predicate_object_map.object_map.child[0] in data.keys():
											if data[predicate_object_map.object_map.child[0]] in join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]]:
												object_list = join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]][data[predicate_object_map.object_map.child[0]]]
											else:
												object_list = []
										else:
											if "." in predicate_object_map.object_map.child[0]:
												iterators = predicate_object_map.object_map.child[0].split(".")
												if "[*]" in iterators[0]:
													data = data[iterators[0].split("[*]")[0]]
													for row in data:
														if str(row[iterators[1]]) in join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]]:
															object_list = join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]][str(row[iterators[1]])]
															if predicate != None and subject != None and object_list:
																for obj in object_list:
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
																				triple = triple[:-2] + " <" + string_substitution_json(graph, "{(.+?)}", data, "subject",ignore, iterator) + ">.\n"
																			else:
																				triple = triple[:-2] + " <" + graph + ">.\n"
																		if duplicate == "yes":
																			if (triple not in generated_triples) and (triple not in g_triples):
																				output_file_descriptor.write(triple)
																				generated_triples.update({triple : number_triple})
																				g_triples.update({triple : number_triple})
																				i += 1
																		else:
																			output_file_descriptor.write(triple)
																			i += 1
																	if predicate[1:-1] in predicate_object_map.graph:
																		triple = subject + " " + predicate + " " + obj + ".\n"
																		if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in predicate_object_map.graph[predicate[1:-1]]:
																			if "{" in predicate_object_map.graph[predicate[1:-1]]:
																				triple = triple[:-2] + " <" + string_substitution_json(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", data, "subject",ignore, iterator) + ">.\n"
																			else:
																				triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
																			if duplicate == "yes":
																				if predicate not in g_triples:					
																					output_file_descriptor.write(triple)
																					generated_triples.update({triple : number_triple})
																					g_triples.update({predicate : {subject + "_" + object: triple}})
																					i += 1
																				elif subject + "_" + object not in g_triples[predicate]:
																					output_file_descriptor.write(triple)
																					generated_triples.update({triple : number_triple})
																					g_triples[predicate].update({subject + "_" + object: triple})
																					i += 1
																				elif triple not in g_triples[predicate][subject + "_" + obj]: 
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
														if str(data[index][iterators[1]]) in join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]]:
															object_list = join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]][str(data[int(index)][iterators[1]])]
														else:
															object_list = []
													else:
														print("Requesting an element outside list range.")
														object_list = []	

									object = None
								else:
									print("hola")
									if triples_map_element.iterator != triples_map.iterator:
										parent_iterator = triples_map_element.iterator
										child_keys = triples_map.iterator.split(".")
										for child in child_keys:
											if child in parent_iterator:
												parent_iterator = parent_iterator.replace(child,"")[1:]
											else:
												break
									else:
										parent_iterator = ""
									try:
										object = "<" + string_substitution_json(triples_map_element.subject_map.value, "{(.+?)}", data, "object",ignore, parent_iterator) + ">"
									except TypeError:
										object = None
							break
						else:
							continue
				else:
					object = None
			else:
				object = None
			
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
							triple = triple[:-2] + " <" + string_substitution_json(graph, "{(.+?)}", data, "subject",ignore, iterator) + ">.\n"
							dictionary_table_update("<" + string_substitution_json(graph, "{(.+?)}", data, "subject",ignore, iterator) + ">")
						else:
							triple = triple[:-2] + " <" + graph + ">.\n"
							dictionary_table_update("<" + graph + ">")
					if duplicate == "yes":
						if predicate in general_predicates:
							if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
								output_file_descriptor.write(triple)
								g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[object]: ""}})
								i += 1
							elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
								output_file_descriptor.write(triple)
								g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[object]: ""})
								i += 1
						else:
							if dic_table[predicate] not in g_triples:					
								output_file_descriptor.write(triple)
								g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[object]: ""}})
								i += 1
							elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
								output_file_descriptor.write(triple)
								g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[object]: ""})
								i += 1 
					else:
						output_file_descriptor.write(triple)
						i += 1
				if predicate[1:-1] in predicate_object_map.graph:
					triple = subject + " " + predicate + " " + object + ".\n"
					if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in predicate_object_map.graph[predicate[1:-1]]:
						if "{" in predicate_object_map.graph[predicate[1:-1]]:
							triple = triple[:-2] + " <" + string_substitution_json(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", data, "subject",ignore, iterator) + ">.\n"
							dictionary_table_update("<" + string_substitution_json(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", data, "subject",ignore, iterator) + ">")
						else:
							triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
							dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
						if duplicate == "yes":
							if predicate in general_predicates:
								if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
									output_file_descriptor.write(triple)
									g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[object]: ""}})
									i += 1
								elif dic_table[subject] + "_" + dic_table[object] not in g_triples[predicate + "_" + predicate_object_map.object_map.value]:
									output_file_descriptor.write(triple)
									g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[object]: ""})
									i += 1
							else:
								if dic_table[predicate] not in g_triples:					
									output_file_descriptor.write(triple)
									g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[object]: ""}})
									i += 1
								elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
									output_file_descriptor.write(triple)
									g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[object]: ""})
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
								triple = triple[:-2] + " <" + string_substitution_json(graph, "{(.+?)}", data, "subject",ignore, iterator) + ">.\n"
								dictionary_table_update("<" + string_substitution_json(graph, "{(.+?)}", data, "subject",ignore, iterator) + ">")
							else:
								triple = triple[:-2] + " <" + graph + ">.\n"
								dictionary_table_update("<" + graph + ">")
						if duplicate == "yes":
							if predicate in general_predicates:
								if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
									output_file_descriptor.write(triple)
									g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
									i += 1
								elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
									output_file_descriptor.write(triple)
									g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
									i += 1
							else:
								if dic_table[predicate] not in g_triples:
									output_file_descriptor.write(triple)
									g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
									i += 1
								elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
									output_file_descriptor.write(triple)
									g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
									i += 1
						else:
							output_file_descriptor.write(triple)
							i += 1

					if predicate[1:-1] in predicate_object_map.graph:
						triple = subject + " " + predicate + " " + obj + ".\n"
						if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in predicate_object_map.graph[predicate[1:-1]]:
							if "{" in predicate_object_map.graph[predicate[1:-1]]:
								triple = triple[:-2] + " <" + string_substitution_json(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", data, "subject",ignore, iterator) + ">.\n"
								dictionary_table_update("<" + string_substitution_json(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", data, "subject",ignore, iterator) + ">")
							else:
								triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
								dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
							if duplicate == "yes":
								if predicate in general_predicates:
									if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
										output_file_descriptor.write(triple)
										g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
										i += 1
									elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
										output_file_descriptor.write(triple)
										g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
										i += 1
								else:
									if dic_table[predicate] not in g_triples:					
										output_file_descriptor.write(triple)
										g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
										i += 1
									elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
										output_file_descriptor.write(triple)
										g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
										i += 1
							else:
								output_file_descriptor.write(triple)
								i += 1
				object_list = []
			else:
				continue
	return i


def semantify_file(triples_map, triples_map_list, delimiter, output_file_descriptor, data):
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
	triples_string = ""
	end_turtle = ""
	i = 0
	no_update = True
	global blank_message
	print("TM:",triples_map.triples_map_name)
	for row in data:
		generated = 0
		duplicate_type = False
		if triples_map.subject_map.subject_mapping_type == "template":
			subject_value = string_substitution(triples_map.subject_map.value, "{(.+?)}", row, "subject", ignore, triples_map.iterator)	
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
						subject = "<" + subject_value  + ">"
					except:
						subject = None 
			else:
				if "IRI" in triples_map.subject_map.term_type:
					subject_value = string_substitution(triples_map.subject_map.value, "{(.+?)}", row, "subject", ignore, triples_map.iterator)
					if triples_map.subject_map.condition == "":

						try:
							if "http" not in subject_value:
								subject = "<" + base + subject_value + ">"
							else:
								subject = "<" + encode_char(subject_value) + ">"
						except:
							subject = None

					else:
					#	field, condition = condition_separetor(triples_map.subject_map.condition)
					#	if row[field] == condition:
						try:
							if "http" not in subject_value:
								subject = subject = "<" + base + subject_value + ">"
							else:
								subject = "<" + subject_value + ">"
						except:
							subject = None 

				elif "BlankNode" in triples_map.subject_map.term_type:
					if triples_map.subject_map.condition == "":
						try:
							if "/" in subject_value:
								subject  = "_:" + encode_char(subject_value.replace("/","2F")).replace("%","")
								if "." in subject:
									subject = subject.replace(".","2E")
								if blank_message:
									print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
									blank_message = False
							else:
								subject = "_:" + encode_char(subject_value).replace("%","")
								if "." in subject:
									subject = subject.replace(".","2E")
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
			subject_value = string_substitution(triples_map.subject_map.value, ".+", row, "subject",ignore , triples_map.iterator)
			if subject_value != None:
				subject_value = subject_value[1:-1]
				if triples_map.subject_map.condition == "":
					if " " not in subject_value:
						if "http" not in subject_value:
							subject = "<" + base + subject_value + ">"
						else:
							subject = "<" + subject_value + ">"
					else:
						subject = None

			else:
			#	field, condition = condition_separetor(triples_map.subject_map.condition)
			#	if row[field] == condition:
				try:
					if "http" not in subject_value:
						subject = "<" + base + subject_value + ">"
					else:
						subject = "<" + subject_value + ">"
				except:
					subject = None

		elif "constant" in triples_map.subject_map.subject_mapping_type:
			subject = "<" + subject_value + ">"

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


		if triples_map.subject_map.rdf_class != None and subject != None:
			predicate = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
			for rdf_class in triples_map.subject_map.rdf_class:
				if rdf_class != None and  "str" == type(rdf_class).__name__:
					obj = "<{}>".format(rdf_class)
					rdf_type = subject + " " + predicate + " " + obj + ".\n"
					for graph in triples_map.subject_map.graph:
						if graph != None and "defaultGraph" not in graph:
							if "{" in graph:	
								rdf_type = rdf_type[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + "> .\n"
								dictionary_table_update("<" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">")
							else:
								rdf_type = rdf_type[:-2] + " <" + graph + "> .\n"
								dictionary_table_update("<" + graph + ">")
					if duplicate == "yes":
						dictionary_table_update(subject)
						dictionary_table_update(obj)
						dictionary_table_update(predicate + "_" + obj)
						if dic_table[predicate + "_" + obj] not in g_triples:
							if output_format.lower() == "n-triples":
								output_file_descriptor.write(rdf_type)
							else:
								output_file_descriptor.write(subject + " a " + determine_prefix(obj))
							g_triples.update({dic_table[predicate  + "_" + obj ] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
							i += 1
						elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + obj]]:
							if output_format.lower() == "n-triples":
								output_file_descriptor.write(rdf_type)
							else:
								output_file_descriptor.write(subject + " a " + determine_prefix(obj))
							g_triples[dic_table[predicate + "_" + obj]].update({dic_table[subject] + "_" + dic_table[obj] : ""})
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
			if predicate_object_map.predicate_map.mapping_type == "constant" or predicate_object_map.predicate_map.mapping_type == "constant shortcut":
				if output_format.lower() == "n-triples":
					predicate = "<" + predicate_object_map.predicate_map.value + ">"
				else:
					predicate = determine_prefix(predicate_object_map.predicate_map.value)
			elif predicate_object_map.predicate_map.mapping_type == "template":
				if predicate_object_map.predicate_map.condition != "":
						#field, condition = condition_separetor(predicate_object_map.predicate_map.condition)
						#if row[field] == condition:
						try:
							predicate = "<" + string_substitution(predicate_object_map.predicate_map.value, "{(.+?)}", row, "predicate",ignore, triples_map.iterator) + ">"
						except:
							predicate = None
						#else:
						#	predicate = None
				else:
					try:
						predicate = "<" + string_substitution(predicate_object_map.predicate_map.value, "{(.+?)}", row, "predicate",ignore, triples_map.iterator) + ">"
					except:
						predicate = None
			elif predicate_object_map.predicate_map.mapping_type == "reference":
				if predicate_object_map.predicate_map.condition != "":
					#field, condition = condition_separetor(predicate_object_map.predicate_map.condition)
					#if row[field] == condition:
					predicate = string_substitution(predicate_object_map.predicate_map.value, ".+", row, "predicate",ignore, triples_map.iterator)
					#else:
					#	predicate = None
				else:
					predicate = string_substitution(predicate_object_map.predicate_map.value, ".+", row, "predicate",ignore, triples_map.iterator)
				predicate = "<" + predicate[1:-1] + ">"
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
						object = "\"" + object[1:-1] + "\"" + "^^{}".format(determine_prefix(predicate_object_map.object_map.datatype))
			elif predicate_object_map.object_map.mapping_type == "template":
				try:
					if predicate_object_map.object_map.term is None:
						object = "<" + string_substitution(predicate_object_map.object_map.value, "{(.+?)}", row, "object",ignore, triples_map.iterator) + ">"
					elif "IRI" in predicate_object_map.object_map.term:
						object = "<" + string_substitution(predicate_object_map.object_map.value, "{(.+?)}", row, "object",ignore, triples_map.iterator) + ">"
					elif "BlankNode" in predicate_object_map.object_map.term:
						object = "_:" + string_substitution(predicate_object_map.object_map.value, "{(.+?)}", row, "object",ignore, triples_map.iterator)
						if "/" in object:
							object  = object.replace("/","2F")
							if blank_message:
								print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
								blank_message = False
						if "." in object:
							object = object.replace(".","2E")
						object = encode_char(object)
					else:
						object = "\"" + string_substitution(predicate_object_map.object_map.value, "{(.+?)}", row, "object",ignore, triples_map.iterator) + "\""
						if predicate_object_map.object_map.datatype != None:
							object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
						elif predicate_object_map.object_map.language != None:
							if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
								object += "@es"
							elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
								object += "@en"
							elif len(predicate_object_map.object_map.language) == 2:
								object += "@"+predicate_object_map.object_map.language
						elif predicate_object_map.object_map.language_map != None:
							lang = string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object",ignore, triples_map.iterator)
							if lang != None:
								object += "@" + string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object",ignore, triples_map.iterator)[1:-1]  
				except TypeError:
					object = None
			elif predicate_object_map.object_map.mapping_type == "reference":
				object = string_substitution(predicate_object_map.object_map.value, ".+", row, "object",ignore, triples_map.iterator)
				if object != None:
					if "\\" in object[1:-1]:
						object = "\"" + object[1:-1].replace("\\","\\\\") + "\""
					if "'" in object[1:-1]:
						object = "\"" + object[1:-1].replace("'","\\\\'") + "\""
					if "\n" in object:
						object = object.replace("\n","\\n")
					if predicate_object_map.object_map.datatype != None:
						if output_format.lower() == "n-triples":
							object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
						else:
							object = "\"" + object[1:-1] + "\"" + "^^{}".format(determine_prefix(predicate_object_map.object_map.datatype))
					elif predicate_object_map.object_map.language != None:
						if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
							object += "@es"
						elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
							object += "@en"
						elif len(predicate_object_map.object_map.language) == 2:
							object += "@"+predicate_object_map.object_map.language
					elif predicate_object_map.object_map.language_map != None:
						lang = string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object",ignore, triples_map.iterator)
						if lang != None:
							object += "@"+ string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object",ignore, triples_map.iterator)[1:-1]
					elif predicate_object_map.object_map.term != None:
						if "IRI" in predicate_object_map.object_map.term:
							if " " not in object:
								object = "\"" + object[1:-1].replace("\\\\'","'") + "\""
								object = "<" + encode_char(object[1:-1]) + ">"
							else:
								object = None
			elif predicate_object_map.object_map.mapping_type == "parent triples map":
				if subject != None:
					for triples_map_element in triples_map_list:
						if triples_map_element.triples_map_id == predicate_object_map.object_map.value:
							if triples_map_element.data_source != triples_map.data_source:
								if len(predicate_object_map.object_map.child) == 1:
									if (triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]) not in join_table:
										if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
											with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
												if str(triples_map_element.file_format).lower() == "csv":
													reader = pd.read_csv(str(triples_map_element.data_source), dtype = str)#, encoding = "ISO-8859-1")
													reader = reader.where(pd.notnull(reader), None)
													reader = reader.drop_duplicates(keep ='first')
													data = reader.to_dict(orient='records')
													hash_maker(data, triples_map_element, predicate_object_map.object_map)
												else:
													data = json.load(input_file_descriptor)
													if triples_map_element.iterator:
														if triples_map_element.iterator != "None" and triples_map_element.iterator != "$.[*]":
															join_iterator(data, triples_map_element.iterator, triples_map_element, predicate_object_map.object_map)
														else:
															if isinstance(data, list):
																hash_maker(data, triples_map_element, predicate_object_map.object_map)
															elif len(data) < 2:
																hash_maker(data[list(data.keys())[0]], triples_map_element, predicate_object_map.object_map)
													else:
														if isinstance(data, list):
															hash_maker(data, triples_map_element, predicate_object_map.object_map)
														elif len(data) < 2:
															hash_maker(data[list(data.keys())[0]], triples_map_element, predicate_object_map.object_map)

										elif triples_map_element.file_format == "XPath":
											with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
												child_tree = ET.parse(input_file_descriptor)
												child_root = child_tree.getroot()
												hash_maker_xml(child_root, triples_map_element, predicate_object_map.object_map)								
										else:
											database, query_list = translate_sql(triples_map)
											db = connector.connect(host=host, port=int(port), user=user, password=password)
											cursor = db.cursor(buffered=True)
											cursor.execute("use " + database)
											for query in query_list:
												cursor.execute(query)
											hash_maker_array(cursor, triples_map_element, predicate_object_map.object_map)

									if sublist(predicate_object_map.object_map.child,row.keys()):
										if child_list_value(predicate_object_map.object_map.child,row) in join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)]:
											object_list = join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)][child_list_value(predicate_object_map.object_map.child,row)]
										else:
											if no_update:
												if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
													with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
														if str(triples_map_element.file_format).lower() == "csv":
															reader = pd.read_csv(str(triples_map_element.data_source), dtype = str)#, encoding = "ISO-8859-1")
															reader = reader.where(pd.notnull(reader), None)
															reader = reader.drop_duplicates(keep ='first')
															data = reader.to_dict(orient='records')
															hash_update(data, triples_map_element, predicate_object_map.object_map, triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0])
														else:
															data = json.load(input_file_descriptor)
															if triples_map_element.iterator:
																if triples_map_element.iterator != "None" and triples_map_element.iterator != "$.[*]":
																	join_iterator(data, triples_map_element.iterator, triples_map_element, predicate_object_map.object_map)
																else:
																	if isinstance(data, list):
																		hash_maker(data, triples_map_element, predicate_object_map.object_map)
																	elif len(data) < 2:
																		hash_maker(data[list(data.keys())[0]], triples_map_element, predicate_object_map.object_map)
															else:
																if isinstance(data, list):
																	hash_maker(data, triples_map_element, predicate_object_map.object_map)
																elif len(data) < 2:
																	hash_maker(data[list(data.keys())[0]], triples_map_element, predicate_object_map.object_map)
												if child_list_value(predicate_object_map.object_map.child,row) in join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]]:
													object_list = join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]][row[predicate_object_map.object_map.child[0]]]
												else:
													object_list = []
												no_update = False
									object = None
								else:
									if (triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)) not in join_table:
										if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
											with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
												if str(triples_map_element.file_format).lower() == "csv":
													data = csv.DictReader(input_file_descriptor, delimiter=delimiter)
													hash_maker_list(data, triples_map_element, predicate_object_map.object_map)
												else:
													data = json.load(input_file_descriptor)
													if isinstance(data, list):
														hash_maker_list(data, triples_map_element, predicate_object_map.object_map)
													elif len(data) < 2:
														hash_maker_list(data[list(data.keys())[0]], triples_map_element, predicate_object_map.object_map)

										elif triples_map_element.file_format == "XPath":
											with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
												child_tree = ET.parse(input_file_descriptor)
												child_root = child_tree.getroot()
												hash_maker_xml(child_root, triples_map_element, predicate_object_map.object_map)						
										else:
											database, query_list = translate_sql(triples_map)
											db = connector.connect(host=host, port=int(port), user=user, password=password)
											cursor = db.cursor(buffered=True)
											cursor.execute("use " + database)
											for query in query_list:
												cursor.execute(query)
											hash_maker_array(cursor, triples_map_element, predicate_object_map.object_map)
									if sublist(predicate_object_map.object_map.child,row.keys()):
										if child_list_value(predicate_object_map.object_map.child,row) in join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)]:
											object_list = join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)][child_list_value(predicate_object_map.object_map.child,row)]
										else:
											object_list = []
									object = None
							else:
								if predicate_object_map.object_map.parent != None:
									if predicate_object_map.object_map.parent[0] != predicate_object_map.object_map.child[0]:
										if (triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)) not in join_table:
											with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
												if str(triples_map_element.file_format).lower() == "csv":
													parent_data = csv.DictReader(input_file_descriptor, delimiter=delimiter)
													hash_maker_list(parent_data, triples_map_element, predicate_object_map.object_map)
												else:
													parent_data = json.load(input_file_descriptor)
													if isinstance(parent_data, list):
														hash_maker_list(parent_data, triples_map_element, predicate_object_map.object_map)
													else:
														hash_maker_list(parent_data[list(parent_data.keys())[0]], triples_map_element, predicate_object_map.object_map)
										if sublist(predicate_object_map.object_map.child,row.keys()):
											if child_list_value(predicate_object_map.object_map.child,row) in join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)]:
												object_list = join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)][child_list_value(predicate_object_map.object_map.child,row)]
											else:
												object_list = []
										object = None
									else:
										try:
											object = "<" + string_substitution(triples_map_element.subject_map.value, "{(.+?)}", row, "object",ignore, triples_map.iterator) + ">"
										except TypeError:
											object = None
								else:
									try:
										object = "<" + string_substitution(triples_map_element.subject_map.value, "{(.+?)}", row, "object",ignore, triples_map.iterator) + ">"
									except TypeError:
										object = None
							break
						else:
							continue
				else:
					object = None
			else:
				object = None

			if duplicate == "yes":
				if predicate in general_predicates:
					dictionary_table_update(predicate + "_" + predicate_object_map.object_map.value)
				else:
					dictionary_table_update(predicate)
			
			if output_format.lower() == "turtle" and triples_map.predicate_object_maps_list[0] == predicate_object_map and not duplicate_type:
				if len(triples_map.predicate_object_maps_list) > 1:
					output_file_descriptor.write(";\n")
				elif len(triples_map.predicate_object_maps_list) == 1:
					if object == None:
						output_file_descriptor.write(".\n")
					else:
						output_file_descriptor.write(";\n")
				elif len(triples_map.predicate_object_maps_list) == 0:
					output_file_descriptor.write(".\n")					

			if end_turtle == ";":
				if predicate != None and object != None and subject != None:
					output_file_descriptor.write(";\n")
				elif predicate != None and subject != None and object_list:
					output_file_descriptor.write(";\n")
				else:
					if predicate_object_map == triples_map.predicate_object_maps_list[len(triples_map.predicate_object_maps_list)-1]:
						output_file_descriptor.write(".\n\n")
						end_turtle = "."


			if predicate != None and object != None and subject != None:
				for graph in triples_map.subject_map.graph:
					triple = subject + " " + predicate + " " + object + ".\n"
					if graph != None and "defaultGraph" not in graph:
						if "{" in graph:
							triple = triple[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">.\n"
							dictionary_table_update("<" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">")
						else:
							triple = triple[:-2] + " <" + graph + ">.\n"
							dictionary_table_update("<" + graph + ">")

					if duplicate == "yes":
						dictionary_table_update(subject)
						dictionary_table_update(object)
						if predicate in general_predicates:
							if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
								if output_format.lower() == "n-triples":
									output_file_descriptor.write(triple)
								else:
									end_turtle = turtle_print(subject, predicate, object, object_list, duplicate_type, predicate_object_map, triples_map, output_file_descriptor, generated)
								g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[object]: ""}})
								i += 1
								generated += 1
							elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
								if output_format.lower() == "n-triples":
									output_file_descriptor.write(triple)
								else:
									end_turtle = turtle_print(subject, predicate, object, object_list, duplicate_type, predicate_object_map, triples_map, output_file_descriptor, generated)
								g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[object]: ""})
								i += 1
								generated += 1
						else:
							if dic_table[predicate] not in g_triples:					
								if output_format.lower() == "n-triples":
									output_file_descriptor.write(triple)
								else:
									end_turtle = turtle_print(subject, predicate, object, object_list, duplicate_type, predicate_object_map, triples_map, output_file_descriptor, generated)
								g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[object]: ""}})
								i += 1
								generated += 1
							elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
								if output_format.lower() == "n-triples":
									output_file_descriptor.write(triple)
								else:
									end_turtle = turtle_print(subject, predicate, object, object_list, duplicate_type, predicate_object_map, triples_map, output_file_descriptor, generated)
								g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[object]: ""})
								i += 1
								generated += 1 
					else:
						if output_format.lower() == "n-triples":
							output_file_descriptor.write(triple)
						else:
							end_turtle = turtle_print(subject, predicate, object, object_list, duplicate_type, predicate_object_map, triples_map, output_file_descriptor, generated)
						i += 1
						generated += 1
				if predicate[1:-1] in predicate_object_map.graph:
					triple = subject + " " + predicate + " " + object + ".\n"
					if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in predicate_object_map.graph[predicate[1:-1]]:
						if "{" in predicate_object_map.graph[predicate[1:-1]]:
							triple = triple[:-2] + " <" + string_substitution(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">.\n"
							dictionary_table_update("<" + string_substitution(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">")
						else:
							triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
							dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
						if duplicate == "yes":
							if predicate in general_predicates:
								if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
									if output_format.lower() == "n-triples":
										output_file_descriptor.write(triple)
									else:
										end_turtle = turtle_print(subject, predicate, object, object_list, duplicate_type, predicate_object_map, triples_map, output_file_descriptor, generated)
									g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[object]: ""}})
									i += 1
									generated += 1
								elif dic_table[subject] + "_" + dic_table[object] not in g_triples[predicate + "_" + predicate_object_map.object_map.value]:
									if output_format.lower() == "n-triples":
										output_file_descriptor.write(triple)
									else:
										end_turtle = turtle_print(subject, predicate, object, object_list, duplicate_type, predicate_object_map, triples_map, output_file_descriptor, generated)
									g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[object]: ""})
									i += 1
									generated += 1
							else:
								if dic_table[predicate] not in g_triples:					
									if output_format.lower() == "n-triples":
										output_file_descriptor.write(triple)
									else:
										end_turtle = turtle_print(subject, predicate, object, object_list, duplicate_type, predicate_object_map, triples_map, output_file_descriptor, generated)
									g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[object]: ""}})
									i += 1
									generated += 1
								elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
									if output_format.lower() == "n-triples":
										output_file_descriptor.write(triple)
									else:
										end_turtle = turtle_print(subject, predicate, object, object_list, duplicate_type, predicate_object_map, triples_map, output_file_descriptor, generated)
									g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[object]: ""})
									i += 1
									generated += 1
						else:
							if output_format.lower() == "n-triples":
								output_file_descriptor.write(triple)
							else:
								end_turtle = turtle_print(subject, predicate, object, object_list, duplicate_type, predicate_object_map, triples_map, output_file_descriptor, generated)
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
								triple = subject + " " + predicate + " " + obj + ".\n"
							if graph != None and "defaultGraph" not in graph:
								if "{" in graph:
									triple = triple[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">.\n"
									dictionary_table_update("<" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">")
								else:
									triple = triple[:-2] + " <" + graph + ">.\n"
									dictionary_table_update("<" + graph + ">")
							if duplicate == "yes":
								dictionary_table_update(subject)
								dictionary_table_update(obj)	
								if predicate in general_predicates:
									if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
										if output_format.lower() == "n-triples":
											output_file_descriptor.write(triple)
										else:
											end_turtle = turtle_print(subject, predicate, obj, object_list, duplicate_type, predicate_object_map, triples_map, output_file_descriptor, generated)
										g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
										i += 1
										generated += 1
									elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
										if output_format.lower() == "n-triples":
											output_file_descriptor.write(triple)
										else:
											end_turtle = turtle_print(subject, predicate, obj, object_list, duplicate_type, predicate_object_map, triples_map, output_file_descriptor, generated)
										g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
										i += 1
										generated += 1
								else:
									if dic_table[predicate] not in g_triples:
										if output_format.lower() == "n-triples":
											output_file_descriptor.write(triple)
										else:
											end_turtle = turtle_print(subject, predicate, obj, object_list, duplicate_type, predicate_object_map, triples_map, output_file_descriptor, generated)
										g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
										i += 1
										generated += 1
									elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
										if output_format.lower() == "n-triples":
											output_file_descriptor.write(triple)
										else:
											end_turtle = turtle_print(subject, predicate, obj, object_list, duplicate_type, predicate_object_map, triples_map, output_file_descriptor, generated)
										g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
										i += 1
										generated += 1

							else:
								if output_format.lower() == "n-triples":
									output_file_descriptor.write(triple)
								else:
									end_turtle = turtle_print(subject, predicate, obj, object_list, duplicate_type, predicate_object_map, triples_map, output_file_descriptor, generated)
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
							if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in predicate_object_map.graph[predicate[1:-1]]:
								if "{" in predicate_object_map.graph[predicate[1:-1]]:
									triple = triple[:-2] + " <" + string_substitution(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">.\n"
									dictionary_table_update("<" + string_substitution(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">")
								else:
									triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
									dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
								if duplicate == "yes":
									if predicate in general_predicates:
										if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
											if output_format.lower() == "n-triples":
												output_file_descriptor.write(triple)
											else:
												end_turtle = turtle_print(subject, predicate, obj, object_list, duplicate_type, predicate_object_map, triples_map, output_file_descriptor, generated)
											g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
											i += 1
											generated += 1
										elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
											if output_format.lower() == "n-triples":
												output_file_descriptor.write(triple)
											else:
												end_turtle = turtle_print(subject, predicate, obj, object_list, duplicate_type, predicate_object_map, triples_map, output_file_descriptor, generated)
											g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
											i += 1
											generated += 1
									else:
										if dic_table[predicate] not in g_triples:					
											if output_format.lower() == "n-triples":
												output_file_descriptor.write(triple)
											else:
												end_turtle = turtle_print(subject, predicate, obj, object_list, duplicate_type, predicate_object_map, triples_map, output_file_descriptor, generated)
											g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
											i += 1
											generated += 1
										elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
											if output_format.lower() == "n-triples":
												output_file_descriptor.write(triple)
											else:
												end_turtle = turtle_print(subject, predicate, obj, object_list, duplicate_type, predicate_object_map, triples_map, output_file_descriptor, generated)
											g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
											i += 1
											generated += 1
								else:
									if output_format.lower() == "n-triples":
										output_file_descriptor.write(triple)
									else:
										end_turtle = turtle_print(subject, predicate, obj, object_list, duplicate_type, predicate_object_map, triples_map, output_file_descriptor, generated)
									i += 1
									generated += 1
				object_list = []
			else:
				continue
	return i

def semantify_mysql(row, row_headers, triples_map, triples_map_list, output_file_descriptor, host, port, user, password,dbase):

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
	triples_map_triples = {}
	generated_triples = {}
	object_list = []
	subject_value = string_substitution_array(triples_map.subject_map.value, "{(.+?)}", row, row_headers, "subject",ignore)
	i = 0

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
					subject = "<" + subject_value  + ">"
				except:
					subject = None
		else:
			if "IRI" in triples_map.subject_map.term_type:
				if triples_map.subject_map.condition == "":

					try:
						if "http" not in subject_value:
							subject = "<" + base + subject_value + ">"
						else:
							subject = "<" + encode_char(subject_value) + ">"
					except:
						subject = None

				else:
				#	field, condition = condition_separetor(triples_map.subject_map.condition)
				#	if row[field] == condition:
					try:
						if "http" not in subject_value:
							subject = "<" + base + subject_value + ">"
						else:
							subject = "<" + subject_value + ">"
					except:
						subject = None
				
			elif "BlankNode" in triples_map.subject_map.term_type:
				if triples_map.subject_map.condition == "":

					try:
						if "/" in subject_value:
							subject  = "_:" + encode_char(subject_value.replace("/","2F")).replace("%","")
							if "." in subject:
								subject = subject.replace(".","2E")
							if blank_message:
								print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
								blank_message = False
						else:
							subject = "_:" + encode_char(subject_value).replace("%","")
							if "." in subject:
								subject = subject.replace(".","2E")  
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
			subject_value = string_substitution_array(triples_map.subject_map.value, ".+", row, row_headers, "subject",ignore)
			subject_value = subject_value[1:-1]
			try:
				if " " not in subject_value:
					if "http" not in subject_value:
						subject = "<" + base + subject_value + ">"
					else:
						subject = "<" + subject_value + ">"
				else:
					print("<http://example.com/base/" + subject_value + "> is an invalid URL")
					subject = None 
			except:
				subject = None
			if triples_map.subject_map.term_type == "IRI":
				if " " not in subject_value:
					subject = "<" + subject_value + ">"
				else:
					subject = None

		else:
		#	field, condition = condition_separetor(triples_map.subject_map.condition)
		#	if row[field] == condition:
			try:
				if "http" not in subject_value:
					subject = "<" + base + subject_value + ">"
				else:
					subject = "<" + subject_value + ">"
			except:
				subject = None

	elif "constant" in triples_map.subject_map.subject_mapping_type:
		subject = "<" + subject_value + ">"

	else:
		if triples_map.subject_map.condition == "":

			try:
				subject =  "\"" + triples_map.subject_map.value + "\""
			except:
				subject = None

		else:
		#	field, condition = condition_separetor(triples_map.subject_map.condition)
		#	if row[field] == condition:
			try:
				subject =  "\"" + triples_map.subject_map.value + "\""
			except:
				subject = None

	if triples_map.subject_map.rdf_class != None and subject != None:
		predicate = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
		for rdf_class in triples_map.subject_map.rdf_class:
			if rdf_class != None:
				obj = "<{}>".format(rdf_class)
				dictionary_table_update(subject)
				dictionary_table_update(obj)
				dictionary_table_update(predicate + "_" + obj)
				rdf_type = subject + " " + predicate + " " + obj +" .\n"
				for graph in triples_map.subject_map.graph:
					if graph != None and "defaultGraph" not in graph:
						if "{" in graph:	
							rdf_type = rdf_type[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + "> .\n"
							dictionary_table_update("<" + string_substitution(graph, "{(.+?)}", row, "subject",ignore, triples_map.iterator) + ">")
						else:
							rdf_type = rdf_type[:-2] + " <" + graph + "> .\n"
							dictionary_table_update("<" + graph + ">")
					if duplicate == "yes":
						if dic_table[predicate + "_" + obj] not in g_triples:
							output_file_descriptor.write(rdf_type)
							g_triples.update({dic_table[predicate  + "_" + obj ] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
							i += 1
						elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + obj]]:
							output_file_descriptor.write(rdf_type)
							g_triples[dic_table[predicate + "_" + obj]].update({dic_table[subject] + "_" + dic_table[obj] : ""})
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
					predicate = "<" + string_substitution_array(predicate_object_map.predicate_map.value, "{(.+?)}", row, row_headers, "predicate",ignore) + ">"
				except:
					predicate = None
			else:
				try:
					predicate = "<" + string_substitution_array(predicate_object_map.predicate_map.value, "{(.+?)}", row, row_headers, "predicate",ignore) + ">"
				except:
					predicate = None
		elif predicate_object_map.predicate_map.mapping_type == "reference":
			predicate = string_substitution_array(predicate_object_map.predicate_map.value, ".+", row, row_headers, "predicate",ignore)
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
		elif predicate_object_map.object_map.mapping_type == "template":
			try:
				if predicate_object_map.object_map.term is None:
					object = "<" + string_substitution_array(predicate_object_map.object_map.value, "{(.+?)}", row, row_headers, "object",ignore) + ">"
				elif "IRI" in predicate_object_map.object_map.term:
					object = "<" + string_substitution_array(predicate_object_map.object_map.value, "{(.+?)}", row, row_headers, "object",ignore) + ">"
				elif "BlankNode" in predicate_object_map.object_map.term:
					object = "_:" + string_substitution_array(predicate_object_map.object_map.value, "{(.+?)}", row, row_headers, "object",ignore)
					if "/" in object:
						object  = object.replace("/","2F")
						if blank_message:
							print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
							blank_message = False
					if "." in object:
						object = object.replace(".","2E")
					object = encode_char(object)
				else:
					object = "\"" + string_substitution_array(predicate_object_map.object_map.value, "{(.+?)}", row, row_headers, "object",ignore) + "\""
					if predicate_object_map.object_map.datatype != None:
						object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
					elif predicate_object_map.object_map.language != None:
						if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
							object += "@es"
						elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
							object += "@en"
						elif len(predicate_object_map.object_map.language) == 2:
							object += "@"+predicate_object_map.object_map.language
					elif predicate_object_map.object_map.language_map != None:
						lang = string_substitution_array(predicate_object_map.object_map.language_map, ".+", row, row_headers, "object",ignore)
						if lang != None:
							object += "@" + string_substitution_array(predicate_object_map.object_map.language_map, ".+", row, row_headers, "object",ignore)[1:-1]
			except TypeError:
				object = None
		elif predicate_object_map.object_map.mapping_type == "reference":
			object = string_substitution_array(predicate_object_map.object_map.value, ".+", row, row_headers, "object",ignore)
			if object != None:
				if "\\" in object[1:-1]:
					object = "\"" + object[1:-1].replace("\\","\\\\") + "\""
				if "'" in object[1:-1]:
					object = "\"" + object[1:-1].replace("'","\\\\'") + "\""
				if "\n" in object:
					object = object.replace("\n","\\n")
				if predicate_object_map.object_map.datatype != None:
					object += "^^<{}>".format(predicate_object_map.object_map.datatype)
				elif predicate_object_map.object_map.language != None:
					if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
						object += "@es"
					elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
						object += "@en"
					elif len(predicate_object_map.object_map.language) == 2:
						object += "@"+predicate_object_map.object_map.language
				elif predicate_object_map.object_map.language_map != None:
					lang = string_substitution_array(predicate_object_map.object_map.language_map, ".+", row, row_headers, "object",ignore)
					if lang != None:
						object += "@"+ string_substitution_array(predicate_object_map.object_map.language_map, ".+", row, row_headers, "object",ignore)[1:-1]
				elif predicate_object_map.object_map.term != None:
					if "IRI" in predicate_object_map.object_map.term:
						if " " not in object:
							object = "\"" + object[1:-1].replace("\\\\'","'") + "\""
							object = "<" + encode_char(object[1:-1]) + ">"
						else:
							object = None

		elif predicate_object_map.object_map.mapping_type == "parent triples map":
			for triples_map_element in triples_map_list:
				if triples_map_element.triples_map_id == predicate_object_map.object_map.value:
					if (triples_map_element.data_source != triples_map.data_source) or (triples_map_element.tablename != triples_map.tablename) or (triples_map_element.query != triples_map.query):
						if len(predicate_object_map.object_map.child) == 1:
							if triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0] not in join_table:
								if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
									with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
										if str(triples_map_element.file_format).lower() == "csv":
											data = csv.DictReader(input_file_descriptor, delimiter=delimiter)
											hash_maker(data, triples_map_element, predicate_object_map.object_map)
										else:
											data = json.load(input_file_descriptor)
											hash_maker(data[list(data.keys())[0]], triples_map_element, predicate_object_map.object_map)
								elif triples_map_element.file_format == "XPath":
									with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
										child_tree = ET.parse(input_file_descriptor)
										child_root = child_tree.getroot()
										hash_maker_xml(child_root, triples_map_element, predicate_object_map.object_map)								
								else:
									database, query_list = translate_sql(triples_map_element)
									db = connector.connect(host = host, port = int(port), user = user, password = password)
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
							jt = join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]]
							if row[row_headers.index(predicate_object_map.object_map.child[0])] != None and row[row_headers.index(predicate_object_map.object_map.child[0])] in jt:
								object_list = jt[row[row_headers.index(predicate_object_map.object_map.child[0])]]
							object = None
						else:
							if (triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)) not in join_table:
								if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
									with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
										if str(triples_map_element.file_format).lower() == "csv":
											data = csv.DictReader(input_file_descriptor, delimiter=delimiter)
											hash_maker_list(data, triples_map_element, predicate_object_map.object_map)
										else:
											data = json.load(input_file_descriptor)
											if isinstance(data, list):
												hash_maker_list(data, triples_map_element, predicate_object_map.object_map)
											elif len(data) < 2:
												hash_maker_list(data[list(data.keys())[0]], triples_map_element, predicate_object_map.object_map)

								elif triples_map_element.file_format == "XPath":
									with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
										child_tree = ET.parse(input_file_descriptor)
										child_root = child_tree.getroot()
										hash_maker_xml(child_root, triples_map_element, predicate_object_map.object_map)						
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
									hash_maker_array_list(cursor, triples_map_element, predicate_object_map.object_map,row_headers)
							if sublist(predicate_object_map.object_map.child,row_headers):
								if child_list_value_array(predicate_object_map.object_map.child,row,row_headers) in join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)]:
									object_list = join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)][child_list_value_array(predicate_object_map.object_map.child,row,row_headers)]
								else:
									object_list = []
							object = None
					else:
						if predicate_object_map.object_map.parent != None:
							if len(predicate_object_map.object_map.parent) == 1:
								if triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0] not in join_table:
									database, query_list = translate_sql(triples_map_element)
									db = connector.connect(host = host, port = int(port), user = user, password = password)
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
								if (triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)) not in join_table:
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
									hash_maker_array_list(cursor, triples_map_element, predicate_object_map.object_map,row_headers)

							if sublist(predicate_object_map.object_map.child,row_headers):
								if child_list_value_array(predicate_object_map.object_map.child,row,row_headers) in join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)]:
									object_list = join_table[triples_map_element.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)][child_list_value_array(predicate_object_map.object_map.child,row,row_headers)]
								else:
									object_list = []
							object = None
						else: 
							try:
								object = "<" + string_substitution_array(triples_map_element.subject_map.value, "{(.+?)}", row, row_headers, "object",ignore) + ">"
							except TypeError:
								object = None
					break
				else:
					continue
		else:
			object = None

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
						triple = triple[:-2] + " <" + string_substitution_array(graph, "{(.+?)}", row, row_headers,"subject",ignore) + ">.\n"
						dictionary_table_update("<" + string_substitution_array(graph, "{(.+?)}", row, row_headers,"subject",ignore) + ">")
					else:
						triple = triple[:-2] + " <" + graph + ">.\n"
						dictionary_table_update("<" + graph + ">")
				if duplicate == "yes":
					if predicate in general_predicates:
						if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
							try:
								output_file_descriptor.write(triple)
							except:
								output_file_descriptor.write(triple.encode("ISO 8859-1"))
							g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[object]: ""}})
							i += 1
						elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
							try:
								output_file_descriptor.write(triple)
							except:
								output_file_descriptor.write(triple.encode("ISO 8859-1"))
							g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[object]: ""})
							i += 1
					else:
						if dic_table[predicate] not in g_triples:					
							try:
								output_file_descriptor.write(triple)
							except:
								output_file_descriptor.write(triple.encode("ISO 8859-1"))
							g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[object]: ""}})
							i += 1
						elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
							try:
								output_file_descriptor.write(triple)
							except:
								output_file_descriptor.write(triple.encode("ISO 8859-1"))
							g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[object]: ""})
							i += 1
				else:
					try:
						output_file_descriptor.write(triple)
					except:
						output_file_descriptor.write(triple.encode("ISO 8859-1"))
					i += 1
			if predicate[1:-1] in predicate_object_map.graph:
				triple = subject + " " + predicate + " " + object + ".\n"
				if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in predicate_object_map.graph[predicate[1:-1]]:
					if "{" in predicate_object_map.graph[predicate[1:-1]]:
						triple = triple[:-2] + " <" + string_substitution_array(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, row_headers,"subject",ignore) + ">.\n"
						dictionary_table_update("<" + string_substitution_array(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, row_headers,"subject",ignore) + ">")
					else:
						triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
						dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
					if duplicate == "yes":
						if predicate in general_predicates:
							if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
								try:
									output_file_descriptor.write(triple)
								except:
									output_file_descriptor.write(triple.encode("ISO 8859-1"))
								g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[object]: ""}})
								i += 1
							elif dic_table[subject] + "_" + dic_table[object] not in g_triples[predicate + "_" + predicate_object_map.object_map.value]:
								try:
									output_file_descriptor.write(triple)
								except:
									output_file_descriptor.write(triple.encode("ISO 8859-1"))
								g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[object]: ""})
								i += 1
						else:
							if dic_table[predicate] not in g_triples:					
								try:
									output_file_descriptor.write(triple)
								except:
									output_file_descriptor.write(triple.encode("ISO 8859-1"))
								g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[object]: ""}})
								i += 1
							elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
								try:
									output_file_descriptor.write(triple)
								except:
									output_file_descriptor.write(triple.encode("ISO 8859-1"))
								g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[object]: ""})
								i += 1
					else:
						try:
							output_file_descriptor.write(triple)
						except:
							output_file_descriptor.write(triple.encode("ISO 8859-1"))
						i += 1
		elif predicate != None and subject != None and object_list:
			dictionary_table_update(subject)
			for obj in object_list:
				dictionary_table_update(obj)
				triple = subject + " " + predicate + " " + obj + ".\n"
				for graph in triples_map.subject_map.graph:
					if graph != None and "defaultGraph" not in graph:
						if "{" in graph:
							triple = triple[:-2] + " <" + string_substitution_array(graph, "{(.+?)}", row, row_headers,"subject",ignore) + ">.\n"
							dictionary_table_update("<" + string_substitution_array(graph, "{(.+?)}", row, row_headers,"subject",ignore) + ">")
						else:
							triple = triple[:-2] + " <" + graph + ">.\n"
							dictionary_table_update("<" + graph + ">")
					if duplicate == "yes":
						if predicate in general_predicates:
							if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
								try:
									output_file_descriptor.write(triple)
								except:
									output_file_descriptor.write(triple.encode("ISO 8859-1"))
								g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
								i += 1
							elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
								try:
									output_file_descriptor.write(triple)
								except:
									output_file_descriptor.write(triple.encode("ISO 8859-1"))
								g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
								i += 1
						else:
							if dic_table[predicate] not in g_triples:
								try:
									output_file_descriptor.write(triple)
								except:
									output_file_descriptor.write(triple.encode("ISO 8859-1"))
								g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
								i += 1
							elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
								try:
									output_file_descriptor.write(triple)
								except:
									output_file_descriptor.write(triple.encode("ISO 8859-1"))
								g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
								i += 1
					else:
						try:
							output_file_descriptor.write(triple)
						except:
							output_file_descriptor.write(triple.encode("ISO 8859-1"))
						i += 1
				if predicate[1:-1] in predicate_object_map.graph:
					triple = subject + " " + predicate + " " + obj + ".\n"
					if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in predicate_object_map.graph[predicate[1:-1]]:
						if "{" in predicate_object_map.graph[predicate[1:-1]]:
							triple = triple[:-2] + " <" + string_substitution_array(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, row_headers,"subject",ignore) + ">.\n"
							dictionary_table_update("<" + string_substitution_array(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, row_headers,"subject",ignore) + ">")
						else:
							triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
							dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
						if duplicate == "yes":
							if predicate in general_predicates:
								if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
									try:
										output_file_descriptor.write(triple)
									except:
										output_file_descriptor.write(triple.encode("ISO 8859-1"))
									g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
									i += 1
								elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
									try:
										output_file_descriptor.write(triple)
									except:
										output_file_descriptor.write(triple.encode("ISO 8859-1"))
									g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
									i += 1
							else:
								if dic_table[predicate] not in g_triples:					
									try:
										output_file_descriptor.write(triple)
									except:
										output_file_descriptor.write(triple.encode("ISO 8859-1"))
									g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
									i += 1
								elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
									try:
										output_file_descriptor.write(triple)
									except:
										output_file_descriptor.write(triple.encode("ISO 8859-1"))
									g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
									i += 1
						else:
							try:
								output_file_descriptor.write(triple)
							except:
								output_file_descriptor.write(triple.encode("ISO 8859-1"))
							i += 1
			object_list = []
		else:
			continue
	return i

def semantify_postgres(row, row_headers, triples_map, triples_map_list, output_file_descriptor, user, password, db, host):

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
	subject_value = string_substitution_array(triples_map.subject_map.value, "{(.+?)}", row, row_headers, "subject",ignore)
	global blank_message
	i = 0
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
					subject = "<" + subject_value  + ">"
					 
				except:
					subject = None
		else:
			if "IRI" in triples_map.subject_map.term_type:
				if triples_map.subject_map.condition == "":

					try:
						if "http" not in subject_value:
							subject = "<" + base + subject_value + ">"
						else:
							subject = "<" + encode_char(subject_value) + ">"					 
					except:
						subject = None

				else:
				#	field, condition = condition_separetor(triples_map.subject_map.condition)
				#	if row[field] == condition:
					try:
						if "http" not in subject_value:
							subject = "<" + base + subject_value + ">"
						else:
							subject = "<" + subject_value + ">"
						 
					except:
						subject = None
				
			elif "BlankNode" in triples_map.subject_map.term_type:
				if triples_map.subject_map.condition == "":

					try:
						if "/" in subject_value:
							subject  = "_:" + encode_char(subject_value.replace("/","2F")).replace("%","")
							if "." in subject:
								subject = subject.replace(".","2E")
							if blank_message:
								print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
								blank_message = False
						else:
							subject = "_:" + encode_char(subject_value).replace("%","")
							if "." in subject:
								subject = subject.replace(".","2E") 
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
			subject_value = string_substitution_array(triples_map.subject_map.value, ".+", row, row_headers,"subject",ignore)
			subject_value = subject_value[1:-1]
			try:
				if " " not in subject_value:
					if "http" not in subject_value:
						subject = "<" + base + subject_value + ">"
					else:
						subject = "<" + subject_value + ">"
				else:
					print("<http://example.com/base/" + subject_value + "> is an invalid URL")
					subject = None 
			except:
				subject = None
			if triples_map.subject_map.term_type == "IRI":
				if " " not in subject_value:
					if "http" in subject_value:
						temp = encode_char(subject_value.replace("http:",""))
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
					subject = "<" + base + subject_value + ">"
				else:
					subject = "<" + encode_char(subject_value) + ">"
			except:
				subject = None
	
	elif "constant" in triples_map.subject_map.subject_mapping_type:
		subject = "<" + subject_value + ">"

	else:
		if triples_map.subject_map.condition == "":

			try:
				subject =  "\"" + triples_map.subject_map.value + "\""
			except:
				subject = None

		else:
		#	field, condition = condition_separetor(triples_map.subject_map.condition)
		#	if row[field] == condition:
			try:
				subject =  "\"" + triples_map.subject_map.value + "\""
			except:
				subject = None

	if triples_map.subject_map.rdf_class != None and subject != None:
		predicate = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
		for rdf_class in triples_map.subject_map.rdf_class:
			if rdf_class != None:
				obj = "<{}>".format(rdf_class)
				dictionary_table_update(subject)
				dictionary_table_update(obj)
				dictionary_table_update(predicate + "_" + obj)
				rdf_type = subject + " " + predicate + " " + obj + " .\n"
				for graph in triples_map.subject_map.graph:
					if graph != None and "defaultGraph" not in graph:
						if "{" in graph:	
							rdf_type = rdf_type[:-2] + " <" + string_substitution_array(graph, "{(.+?)}", row, row_headers,"subject",ignore) + "> .\n"
							dictionary_table_update("<" + string_substitution_array(graph, "{(.+?)}", row, row_headers,"subject",ignore) + ">")
						else:
							rdf_type = rdf_type[:-2] + " <" + graph + "> .\n"
							dictionary_table_update("<" + graph + ">")
					if duplicate == "yes":
						if dic_table[predicate + "_" + obj] not in g_triples:
							try:
								output_file_descriptor.write(rdf_type)
							except:
								output_file_descriptor.write(rdf_type.encode("ISO 8859-1"))
							g_triples.update({dic_table[predicate  + "_" + obj ] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
							i += 1
						elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + obj]]:
							try:
								output_file_descriptor.write(rdf_type)
							except:
								output_file_descriptor.write(rdf_type.encode("ISO 8859-1"))
							g_triples[dic_table[predicate + "_" + obj]].update({dic_table[subject] + "_" + dic_table[obj] : ""})
							i += 1
					else:
						try:
							output_file_descriptor.write(rdf_type)
						except:
							output_file_descriptor.write(rdf_type.encode("ISO 8859-1"))
						i += 1

	for predicate_object_map in triples_map.predicate_object_maps_list:
		if predicate_object_map.predicate_map.mapping_type == "constant" or predicate_object_map.predicate_map.mapping_type == "constant shortcut":
			predicate = "<" + predicate_object_map.predicate_map.value + ">"
		elif predicate_object_map.predicate_map.mapping_type == "template":
			if predicate_object_map.predicate_map.condition != "":
				try:
					predicate = "<" + string_substitution_postgres(predicate_object_map.predicate_map.value, "{(.+?)}", row, row_headers, "predicate",ignore) + ">"
				except:
					predicate = None
			else:
				try:
					predicate = "<" + string_substitution_postgres(predicate_object_map.predicate_map.value, "{(.+?)}", row, row_headers, "predicate",ignore) + ">"
				except:
					predicate = None
		elif predicate_object_map.predicate_map.mapping_type == "reference":
			predicate = string_substitution_postgres(predicate_object_map.predicate_map.value, ".+", row, row_headers, "predicate",ignore)
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
		elif predicate_object_map.object_map.mapping_type == "template":
			try:
				if predicate_object_map.object_map.term is None:
					object = "<" + string_substitution_postgres(predicate_object_map.object_map.value, "{(.+?)}", row, row_headers, "object",ignore) + ">"
				elif "IRI" in predicate_object_map.object_map.term:
					object = "<" + string_substitution_postgres(predicate_object_map.object_map.value, "{(.+?)}", row, row_headers, "object",ignore) + ">"
				elif "BlankNode" in predicate_object_map.object_map.term:
					object = "_:" + string_substitution_postgres(predicate_object_map.object_map.value, "{(.+?)}", row, row_headers, "object",ignore)
					if "/" in object:
						object  = encode_char(object.replace("/","2F")).replace("%","")
						if blank_message:
							print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
							blank_message = False
					if "." in object:
						object = object.replace(".","2E")
					object = encode_char(object)
				else:
					object = "\"" + string_substitution_postgres(predicate_object_map.object_map.value, "{(.+?)}", row, row_headers, "object",ignore) + "\""
					if predicate_object_map.object_map.datatype != None:
						object = "\"" + object[1:-1] + "\"" + "^^<{}>".format(predicate_object_map.object_map.datatype)
					elif predicate_object_map.object_map.language != None:
						if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
							object += "@es"
						elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
							object += "@en"
						elif len(predicate_object_map.object_map.language) == 2:
							object += "@"+predicate_object_map.object_map.language
					elif predicate_object_map.object_map.language_map != None:
						lang = string_substitution_postgres(predicate_object_map.object_map.language_map, ".+", row, row_headers, "object",ignore)
						if lang != None:
							object += "@" + string_substitution_postgres(predicate_object_map.object_map.language_map, ".+", row, row_headers, "object",ignore)[1:-1]
			except TypeError:
				object = None
		elif predicate_object_map.object_map.mapping_type == "reference":
			object = string_substitution_postgres(predicate_object_map.object_map.value, ".+", row, row_headers, "object",ignore)
			if object != None:
				if "\\" in object[1:-1]:
					object = "\"" + object[1:-1].replace("\\","\\\\") + "\""
				if "'" in object[1:-1]:
					object = "\"" + object[1:-1].replace("'","\\\\'") + "\""
				if "\n" in object:
					object = object.replace("\n","\\n")
				if predicate_object_map.object_map.datatype != None:
					object += "^^<{}>".format(predicate_object_map.object_map.datatype)
				elif predicate_object_map.object_map.language != None:
					if "spanish" in predicate_object_map.object_map.language or "es" in predicate_object_map.object_map.language :
						object += "@es"
					elif "english" in predicate_object_map.object_map.language or "en" in predicate_object_map.object_map.language :
						object += "@en"
					elif len(predicate_object_map.object_map.language) == 2:
						object += "@"+predicate_object_map.object_map.language
				elif predicate_object_map.object_map.language_map != None:
					lang = string_substitution_postgres(predicate_object_map.object_map.language_map, ".+", row, row_headers, "object",ignore)
					if lang != None:
						object += "@"+ string_substitution_postgres(predicate_object_map.object_map.language_map, ".+", row, row_headers, "object",ignore)[1:-1]
				elif predicate_object_map.object_map.term != None:
					if "IRI" in predicate_object_map.object_map.term:
						if " " not in object:
							object = "\"" + object[1:-1].replace("\\\\'","'") + "\""
							object = "<" + encode_char(object[1:-1]) + ">"
						else:
							object = None
		elif predicate_object_map.object_map.mapping_type == "parent triples map":
			for triples_map_element in triples_map_list:
				if triples_map_element.triples_map_id == predicate_object_map.object_map.value:
					if (triples_map_element.data_source != triples_map.data_source) or (triples_map_element.tablename != triples_map.tablename):
						if triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0] not in join_table:
							if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
								with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
									if str(triples_map_element.file_format).lower() == "csv":
										data = csv.DictReader(input_file_descriptor, delimiter=delimiter)
										hash_maker(data, triples_map_element, predicate_object_map.object_map)
									else:
										data = json.load(input_file_descriptor)
										hash_maker(data[list(data.keys())[0]], triples_map_element, predicate_object_map.object_map)

							elif triples_map_element.file_format == "XPath":
								with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
									child_tree = ET.parse(input_file_descriptor)
									child_root = child_tree.getroot()
									hash_maker_xml(child_root, triples_map_element, predicate_object_map.object_map)								
							else:
								db_element = psycopg2.connect( host=host, user=user, password=password, dbname=db )
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
						jt = join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]]
						if row[row_headers.index(predicate_object_map.object_map.child[0])] != None and row[row_headers.index(predicate_object_map.object_map.child[0])] in jt:
							object_list = jt[row[row_headers.index(predicate_object_map.object_map.child[0])]]
						object = None
					else:
						if predicate_object_map.object_map.parent != None:
							if triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0] not in join_table:
								db_element = psycopg2.connect( host=host, user=user, password=password, dbname=db )
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
							jt = join_table[triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]]
							if row[row_headers.index(predicate_object_map.object_map.child[0])] != None and row[row_headers.index(predicate_object_map.object_map.child[0])] in jt:
								object_list = jt[row[row_headers.index(predicate_object_map.object_map.child[0])]]
							object = None
						else:
							try:
								database, query_list = translate_postgressql(triples_map)
								database2, query_list_origin = translate_postgressql(triples_map_element)
								db_element = psycopg2.connect( host=host, user=user, password=password, dbname=db )
								cursor = db_element.cursor()
								for query in query_list:
									for q in query_list_origin:
										query_1 = q.split("FROM")
										query_2 = query.split("SELECT")[1].split("FROM")[0]
										query_new = query_1[0] + ", " + query_2 + " FROM " + query_1[1]
										cursor.execute(query_new)
										r_h=[x[0] for x in cursor.description]
										for r in cursor:
											s = string_substitution_postgres(triples_map.subject_map.value, "{(.+?)}", r, r_h, "subject",ignore)
											if subject_value == s:
												object = "<" + string_substitution_postgres(triples_map_element.subject_map.value, "{(.+?)}", r, r_h, "object",ignore) + ">"
							except TypeError:
								object = None
					break
				else:
					continue
		else:
			object = None

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
						triple = triple[:-2] + " <" + string_substitution_array(graph, "{(.+?)}", row, row_headers,"subject",ignore) + ">.\n"
						dictionary_table_update("<" + string_substitution_array(graph, "{(.+?)}", row, row_headers,"subject",ignore) + ">")
					else:
						triple = triple[:-2] + " <" + graph + ">.\n"
						dictionary_table_update("<" + graph + ">")
				if duplicate == "yes":
					if predicate in general_predicates:
						if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
							try:
								output_file_descriptor.write(triple)
							except:
								output_file_descriptor.write(triple.encode("ISO 8859-1"))
							g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[object]: ""}})
							i += 1
						elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
							try:
								output_file_descriptor.write(triple)
							except:
								output_file_descriptor.write(triple.encode("ISO 8859-1"))
							g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[object]: ""})
							i += 1
					else:
						if dic_table[predicate] not in g_triples:					
							try:
								output_file_descriptor.write(triple)
							except:
								output_file_descriptor.write(triple.encode("ISO 8859-1"))
							g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[object]: ""}})
							i += 1
						elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
							try:
								output_file_descriptor.write(triple)
							except:
								output_file_descriptor.write(triple.encode("ISO 8859-1"))
							g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[object]: ""})
							i += 1
				else:
					try:
						output_file_descriptor.write(triple)
					except:
						output_file_descriptor.write(triple.encode("ISO 8859-1"))
					i += 1
			if predicate[1:-1] in predicate_object_map.graph:
				triple = subject + " " + predicate + " " + object + ".\n"
				if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in predicate_object_map.graph[predicate[1:-1]]:
					if "{" in predicate_object_map.graph[predicate[1:-1]]:
						triple = triple[:-2] + " <" + string_substitution_array(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, row_headers,"subject",ignore) + ">.\n"
						dictionary_table_update("<" + string_substitution_array(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, row_headers,"subject",ignore) + ">")
					else:
						triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
						dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
					if duplicate == "yes":
						if predicate in general_predicates:
							if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
								try:
									output_file_descriptor.write(triple)
								except:
									output_file_descriptor.write(triple.encode("ISO 8859-1"))
								g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[object]: ""}})
								i += 1
							elif dic_table[subject] + "_" + dic_table[object] not in g_triples[predicate + "_" + predicate_object_map.object_map.value]:
								try:
									output_file_descriptor.write(triple)
								except:
									output_file_descriptor.write(triple.encode("ISO 8859-1"))
								g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[object]: ""})
								i += 1
						else:
							if dic_table[predicate] not in g_triples:					
								try:
									output_file_descriptor.write(triple)
								except:
									output_file_descriptor.write(triple.encode("ISO 8859-1"))
								g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[object]: ""}})
								i += 1
							elif dic_table[subject] + "_" + dic_table[object] not in g_triples[dic_table[predicate]]:
								try:
									output_file_descriptor.write(triple)
								except:
									output_file_descriptor.write(triple.encode("ISO 8859-1"))
								g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[object]: ""})
								i += 1
					else:
						try:
							output_file_descriptor.write(triple)
						except:
							output_file_descriptor.write(triple.encode("ISO 8859-1"))
		elif predicate != None and subject != None and object_list:
			dictionary_table_update(subject)
			for obj in object_list:
				dictionary_table_update(obj)
				for graph in triples_map.subject_map.graph:
					triple = subject + " " + predicate + " " + obj + ".\n"
					if graph != None and "defaultGraph" not in graph:
						if "{" in graph:
							triple = triple[:-2] + " <" + string_substitution_array(graph, "{(.+?)}", row, row_headers,"subject",ignore) + ">.\n"
							dictionary_table_update("<" + string_substitution_array(graph, "{(.+?)}", row, row_headers,"subject",ignore) + ">")
						else:
							triple = triple[:-2] + " <" + graph + ">.\n"
							dictionary_table_update("<" + graph + ">")
					if duplicate == "yes":
						if predicate in general_predicates:
							if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:
								try:
									output_file_descriptor.write(triple)
								except:
									output_file_descriptor.write(triple.encode("ISO 8859-1"))
								g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
								i += 1
							elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
								try:
									output_file_descriptor.write(triple)
								except:
									output_file_descriptor.write(triple.encode("ISO 8859-1"))
								g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
								i += 1
						else:
							if dic_table[predicate] not in g_triples:
								try:
									output_file_descriptor.write(triple)
								except:
									output_file_descriptor.write(triple.encode("ISO 8859-1"))
								g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
								i += 1
							elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
								try:
									output_file_descriptor.write(triple)
								except:
									output_file_descriptor.write(triple.encode("ISO 8859-1"))
								g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
								i += 1
					else:
						try:
							output_file_descriptor.write(triple)
						except:
							output_file_descriptor.write(triple.encode("ISO 8859-1"))
						i += 1
				if predicate[1:-1] in predicate_object_map.graph:
					triple = subject + " " + predicate + " " + obj + ".\n"
					if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in predicate_object_map.graph[predicate[1:-1]]:
						if "{" in predicate_object_map.graph[predicate[1:-1]]:
							triple = triple[:-2] + " <" + string_substitution_array(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, row_headers,"subject",ignore) + ">.\n"
							dictionary_table_update("<" + string_substitution_array(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, row_headers,"subject",ignore) + ">")
						else:
							triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">.\n"
							dictionary_table_update("<" + predicate_object_map.graph[predicate[1:-1]] + ">")
						if duplicate == "yes":
							if predicate in general_predicates:
								if dic_table[predicate + "_" + predicate_object_map.object_map.value] not in g_triples:					
									try:
										output_file_descriptor.write(triple)
									except:
										output_file_descriptor.write(triple.encode("ISO 8859-1"))
									g_triples.update({dic_table[predicate + "_" + predicate_object_map.object_map.value] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
									i += 1
								elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]]:
									try:
										output_file_descriptor.write(triple)
									except:
										output_file_descriptor.write(triple.encode("ISO 8859-1"))
									g_triples[dic_table[predicate + "_" + predicate_object_map.object_map.value]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
									i += 1
							else:
								if dic_table[predicate] not in g_triples:					
									try:
										output_file_descriptor.write(triple)
									except:
										output_file_descriptor.write(triple.encode("ISO 8859-1"))
									g_triples.update({dic_table[predicate] : {dic_table[subject] + "_" + dic_table[obj]: ""}})
									i += 1
								elif dic_table[subject] + "_" + dic_table[obj] not in g_triples[dic_table[predicate]]:
									try:
										output_file_descriptor.write(triple)
									except:
										output_file_descriptor.write(triple.encode("ISO 8859-1"))
									g_triples[dic_table[predicate]].update({dic_table[subject] + "_" + dic_table[obj]: ""})
									i += 1
						else:
							try:
								output_file_descriptor.write(triple)
							except:
								output_file_descriptor.write(triple.encode("ISO 8859-1"))
							i += 1
			object_list = []
		else:
			continue
	return i

def translate_sql(triples_map):

    query_list = []
    proyections = []
  
    if "{" in triples_map.subject_map.value:
        subject = triples_map.subject_map.value
        count = count_characters(subject)
        if (count == 1) and (subject.split("{")[1].split("}")[0] not in proyections):
            subject = subject.split("{")[1].split("}")[0]
            if "[" in subject:
                subject = subject.split("[")[0]
            proyections.append(subject)
        elif count > 1:
            subject_list = subject.split("{")
            for s in subject_list:
                if "}" in s:
                    subject = s.split("}")[0]
                    if "[" in subject:
                        subject = subject.split("[")
                    if subject not in proyections:
                        proyections.append(subject)
    else:
        if triples_map.subject_map.value not in proyections:
            proyections.append(triples_map.subject_map.value)

    for po in triples_map.predicate_object_maps_list:
        if po.object_map.mapping_type != "constant":
            if "{" in po.object_map.value:
                count = count_characters(po.object_map.value)
                if 0 < count <= 1 :
                    predicate = po.object_map.value.split("{")[1].split("}")[0]
                    if "[" in predicate:
                        predicate = predicate.split("[")[0]
                    if predicate not in proyections:
                        proyections.append(predicate)

                elif 1 < count:
                    predicate = po.object_map.value.split("{")
                    for po_e in predicate:
                        if "}" in po_e:
                            pre = po_e.split("}")[0]
                            if "[" in pre:
                                pre = pre.split("[")
                            if pre not in proyections:
                                proyections.append(pre)
            elif "#" in po.object_map.value:
                pass
            elif "/" in po.object_map.value:
                pass
            else:
                predicate = po.object_map.value 
                if "[" in predicate:
                    predicate = predicate.split("[")[0]
                if predicate not in proyections:
                    proyections.append(predicate)
            if po.object_map.child != None:
                for c in po.object_map.child:
                    if c not in proyections:
                        proyections.append(c)

    temp_query = "SELECT DISTINCT "
    for p in proyections:
        if type(p) == str:
            if p != "None":
                temp_query += "`" + p + "`, "
        elif type(p) == list:
            for pr in p:
                temp_query += "`" + pr + "`, " 
    temp_query = temp_query[:-2] 
    if triples_map.tablename != "None":
        temp_query = temp_query + " FROM " + triples_map.tablename + ";"
    else:
        temp_query = temp_query + " FROM " + triples_map.data_source + ";"
    query_list.append(temp_query)

    return triples_map.iterator, query_list


def translate_postgressql(triples_map):

	query_list = []
	
	
	proyections = []

		
	if "{" in triples_map.subject_map.value:
		subject = triples_map.subject_map.value
		count = count_characters(subject)
		if (count == 1) and (subject.split("{")[1].split("}")[0] not in proyections):
			subject = subject.split("{")[1].split("}")[0]
			if "[" in subject:
				subject = subject.split("[")[0]
			proyections.append(subject)
		elif count > 1:
			subject_list = subject.split("{")
			for s in subject_list:
				if "}" in s:
					subject = s.split("}")[0]
					if "[" in subject:
						subject = subject.split("[")
					if subject not in proyections:
						proyections.append(subject)

	for po in triples_map.predicate_object_maps_list:
		if "{" in po.object_map.value:
			count = count_characters(po.object_map.value)
			if 0 < count <= 1 :
				predicate = po.object_map.value.split("{")[1].split("}")[0]
				if "[" in predicate:
					predicate = predicate.split("[")[0]
				if predicate not in proyections:
					proyections.append(predicate)

			elif 1 < count:
				predicate = po.object_map.value.split("{")
				for po_e in predicate:
					if "}" in po_e:
						pre = po_e.split("}")[0]
						if "[" in pre:
							pre = pre.split("[")
						if pre not in proyections:
							proyections.append(pre)
		elif "#" in po.object_map.value:
			pass
		elif "/" in po.object_map.value:
			pass
		else:
			predicate = po.object_map.value 
			if "[" in predicate:
				predicate = predicate.split("[")[0]
			if predicate not in proyections:
					proyections.append(predicate)
		if po.object_map.child != None:
			for child in po.object_map.child:
				if child not in proyections:
					proyections.append(child)

	temp_query = "SELECT "
	for p in proyections:
		if p != "None":
			if p == proyections[len(proyections)-1]:
				temp_query += "\"" + p + "\""
			else:
				temp_query += "\"" + p + "\", " 
		else:
			temp_query = temp_query[:-2] 
	if triples_map.tablename != "None":
		temp_query = temp_query + " FROM " + triples_map.tablename + ";"
	else:
		temp_query = temp_query + " FROM " + triples_map.data_source + ";"
	query_list.append(temp_query)
	return triples_map.iterator, query_list


def semantify(config_path):
	start_time = time.time()
	if os.path.isfile(config_path) == False:
		print("The configuration file " + config_path + " does not exist.")
		print("Aborting...")
		sys.exit(1)

	config = ConfigParser(interpolation=ExtendedInterpolation())
	config.read(config_path)

	global duplicate
	duplicate = config["datasets"]["remove_duplicate"]

	global output_format
	if "output_format" in config["datasets"]:
		output_format = config["datasets"]["output_format"]
	else:
		output_format = "n-triples"
	enrichment = config["datasets"]["enrichment"]

	if not os.path.exists(config["datasets"]["output_folder"]):
		os.mkdir(config["datasets"]["output_folder"])

	global number_triple
	global blank_message
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
				print("Semantifying {}...".format(config[dataset_i]["name"]))
				
				with open(output_file, "w", encoding = "ISO 8859-1") as output_file_descriptor:
					if "turtle" == output_format.lower():
						string_prefixes = prefix_extraction(config[dataset_i]["mapping"])
						output_file_descriptor.write(string_prefixes)
					sorted_sources, predicate_list, order_list = files_sort(triples_map_list, config["datasets"]["ordered"])
					if sorted_sources:
						if order_list:
							for source_type in order_list:
								if source_type == "csv":
									for source in order_list[source_type]:
										if enrichment == "yes":
											if ".csv" in source:
												reader = pd.read_csv(source, dtype = str)#, encoding = "ISO-8859-1")
											else:
												reader = pd.read_csv(source, dtype = str, sep='\t')#, encoding = "ISO-8859-1")
											reader = reader.where(pd.notnull(reader), None)
											if duplicate == "yes":
												reader = reader.drop_duplicates(keep ='first')
											data = reader.to_dict(orient='records')
											for triples_map in sorted_sources[source_type][source]:
												blank_message = True
												number_triple += executor.submit(semantify_file, sorted_sources[source_type][source][triples_map], triples_map_list, ",", output_file_descriptor, data).result()
												if duplicate == "yes":
													predicate_list = release_PTT(sorted_sources[source_type][source][triples_map],predicate_list)
										else:
											for triples_map in sorted_sources[source_type][source]:
												with open(source, "r", encoding = "ISO 8859-1") as input_file_descriptor:
													if ".csv" in source:
														data = csv.DictReader(input_file_descriptor, delimiter=',')
													else:
														data = csv.DictReader(input_file_descriptor, delimiter='\t')
													blank_message = True
													number_triple += executor.submit(semantify_file, sorted_sources[source_type][source][triples_map], triples_map_list, ",", output_file_descriptor, data).result()
													if duplicate == "yes":
														predicate_list = release_PTT(sorted_sources[source_type][source][triples_map],predicate_list)
								elif source_type == "JSONPath":
									for source in order_list[source_type]:
										with open(str(source), "r") as input_file_descriptor:
											data = json.load(input_file_descriptor)
											for triples_map in sorted_sources[source_type][source]:
												blank_message = True
												if isinstance(data, list):
													number_triple += executor.submit(semantify_file, sorted_sources[source_type][source][triples_map], triples_map_list, ",",output_file_descriptor, data).result()
												else:
													number_triple += executor.submit(semantify_json, sorted_sources[source_type][source][triples_map], triples_map_list, ",",output_file_descriptor, data, sorted_sources[source_type][source][triples_map].iterator).result()
												if duplicate == "yes":
													predicate_list = release_PTT(sorted_sources[source_type][source][triples_map],predicate_list)
								elif source_type == "XPath":
									for source in order_list[source_type]:
										for triples_map in sorted_sources[source_type][source]:
											blank_message = True
											number_triple += executor.submit(semantify_xml, sorted_sources[source_type][source][triples_map], triples_map_list, output_file_descriptor).result()
											if duplicate == "yes":
												predicate_list = release_PTT(sorted_sources[source_type][source][triples_map],predicate_list)			
						else:
							for source_type in sorted_sources:
								if source_type == "csv":
									for source in sorted_sources[source_type]:
										if enrichment == "yes":
											if ".csv" in source:
												reader = pd.read_csv(source, dtype = str)#, encoding = "ISO-8859-1")
											else:
												reader = pd.read_csv(source, dtype = str,sep="\t",header=0)#, encoding = "ISO-8859-1")
											reader = reader.where(pd.notnull(reader), None)
											if duplicate == "yes":
												reader = reader.drop_duplicates(keep ='first')
											data = reader.to_dict(orient='records')
											for triples_map in sorted_sources[source_type][source]:
												blank_message = True
												number_triple += executor.submit(semantify_file, sorted_sources[source_type][source][triples_map], triples_map_list, ",", output_file_descriptor, data).result()
												if duplicate == "yes":
													predicate_list = release_PTT(sorted_sources[source_type][source][triples_map],predicate_list)	
										else:
											for triples_map in sorted_sources[source_type][source]:
												blank_message = True
												with open(source, "r", encoding = "ISO 8859-1") as input_file_descriptor:
													if ".csv" in source:
														data = csv.DictReader(input_file_descriptor, delimiter=',')
													else:
														data = csv.DictReader(input_file_descriptor, delimiter='\t')
													number_triple += executor.submit(semantify_file, sorted_sources[source_type][source][triples_map], triples_map_list, ",", output_file_descriptor, data).result()
													if duplicate == "yes":
														predicate_list = release_PTT(sorted_sources[source_type][source][triples_map],predicate_list)
								elif source_type == "JSONPath":
									for source in sorted_sources[source_type]:
										with open(str(source), "r") as input_file_descriptor:
											data = json.load(input_file_descriptor)
											for triples_map in sorted_sources[source_type][source]:
												blank_message = True
												if isinstance(data, list):
													number_triple += executor.submit(semantify_file, sorted_sources[source_type][source][triples_map], triples_map_list, ",",output_file_descriptor, data).result()
												else:
													number_triple += executor.submit(semantify_json, sorted_sources[source_type][source][triples_map], triples_map_list, ",",output_file_descriptor, data, sorted_sources[source_type][source][triples_map].iterator).result()
												if duplicate == "yes":
													predicate_list = release_PTT(sorted_sources[source_type][source][triples_map],predicate_list)
								elif source_type == "XPath":
									for source in sorted_sources[source_type]:
										for triples_map in sorted_sources[source_type][source]:
											blank_message = True
											number_triple += executor.submit(semantify_xml, sorted_sources[source_type][source][triples_map], triples_map_list, output_file_descriptor).result()
											if duplicate == "yes":
												predicate_list = release_PTT(sorted_sources[source_type][source][triples_map],predicate_list)	
					if predicate_list:
						for triples_map in triples_map_list:
							blank_message = True
							if str(triples_map.file_format).lower() != "csv" and triples_map.file_format != "JSONPath" and triples_map.file_format != "XPath":
								if config["datasets"]["dbType"] == "mysql":
									print("TM:", triples_map.triples_map_name)
									database, query_list = translate_sql(triples_map)
									db = connector.connect(host = config[dataset_i]["host"], port = int(config[dataset_i]["port"]), user = config[dataset_i]["user"], password = config[dataset_i]["password"])
									cursor = db.cursor(buffered=True)
									if config[dataset_i]["db"].lower() != "none":
										cursor.execute("use " + config[dataset_i]["db"])
									else:
										if database != "None":
											cursor.execute("use " + database)
									if triples_map.query == "None":	
										for query in query_list:
											cursor.execute(query)
											row_headers=[x[0] for x in cursor.description]
											for row in cursor:
												if config[dataset_i]["db"].lower() != "none":
													number_triple += executor.submit(semantify_mysql, row, row_headers, triples_map, triples_map_list, output_file_descriptor, config[dataset_i]["host"], int(config[dataset_i]["port"]), config[dataset_i]["user"], config[dataset_i]["password"],config[dataset_i]["db"]).result()
												else:
													number_triple += executor.submit(semantify_mysql, row, row_headers, triples_map, triples_map_list, output_file_descriptor, config[dataset_i]["host"], int(config[dataset_i]["port"]), config[dataset_i]["user"], config[dataset_i]["password"],"None").result()
									else:
										cursor.execute(triples_map.query)
										row_headers=[x[0] for x in cursor.description]
										for row in cursor:
											#print(row)
											if config[dataset_i]["db"].lower() != "none":
												number_triple += executor.submit(semantify_mysql, row, row_headers, triples_map, triples_map_list, output_file_descriptor, config[dataset_i]["host"], int(config[dataset_i]["port"]), config[dataset_i]["user"], config[dataset_i]["password"],config[dataset_i]["db"]).result()
											else:
												number_triple += executor.submit(semantify_mysql, row, row_headers, triples_map, triples_map_list, output_file_descriptor, config[dataset_i]["host"], int(config[dataset_i]["port"]), config[dataset_i]["user"], config[dataset_i]["password"],"None").result()
									predicate_list = release_PTT(triples_map,predicate_list)
								elif config["datasets"]["dbType"] == "postgres":
									print("TM:", triples_map.triples_map_name)	
									database, query_list = translate_postgressql(triples_map)
									db = psycopg2.connect( host=config[dataset_i]["host"], user= config[dataset_i]["user"], password=config[dataset_i]["password"], dbname=config[dataset_i]["db"] )
									cursor = db.cursor()
									if triples_map.query == "None":	
										for query in query_list:
											cursor.execute(query)
											row_headers=[x[0] for x in cursor.description]
											for row in cursor:
												number_triple += executor.submit(semantify_postgres, row, row_headers, triples_map, triples_map_list, output_file_descriptor,config[dataset_i]["user"], config[dataset_i]["password"], config[dataset_i]["db"], config[dataset_i]["host"]).result()
									else:
										cursor.execute(triples_map.query)
										row_headers=[x[0] for x in cursor.description]
										for row in cursor:
											number_triple += executor.submit(semantify_postgres, row, row_headers, triples_map, triples_map_list, output_file_descriptor,config[dataset_i]["user"], config[dataset_i]["password"], config[dataset_i]["db"], config[dataset_i]["host"]).result()
									predicate_list = release_PTT(triples_map,predicate_list)					
								else:
									print("Invalid reference formulation or format")
									print("Aborting...")
									sys.exit(1)
				print("Successfully semantified {}.\n\n".format(config[dataset_i]["name"]))
	else:
		if "turtle" == output_format.lower():
			output_file = config["datasets"]["output_folder"] + "/" + config["datasets"]["name"] + ".ttl"
		else:
			output_file = config["datasets"]["output_folder"] + "/" + config["datasets"]["name"] + ".nt"

		with ThreadPoolExecutor(max_workers=10) as executor:
			with open(output_file, "w", encoding="ISO 8859-1") as output_file_descriptor:
				for dataset_number in range(int(config["datasets"]["number_of_datasets"])):
					dataset_i = "dataset" + str(int(dataset_number) + 1)
					triples_map_list = mapping_parser(config[dataset_i]["mapping"])
					base = extract_base(config[dataset_i]["mapping"])
					if "turtle" == output_format.lower():
						string_prefixes = prefix_extraction(config[dataset_i]["mapping"])
						output_file_descriptor.write(string_prefixes)
					print("Semantifying {}...".format(config[dataset_i]["name"]))
				
					sorted_sources, predicate_list, order_list = files_sort(triples_map_list, config["datasets"]["ordered"])
					if sorted_sources:
						if order_list:
							for source_type in order_list:
								if source_type == "csv":
									for source in order_list[source_type]:
										if enrichment == "yes":
											reader = pd.read_csv(source, encoding = "ISO-8859-1")
											reader = reader.where(pd.notnull(reader), None)
											if duplicate == "yes":
												reader = reader.drop_duplicates(keep ='first')
											data = reader.to_dict(orient='records')
											for triples_map in sorted_sources[source_type][source]:
												blank_message = True
												number_triple += executor.submit(semantify_file, sorted_sources[source_type][source][triples_map], triples_map_list, ",", output_file_descriptor, data).result()
												if duplicate == "yes":
													predicate_list = release_PTT(sorted_sources[source_type][source][triples_map],predicate_list)
										else:
											for triples_map in sorted_sources[source_type][source]:
												blank_message = True
												with open(source, "r", encoding = "ISO-8859-1") as input_file_descriptor:
													data = csv.DictReader(input_file_descriptor, delimiter=',') 
													number_triple += executor.submit(semantify_file, sorted_sources[source_type][source][triples_map], triples_map_list, ",", output_file_descriptor, data).result()
													if duplicate == "yes":
														predicate_list = release_PTT(sorted_sources[source_type][source][triples_map],predicate_list)
								elif source_type == "JSONPath":
									for source in order_list[source_type]:
										with open(str(source), "r") as input_file_descriptor:
											data = json.load(input_file_descriptor)
											for triples_map in sorted_sources[source_type][source]:
												blank_message = True
												if isinstance(data, list):
													number_triple += executor.submit(semantify_file, sorted_sources[source_type][source][triples_map], triples_map_list, ",",output_file_descriptor,  data).result()
												else:
													number_triple += executor.submit(semantify_json, sorted_sources[source_type][source][triples_map], triples_map_list, ",",output_file_descriptor, data, sorted_sources[source_type][source][triples_map].iterator).result()
												predicate_list = release_PTT(sorted_sources[source_type][source][triples_map],predicate_list)
								elif source_type == "XPath":
									for source in order_list[source_type]:
										for triples_map in sorted_sources[source_type][source]:
											blank_message = True
											number_triple += executor.submit(semantify_xml, sorted_sources[source_type][source][triples_map], triples_map_list, output_file_descriptor).result()
											predicate_list = release_PTT(sorted_sources[source_type][source][triples_map],predicate_list)			
						else:
							for source_type in sorted_sources:
								if source_type == "csv":
									for source in sorted_sources[source_type]:
										if enrichment == "yes":
											reader = pd.read_csv(source, encoding = "ISO-8859-1")
											reader = reader.where(pd.notnull(reader), None)
											if duplicate == "yes":
												reader = reader.drop_duplicates(keep ='first')
											data = reader.to_dict(orient='records')
											for triples_map in sorted_sources[source_type][source]:
												blank_message = True
												number_triple += executor.submit(semantify_file, sorted_sources[source_type][source][triples_map], triples_map_list, ",", output_file_descriptor, data).result()
												if duplicate == "yes":
													predicate_list = release_PTT(sorted_sources[source_type][source][triples_map],predicate_list)	
										else:
											with open(source, "r", encoding = "ISO-8859-1") as input_file_descriptor:
												data = csv.DictReader(input_file_descriptor, delimiter=',') 
												for triples_map in sorted_sources[source_type][source]:
													blank_message = True
													number_triple += executor.submit(semantify_file, sorted_sources[source_type][source][triples_map], triples_map_list, ",", output_file_descriptor, data).result()
													if duplicate == "yes":
														predicate_list = release_PTT(sorted_sources[source_type][source][triples_map],predicate_list)
								elif source_type == "JSONPath":
									for source in sorted_sources[source_type]:
										with open(str(source), "r") as input_file_descriptor:
											data = json.load(input_file_descriptor)
											for triples_map in sorted_sources[source_type][source]:
												blank_message = True
												if isinstance(data, list):
													number_triple += executor.submit(semantify_file, sorted_sources[source_type][source][triples_map], triples_map_list, ",",output_file_descriptor, data).result()
												else:
													number_triple += executor.submit(semantify_json, sorted_sources[source_type][source][triples_map], triples_map_list, ",",output_file_descriptor, data, sorted_sources[source_type][source][triples_map].iterator).result()
												predicate_list = release_PTT(sorted_sources[source_type][source][triples_map],predicate_list)
								elif source_type == "XPath":
									for source in sorted_sources[source_type]:
										for triples_map in sorted_sources[source_type][source]:
											blank_message = True
											number_triple += executor.submit(semantify_xml, sorted_sources[source_type][source][triples_map], triples_map_list, output_file_descriptor).result()
											predicate_list = release_PTT(sorted_sources[source_type][source][triples_map],predicate_list)	
					
					if predicate_list:
						for triples_map in triples_map_list:
							blank_message = True
							if str(triples_map.file_format).lower() != "csv" and triples_map.file_format != "JSONPath" and triples_map.file_format != "XPath":
								if config["datasets"]["dbType"] == "mysql":
									print("TM:", triples_map.triples_map_name)
									database, query_list = translate_sql(triples_map)
									db = connector.connect(host = config[dataset_i]["host"], port = int(config[dataset_i]["port"]), user = config[dataset_i]["user"], password = config[dataset_i]["password"])
									cursor = db.cursor(buffered=True)
									if config[dataset_i]["db"].lower() != "none":
										cursor.execute("use " + config[dataset_i]["db"])
									else:
										if database != "None":
											cursor.execute("use " + database)
									if triples_map.query == "None":	
										for query in query_list:
											cursor.execute(query)
											row_headers=[x[0] for x in cursor.description]
											for row in cursor:
												if config[dataset_i]["db"].lower() != "none":
													number_triple += executor.submit(semantify_mysql, row, row_headers, triples_map, triples_map_list, output_file_descriptor, config[dataset_i]["host"], int(config[dataset_i]["port"]), config[dataset_i]["user"], config[dataset_i]["password"],config[dataset_i]["db"]).result()
												else:
													number_triple += executor.submit(semantify_mysql, row, row_headers, triples_map, triples_map_list, output_file_descriptor, config[dataset_i]["host"], int(config[dataset_i]["port"]), config[dataset_i]["user"], config[dataset_i]["password"],"None").result()
									else:
										cursor.execute(triples_map.query)
										row_headers=[x[0] for x in cursor.description]
										for row in cursor:
											if config[dataset_i]["db"].lower() != "none":
												number_triple += executor.submit(semantify_mysql, row, row_headers, triples_map, triples_map_list, output_file_descriptor, config[dataset_i]["host"], int(config[dataset_i]["port"]), config[dataset_i]["user"], config[dataset_i]["password"],config[dataset_i]["db"]).result()
											else:
												number_triple += executor.submit(semantify_mysql, row, row_headers, triples_map, triples_map_list, output_file_descriptor, config[dataset_i]["host"], int(config[dataset_i]["port"]), config[dataset_i]["user"], config[dataset_i]["password"],"None").result()
									predicate_list = release_PTT(triples_map,predicate_list)
								elif config["datasets"]["dbType"] == "postgres":
									print("TM:", triples_map.triples_map_name)	
									database, query_list = translate_postgressql(triples_map)
									db = psycopg2.connect(host=config[dataset_i]["host"], user= config[dataset_i]["user"], password=config[dataset_i]["password"], dbname=config[dataset_i]["db"] )
									cursor = db.cursor()
									if triples_map.query == "None":	
										for query in query_list:
											cursor.execute(query)
											row_headers=[x[0] for x in cursor.description]
											for row in cursor:
												number_triple += executor.submit(semantify_postgres, row, row_headers, triples_map, triples_map_list, output_file_descriptor,config[dataset_i]["user"], config[dataset_i]["password"], config[dataset_i]["db"], config[dataset_i]["host"]).result()
									else:
										cursor.execute(triples_map.query)
										row_headers=[x[0] for x in cursor.description]
										for row in cursor:
											number_triple += executor.submit(semantify_postgres, row, row_headers, triples_map, triples_map_list, output_file_descriptor,config[dataset_i]["user"], config[dataset_i]["password"], config[dataset_i]["db"], config[dataset_i]["host"]).result()
									predicate_list = release_PTT(triples_map,predicate_list)					
								else:
									print("Invalid reference formulation or format")
									print("Aborting...")
									sys.exit(1)
					print("Successfully semantified {}.\n\n".format(config[dataset_i]["name"]))

	duration = time.time() - start_time

	print("Successfully semantified all datasets in {:.3f} seconds.".format(duration))

		


"""
According to the meeting held on 11.04.2018, semantifying json files != a top priority right
now, thus the reimplementation of following functions remain largely undocumented and unfinished.

def json_generator(file_descriptor, iterator):
	if len(iterator) != 0:
		if "[*]" not in iterator[0] and iterator[0] != "$":
			yield from json_generator(file_descriptor[iterator[0]], iterator[1:])
		elif "[*]" not in iterator[0] and iterator[0] == "$":
			yield from json_generator(file, iterator[1:])
		elif "[*]" in iterator[0] and "$" not in iterator[0]:
			file_array = file_descriptor[iterator[0].replace("[*]","")]
			for array_elem in file_array:
				yield from json_generator(array_elem, iterator[1:])
		elif iterator[0] == "$[*]":
			for array_elem in file_descriptor:
				yield from json_generator(array_elem, iterator[1:])
	else:
		yield file_descriptor


"""
