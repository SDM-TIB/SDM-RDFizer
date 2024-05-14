import os
import re
import datetime
import sys
import urllib
import math
from .functions import *
global inner_join_table
inner_join_table = {}
global general_predicates
general_predicates = {"http://www.w3.org/2000/01/rdf-schema#subClassOf": "",
                      "http://www.w3.org/2002/07/owl#sameAs": "",
                      "http://www.w3.org/2000/01/rdf-schema#seeAlso": "",
                      "http://www.w3.org/2000/01/rdf-schema#subPropertyOf": ""}

def inner_hash_maker(parent_data, parent_subject, child_object, quoted, triples_map_list):
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
        join_table.update({"quoted_" + parent_subject.triples_map_id + "_" + child_object.child[0] : hash_table})

def inner_semantify_file(triples_map, triples_map_list, delimiter, row, base):
	object_list = []
	subject_list = []
	triples_list = []

	if triples_map.subject_map.subject_mapping_type == "template":
		subject_value = string_substitution(triples_map.subject_map.value, "{(.+?)}", row, "subject", "yes", triples_map.iterator)
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
				subject_value = string_substitution(triples_map.subject_map.value, "{(.+?)}", row, "subject", "yes", triples_map.iterator)
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
		subject_value = string_substitution(triples_map.subject_map.value, ".+", row, "subject","yes" , triples_map.iterator)
		if subject_value != None:
			subject_value = subject_value[1:-1]
			if triples_map.subject_map.condition == "":
				if " " not in subject_value:
					if "BlankNode" in triples_map.subject_map.term_type:
						subject = "_:" + subject_value
					else:
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

	elif "quoted triples map" in triples_map.subject_map.subject_mapping_type:
		for triples_map_element in triples_map_list:
			if triples_map_element.triples_map_id == triples_map.subject_map.value:
				if triples_map_element.data_source != triples_map.data_source:
					if triples_map.subject_map.parent[0] != None:
						if ("quoted_" + triples_map_element.triples_map_id + "_" + triples_map.subject_map.child[0]) not in join_table:
							if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
								with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
									if str(triples_map_element.file_format).lower() == "csv":
										data = csv.DictReader(input_file_descriptor, delimiter=',')
										hash_maker(data, triples_map_element, triples_map.subject_map, "quoted", triples_map_list)
									else:
										pass
						if row[triples_map.subject_map.child[0]] in join_table["quoted_" + triples_map_element.triples_map_id + "_" + triples_map.subject_map.child[0]]:
						    subject_list = join_table["quoted_" + triples_map_element.triples_map_id + "_" + triples_map.subject_map.child[0]][row[triples_map.subject_map.child[0]]]
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


	if triples_map.subject_map.rdf_class != None and subject != None:
		predicate = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
		for rdf_class in triples_map.subject_map.rdf_class:
			if rdf_class != None:
				obj = "<{}>".format(rdf_class)
				rdf_type = subject + " " + predicate + " " + obj 
				for graph in triples_map.subject_map.graph:
					if graph != None and "defaultGraph" not in graph:
						if "{" in graph:	
							rdf_type = rdf_type[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject","yes", triples_map.iterator) + "> .\n"
						else:
							rdf_type = rdf_type[:-2] + " <" + graph + "> .\n"
				triples_list.append(rdf_type)

	
	for predicate_object_map in triples_map.predicate_object_maps_list:
		if predicate_object_map.predicate_map.mapping_type == "constant" or predicate_object_map.predicate_map.mapping_type == "constant shortcut":
			predicate = "<" + predicate_object_map.predicate_map.value + ">"
		elif predicate_object_map.predicate_map.mapping_type == "template":
			if predicate_object_map.predicate_map.condition != "":
					#field, condition = condition_separetor(predicate_object_map.predicate_map.condition)
					#if row[field] == condition:
					try:
						predicate = "<" + string_substitution(predicate_object_map.predicate_map.value, "{(.+?)}", row, "predicate","yes", triples_map.iterator) + ">"
					except:
						predicate = None
					#else:
					#	predicate = None
			else:
				try:
					predicate = "<" + string_substitution(predicate_object_map.predicate_map.value, "{(.+?)}", row, "predicate","yes", triples_map.iterator) + ">"
				except:
					predicate = None
		elif predicate_object_map.predicate_map.mapping_type == "reference":
			if predicate_object_map.predicate_map.condition != "":
				#field, condition = condition_separetor(predicate_object_map.predicate_map.condition)
				#if row[field] == condition:
				predicate = string_substitution(predicate_object_map.predicate_map.value, ".+", row, "predicate","yes", triples_map.iterator)
				#else:
				#	predicate = None
			else:
				predicate = string_substitution(predicate_object_map.predicate_map.value, ".+", row, "predicate","yes", triples_map.iterator)
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
					object = "<" + string_substitution(predicate_object_map.object_map.value, "{(.+?)}", row, "object","yes", triples_map.iterator) + ">"
				elif "IRI" in predicate_object_map.object_map.term:
					object = "<" + string_substitution(predicate_object_map.object_map.value, "{(.+?)}", row, "object","yes", triples_map.iterator) + ">"
				elif "BlankNode" in predicate_object_map.object_map.term:
					object = "_:" + string_substitution(predicate_object_map.object_map.value, "{(.+?)}", row, "object","yes", triples_map.iterator)
					if "/" in object:
						object  = object.replace("/","2F")
						if blank_message:
							print("Incorrect format for Blank Nodes. \"/\" will be replace with \"2F\".")
							blank_message = False
					if "." in object:
						object = object.replace(".","2E")
					object = encode_char(object)
				else:
					object = "\"" + string_substitution(predicate_object_map.object_map.value, "{(.+?)}", row, "object","yes", triples_map.iterator) + "\""
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
						lang = string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object","yes", triples_map.iterator)
						if lang != None:
							object += "@" + string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object","yes", triples_map.iterator)[1:-1] 
			except TypeError:
				object = None
		elif predicate_object_map.object_map.mapping_type == "reference":
			object = string_substitution(predicate_object_map.object_map.value, ".+", row, "object","yes", triples_map.iterator)
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
					lang = string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object","yes", triples_map.iterator)
					if lang != None:
						object += "@"+ string_substitution(predicate_object_map.object_map.language_map, ".+", row, "object","yes", triples_map.iterator)[1:-1]
				elif predicate_object_map.object_map.term != None:
					if "IRI" in predicate_object_map.object_map.term:
						if " " not in object:
							object = "\"" + object[1:-1].replace("\\\\'","'") + "\""
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
				for triples_map_inner in triples_map_list:
					if triples_map_inner.triples_map_id == predicate_object_map.object_map.value:
						if triples_map_inner.data_source != triples_map.data_source:
							if len(predicate_object_map.object_map.child) == 1:
								if (triples_map_inner.triples_map_id + "_" + predicate_object_map.object_map.child[0]) not in join_table:
									if str(triples_map_inner.file_format).lower() == "csv" or triples_map.file_format == "JSONPath":
										with open(str(triples_map_inner.data_source), "r") as input_file_descriptor:
											if str(triples_map.file_format).lower() == "csv":
												reader = pd.read_csv(str(triples_map_inner.data_source), dtype = str)#, encoding = "ISO-8859-1")
												reader = reader.where(pd.notnull(reader), None)
												reader = reader.drop_duplicates(keep ='first')
												data = reader.to_dict(orient='records')
												hash_maker(data, triples_map_inner, predicate_object_map.object_map)
											else:
												data = json.load(input_file_descriptor)
												if triples_map_inner.iterator:
													if triples_map_inner.iterator != "None" and triples_map_inner.iterator != "$.[*]":
														join_iterator(data, triples_map_inner.iterator, triples_map_inner, predicate_object_map.object_map)
													else:
														if isinstance(data, list):
															hash_maker(data, triples_map_inner, predicate_object_map.object_map)
														elif len(data) < 2:
															hash_maker(data[list(data.keys())[0]], triples_map_inner, predicate_object_map.object_map)
												else:
													if isinstance(data, list):
														hash_maker(data, triples_map_inner, predicate_object_map.object_map)
													elif len(data) < 2:
														hash_maker(data[list(data.keys())[0]], triples_map_inner, predicate_object_map.object_map)

									elif triples_map.file_format == "XPath":
										with open(str(triples_map.data_source), "r") as input_file_descriptor:
											child_tree = ET.parse(input_file_descriptor)
											child_root = child_tree.getroot()
											hash_maker_xml(child_root, triples_map_inner, predicate_object_map.object_map)								
									else:
										database, query_list = translate_sql(triples_map)
										db = connector.connect(host=host, port=int(port), user=user, password=password)
										cursor = db.cursor(buffered=True)
										cursor.execute("use " + database)
										for query in query_list:
											cursor.execute(query)
										hash_maker_array(cursor, triples_map_inner, predicate_object_map.object_map)

								if sublist(predicate_object_map.object_map.child,row.keys()):
									if child_list_value(predicate_object_map.object_map.child,row) in join_table[triples_map_inner.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)]:
										object_list = join_table[triples_map_inner.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)][child_list_value(predicate_object_map.object_map.child,row)]
									else:
										if no_update:
											if str(triples_map_inner.file_format).lower() == "csv" or triples_map_inner.file_format == "JSONPath":
												with open(str(triples_map_inner.data_source), "r") as input_file_descriptor:
													if str(triples_map_inner.file_format).lower() == "csv":
														reader = pd.read_csv(str(triples_map_inner.data_source), dtype = str)#, encoding = "ISO-8859-1")
														reader = reader.where(pd.notnull(reader), None)
														reader = reader.drop_duplicates(keep ='first')
														data = reader.to_dict(orient='records')
														hash_update(data, triples_map_inner, predicate_object_map.object_map, triples_map_inner.triples_map_id + "_" + predicate_object_map.object_map.child[0])
													else:
														data = json.load(input_file_descriptor)
														if triples_map_inner.iterator:
															if triples_map_inner.iterator != "None" and triples_map_inner.iterator != "$.[*]":
																join_iterator(data, triples_map_inner.iterator, triples_map_inner, predicate_object_map.object_map)
															else:
																if isinstance(data, list):
																	hash_maker(data, triples_map_inner, predicate_object_map.object_map)
																elif len(data) < 2:
																	hash_maker(data[list(data.keys())[0]], triples_map_inner, predicate_object_map.object_map)
														else:
															if isinstance(data, list):
																hash_maker(data, triples_map_inner, predicate_object_map.object_map)
															elif len(data) < 2:
																hash_maker(data[list(data.keys())[0]], triples_map_inner, predicate_object_map.object_map)
											if child_list_value(predicate_object_map.object_map.child,row) in join_table[triples_map_inner.triples_map_id + "_" + predicate_object_map.object_map.child[0]]:
												object_list = join_table[triples_map_inner.triples_map_id + "_" + predicate_object_map.object_map.child[0]][row[predicate_object_map.object_map.child[0]]]
											else:
												object_list = []
											no_update = False
								object = None
							else:
								if (triples_map_inner.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)) not in join_table:
									if str(triples_map_inner.file_format).lower() == "csv" or triples_map_inner.file_format == "JSONPath":
										with open(str(triples_map_inner.data_source), "r") as input_file_descriptor:
											if str(triples_map_inner.file_format).lower() == "csv":
												data = csv.DictReader(input_file_descriptor, delimiter=delimiter)
												hash_maker_list(data, triples_map_inner, predicate_object_map.object_map)
											else:
												data = json.load(input_file_descriptor)
												if isinstance(data, list):
													hash_maker_list(data, triples_map_inner, predicate_object_map.object_map)
												elif len(data) < 2:
													hash_maker_list(data[list(data.keys())[0]], triples_map_inner, predicate_object_map.object_map)

									elif triples_map_inner.file_format == "XPath":
										with open(str(triples_map_inner.data_source), "r") as input_file_descriptor:
											child_tree = ET.parse(input_file_descriptor)
											child_root = child_tree.getroot()
											hash_maker_xml(child_root, triples_map_inner, predicate_object_map.object_map)						
									else:
										database, query_list = translate_sql(triples_map)
										db = connector.connect(host=host, port=int(port), user=user, password=password)
										cursor = db.cursor(buffered=True)
										cursor.execute("use " + database)
										for query in query_list:
											cursor.execute(query)
										hash_maker_array(cursor, triples_map_inner, predicate_object_map.object_map)
								if sublist(predicate_object_map.object_map.child,row.keys()):
									if child_list_value(predicate_object_map.object_map.child,row) in join_table[triples_map_inner.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)]:
										object_list = join_table[triples_map_inner.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)][child_list_value(predicate_object_map.object_map.child,row)]
									else:
										object_list = []
								object = None
						else:
							if predicate_object_map.object_map.parent != None:
								if predicate_object_map.object_map.parent[0] != predicate_object_map.object_map.child[0]:
									if (triples_map_inner.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)) not in join_table:
										with open(str(triples_map_inner.data_source), "r") as input_file_descriptor:
											if str(triples_map_inner.file_format).lower() == "csv":
												parent_data = csv.DictReader(input_file_descriptor, delimiter=delimiter)
												hash_maker_list(parent_data, triples_map_inner, predicate_object_map.object_map)
											else:
												parent_data = json.load(input_file_descriptor)
												if isinstance(parent_data, list):
													hash_maker_list(parent_data, triples_map_inner, predicate_object_map.object_map)
												else:
													hash_maker_list(parent_data[list(parent_data.keys())[0]], triples_map_inner, predicate_object_map.object_map)
									if sublist(predicate_object_map.object_map.child,row.keys()):
										if child_list_value(predicate_object_map.object_map.child,row) in join_table[triples_map_inner.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)]:
											object_list = join_table[triples_map_inner.triples_map_id + "_" + child_list(predicate_object_map.object_map.child)][child_list_value(predicate_object_map.object_map.child,row)]
										else:
											object_list = []
									object = None
								else:
									try:
										object = "<" + string_substitution(triples_map_inner.subject_map.value, "{(.+?)}", row, "object","yes", triples_map.iterator) + ">"
									except TypeError:
										object = None
							else:
								try:
									object = "<" + string_substitution(triples_map_inner.subject_map.value, "{(.+?)}", row, "object","yes", triples_map.iterator) + ">"
								except TypeError:
									object = None
						break
					else:
						continue
			else:
				object = None
		elif "quoted triples map" in predicate_object_map.object_map.mapping_type:
			for triples_map_element in triples_map_list:
				if triples_map_element.triples_map_id == predicate_object_map.object_map.value:
					if triples_map_element.data_source != triples_map.data_source:
						if predicate_object_map.object_map.parent[0] != None:
							if ("quoted_" + triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]) not in join_table:
								if str(triples_map_element.file_format).lower() == "csv" or triples_map_element.file_format == "JSONPath":
									with open(str(triples_map_element.data_source), "r") as input_file_descriptor:
										if str(triples_map_element.file_format).lower() == "csv":
											data = csv.DictReader(input_file_descriptor, delimiter=',')
											inner_hash_maker(data, triples_map_element, predicate_object_map.object_map, "quoted", triples_map_list)
										else:
											pass
							if row[predicate_object_map.object_map.child[0]] in join_table["quoted_" + triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]]:
								object_list = join_table["quoted_" + triples_map_element.triples_map_id + "_" + predicate_object_map.object_map.child[0]][row[predicate_object_map.object_map.child[0]]]
					else:
						object_list = inner_semantify_file(triples_map_element, triples_map_list, delimiter, row, base)
					object = None
		else:
			object = None
		if predicate != None and object != None and subject != None:
			for graph in triples_map.subject_map.graph:
				triple = subject + " " + predicate + " " + object 
				if graph != None and "defaultGraph" not in graph:
					if "{" in graph:
						triple = triple[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject","yes", triples_map.iterator) + ">"
					else:
						triple = triple[:-2] + " <" + graph + ">"
				triples_list.append(triple)
			if predicate[1:-1] in predicate_object_map.graph:
				triple = subject + " " + predicate + " " + object 
				if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in predicate_object_map.graph[predicate[1:-1]]:
					if "{" in predicate_object_map.graph[predicate[1:-1]]:
						triple = triple[:-2] + " <" + string_substitution(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, "subject","yes", triples_map.iterator) + ">"
					else:
						triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">"
					triples_list.append(triple)
		elif predicate != None and subject != None and object_list:
			for obj in object_list:
				if obj != None:
					for graph in triples_map.subject_map.graph:
						if predicate_object_map.object_map.term != None:
							if "IRI" in predicate_object_map.object_map.term:
								triple = subject + " " + predicate + " <" + obj[1:-1] + ">"
							else:
								triple = subject + " " + predicate + " " + obj 
						else:
							if "quoted triples map" in predicate_object_map.object_map.mapping_type:
								triple = subject + " " + predicate + " <<" + obj + ">>"
							else:
								triple = subject + " " + predicate + " " + obj 
						if graph != None and "defaultGraph" not in graph:
							if "{" in graph:
								triple = triple[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject","yes", triples_map.iterator) + ">"
							else:
								triple = triple[:-2] + " <" + graph + ">"
						triples_list.append(triple)

					if predicate[1:-1] in predicate_object_map.graph:
						if predicate_object_map.object_map.term != None:
							if "IRI" in predicate_object_map.object_map.term:
								triple = subject + " " + predicate + " <" + obj[1:-1] + ">"
							else:
								triple = subject + " " + predicate + " " + obj 
						else:
							triple = subject + " " + predicate + " " + obj 
						if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in predicate_object_map.graph[predicate[1:-1]]:
							if "{" in predicate_object_map.graph[predicate[1:-1]]:
								triple = triple[:-2] + " <" + string_substitution(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, "subject","yes", triples_map.iterator) + ">"
							else:
								triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">"
							triples_list.append(triple)
			object_list = []
		elif predicate != None and object != None and subject_list:
			for subj in subject_list:
				if subj != None:
					for graph in triples_map.subject_map.graph:
						triple = "<< " + subj + ">> " + predicate + " " + object 
						if graph != None and "defaultGraph" not in graph:
							if "{" in graph:
								triple = triple[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject","yes", triples_map.iterator) + ">"
							else:
								triple = triple[:-2] + " <" + graph + ">"
						triples_list.append(triple)

					if predicate[1:-1] in predicate_object_map.graph:
						triple = "<< " + subj + " >> " + predicate + " " + object 
						if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in predicate_object_map.graph[predicate[1:-1]]:
							if "{" in predicate_object_map.graph[predicate[1:-1]]:
								triple = triple[:-2] + " <" + string_substitution(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, "subject","yes", triples_map.iterator) + ">"
							else:
								triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">"
							triples_list.append(triple)
			subject_list = []
		elif predicate != None and object_list and subject_list:
			for subj in subject_list:
				for obj in object_list:
					if subj != None:
						for graph in triples_map.subject_map.graph:
							if "quoted triples map" in predicate_object_map.object_map.mapping_type:
								triple = "<< " + subj + ">> " + predicate + " <<" + obj + ">>" 
							else:
								triple = "<< " + subj + ">> " + predicate + " " + obj 
							if graph != None and "defaultGraph" not in graph:
								if "{" in graph:
									triple = triple[:-2] + " <" + string_substitution(graph, "{(.+?)}", row, "subject","yes", triples_map.iterator) + ">"
								else:
									triple = triple[:-2] + " <" + graph + ">"
							triples_list.append(triple)

						if predicate[1:-1] in predicate_object_map.graph:
							triple = "<< " + subj + ">> " + predicate + " " + obj 
							if predicate_object_map.graph[predicate[1:-1]] != None and "defaultGraph" not in predicate_object_map.graph[predicate[1:-1]]:
								if "{" in predicate_object_map.graph[predicate[1:-1]]:
									triple = triple[:-2] + " <" + string_substitution(predicate_object_map.graph[predicate[1:-1]], "{(.+?)}", row, "subject","yes", triples_map.iterator) + ">"
								else:
									triple = triple[:-2] + " <" + predicate_object_map.graph[predicate[1:-1]] + ">"
			subject_list = []
			object_list = []
		else:
			continue
	return triples_list