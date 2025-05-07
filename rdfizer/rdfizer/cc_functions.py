import io
import csv
import json
from jsonpath_ng import jsonpath, parse
from .functions import *
from uuid import uuid4

global gather_blank
gather_blank = 1
global cc_join_table
cc_join_table = {}

def hash_maker_cc(parent_data, parent_subject, parent, child, triples_map_list):
	ignore = "yes"
	duplicate = "yes"
	global blank_message
	hash_table = {}
	for row in parent_data:
		if parent in row.keys():
			if row[parent] != "" and row[parent] != None:
				if row[parent] in hash_table:
					if duplicate == "yes":
						if parent_subject.subject_map.subject_mapping_type == "reference":
							value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore,
														parent_subject.iterator)
							if value != None:
								if "http" in value and "<" not in value:
									value = "<" + value[1:-1] + ">"
								elif "http" in value and "<" in value:
									value = value[1:-1]
							if value not in hash_table[row[parent]]:
								hash_table[row[parent]].update({value: "object"})
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
									hash_table[row[parent]].update({value: "object"})
					else:
						if parent_subject.subject_map.subject_mapping_type == "reference":
							value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore,
														parent_subject.iterator)
							if "http" in value and "<" not in value:
								value = "<" + value[1:-1] + ">"
							elif "http" in value and "<" in value:
								value = value[1:-1]
							hash_table[row[parent]].update({value: "object"})
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
								hash_table[row[parent]].update({value: "object"})

				else:
					if parent_subject.subject_map.subject_mapping_type == "reference":
						value = string_substitution(parent_subject.subject_map.value, ".+", row, "object", ignore,
													parent_subject.iterator)
						if value != None:
							if "http" in value and "<" not in value:
								value = "<" + value[1:-1] + ">"
							elif "http" in value and "<" in value:
								value = value[1:-1]
						hash_table.update({row[parent]: {value: "object"}})
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
								hash_table.update({row[parent]: {value: "object"}})

	cc_join_table.update({parent_subject.triples_map_id + "_" + child : hash_table})

def gather_subject(data, subject, gather_map, output_file_descriptor, iterator):
	blank_id = str(uuid4())
	global gather_blank
	if "#Bag" not in gather_map.type and "#List" not in gather_map.type:
		rdf_type = subject + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + gather_map.type + "> .\n"
		output_file_descriptor.write(rdf_type)
	if "#Alt" in gather_map.type:
		for element in gather_map.gather_list:
			element_values = string_substitution_json(element,".+", data, "object", "yes", iterator)
			i = 1
			for value in element_values:
				triple = subject + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#_" + str(i) + "> \"" + value + "\" .\n"
				output_file_descriptor.write(triple)
				i += 1
	elif "#Bag" in gather_map.type:
		for element in gather_map.gather_list:
			element_values = string_substitution_json(element,".+", data, "object", "yes", iterator)
			if gather_map.empty != None and gather_map.empty:
				if element_values == []:
					rdf_type = subject + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + gather_map.type + "> .\n"
					output_file_descriptor.write(rdf_type)
				else:
					rdf_type = subject + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + gather_map.type + "> .\n"
					output_file_descriptor.write(rdf_type)
					i = 1
					for value in element_values:
						triple = subject + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#_" + str(i) + "> \"" + value + "\" .\n"
						output_file_descriptor.write(triple)
						i += 1
			else:
				if element_values != []:
					rdf_type = subject + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + gather_map.type + "> .\n"
					output_file_descriptor.write(rdf_type)
					i = 1
					for value in element_values:
						triple = subject + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#_" + str(i) + "> \"" + value + "\" .\n"
						output_file_descriptor.write(triple)
						i += 1
	elif "#List" in gather_map.type:
		for element in gather_map.gather_list:
			element_values = string_substitution_json(element,".+", data, "object", "yes", iterator)
			if gather_map.empty != None and gather_map.empty:
				if element_values == []:
					triple = subject + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .\n"
					output_file_descriptor.write(triple)
				else:
					for value in element_values:
						if value == element_values[0]:
							triple = subject + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> \"" + value + "\" .\n"
							output_file_descriptor.write(triple)
							triple = subject + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> " + "_:" + blank_id + str(gather_blank) + " .\n"
							output_file_descriptor.write(triple)
						elif value == element_values[len(element_values)-1]:
							triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> \"" + value + "\" .\n"
							output_file_descriptor.write(triple)
							triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>.\n"
							output_file_descriptor.write(triple)
							gather_blank += 1
						else:
							triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> \"" + value + "\" .\n"
							output_file_descriptor.write(triple)
							triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> " + "_:" + blank_id + str(gather_blank+1) + " .\n"
							output_file_descriptor.write(triple)
							gather_blank += 1
			else:
				if element_values != []:
					for value in element_values:
						if value == element_values[0]:
							triple = subject + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> \"" + value + "\" .\n"
							output_file_descriptor.write(triple)
							triple = subject + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> " + "_:" + blank_id + str(gather_blank) + " .\n"
							output_file_descriptor.write(triple)
						elif value == element_values[len(element_values)-1]:
							triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> \"" + value + "\" .\n"
							output_file_descriptor.write(triple)
							triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>.\n"
							output_file_descriptor.write(triple)
							gather_blank += 1
						else:
							triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> \"" + value + "\" .\n"
							output_file_descriptor.write(triple)
							triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> " + "_:" + blank_id + str(gather_blank+1) + " .\n"
							output_file_descriptor.write(triple)
							gather_blank += 1

	elif "#Seq" in gather_map.type:
		for element in gather_map.gather_list:
			element_values = string_substitution_json(element,".+", data, "object", "yes", iterator)
			i = 1
			for value in element_values:
				triple = subject + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#_" + str(i) + "> \"" + value + "\" .\n"
				output_file_descriptor.write(triple)
				i += 1
	else:
		return None

def gather_triples_generation(data, subject_predicate, base, gather_map, output_file_descriptor, iterator, triples_map_list, gather_map_list, graph):
	blank_id = str(uuid4())
	if gather_map.value != "None":
		object = string_substitution_json(gather_map.value, "{(.+?)}", data, "subject", "yes", iterator)
		if "http" in object:
			object = "<" + object + ">"
		else:
			if base != "":
				object = "<" + base +  object + ">"
			else:
				object = "<http://example.com/base/" + base +  object + ">"
	else:
		global gather_blank
		object = "_:" + blank_id + str(gather_blank)
		gather_blank += 1
	if "#Bag" not in gather_map.type and "#List" not in gather_map.type:
		triple = subject_predicate + " " + object + " .\n"
		output_file_descriptor.write(triple)
		rdf_type = object + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + gather_map.type + "> .\n"
		output_file_descriptor.write(rdf_type)
	if "#Alt" in gather_map.type:
		for element in gather_map.gather_list:
			element_values = string_substitution_json(element["value"],".+", data, "object", "yes", iterator)
			i = 1
			for value in element_values:
				triple = object + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#_" + str(i) + "> \"" + value + "\" .\n"
				output_file_descriptor.write(triple)
				i += 1
	elif "#Bag" in gather_map.type:
		element_values = []
		for element in gather_map.gather_list:
			if element["type"] == "reference":
				element_values += string_substitution_json(element["value"],".+", data, "object", "yes", iterator)
		if gather_map.empty != None and gather_map.empty:
			if element_values == []:
				if "" != subject_predicate:
					if graph == "":
						triple = subject_predicate + " " + object + " .\n"
					else:
						triple = subject_predicate + " " + object + " " + graph + " .\n"
					output_file_descriptor.write(triple)
				if graph == "":
					rdf_type = object + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + gather_map.type + "> .\n"
				else:
					rdf_type = object + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + gather_map.type + ">" + graph + " .\n"
				output_file_descriptor.write(rdf_type)
			else:
				if "" != subject_predicate:
					if graph == "":
						triple = subject_predicate + " " + object + " .\n"
					else:
						triple = subject_predicate + " " + object + " " + graph + " .\n"
					output_file_descriptor.write(triple)
				if graph == "":
					rdf_type = object + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + gather_map.type + "> .\n"
				else:
					rdf_type = object + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + gather_map.type + ">" + graph + " .\n"
				output_file_descriptor.write(rdf_type)
				i = 1
				for value in element_values:
					if graph == "":
						triple = object + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#_" + str(i) + "> \"" + value + "\" .\n"
					else:
						triple = object + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#_" + str(i) + "> \"" + value + "\" " + graph + ".\n"
					output_file_descriptor.write(triple)
					i += 1
		else:
			if element_values != []:
				if "" != subject_predicate:
					if graph == "":
						triple = subject_predicate + " " + object + " .\n"
					else:
						triple = subject_predicate + " " + object + " " + graph + " .\n"
					output_file_descriptor.write(triple)
				if graph == "":
					rdf_type = object + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + gather_map.type + "> .\n"
				else:
					rdf_type = object + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + gather_map.type + ">" + graph + " .\n"
				output_file_descriptor.write(rdf_type)
				i = 1
				for value in element_values:
					if graph == "":
						triple = object + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#_" + str(i) + "> \"" + value + "\" .\n"
					else:
						triple = object + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#_" + str(i) + "> \"" + value + "\" " + graph + ".\n"
					output_file_descriptor.write(triple)
					i += 1
	elif "#List" in gather_map.type:
		if "cartesian" not in gather_map.strategy:
			element_values = []
			for element in gather_map.gather_list:
				if element["type"] == "reference":
					element_values += string_substitution_json(element["value"],".+", data, "object", "yes", iterator)
				elif element["type"] == "gather map":
					element_values.append(gather_triples_generation(data, "", base, gather_map_list[element["value"]], output_file_descriptor, iterator, triples_map_list, gather_map_list,graph))
				elif element["type"] == "join":
					for tm in triples_map_list:
						if tm.triples_map_id == element["value"]:
							if element["child"] != "None" and element["child"] != "None":
								if tm.triples_map_id + "_" + element["child"] not in cc_join_table:
									with open(str(tm.data_source),"r") as input_file_descriptor:
										parent_data = csv.DictReader(input_file_descriptor, delimiter=",")
										hash_maker_cc(parent_data, tm, element["parent"], element["child"], triples_map_list)
								element_values += cc_join_table[tm.triples_map_id + "_" + element["child"]][data[element["child"]]]
							else:
								element_values.append("<" + string_substitution(tm.subject_map.value, "{(.+?)}", data, "subject", "yes", "") + ">")

			if gather_map.empty != None and gather_map.empty:
				if element_values == []:
					if "" != subject_predicate:
						if graph == "":
							triple = subject_predicate + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> .\n"
						else:
							triple = subject_predicate + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> " + graph + " .\n"
						output_file_descriptor.write(triple)
				else:
					if "" != subject_predicate:
						triple = subject_predicate + " " + object + " .\n"
						output_file_descriptor.write(triple)
					for value in element_values:
						if value == element_values[0]:
							if "_:" not in value and "http" not in value:
								triple = object + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> \"" + value + "\" .\n"
							else:
								triple = object + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> " + value + " .\n"
							if graph != "":
								triple = triple[:-2] + graph + " .\n"
							output_file_descriptor.write(triple)
							triple = object + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> " + "_:" + blank_id + str(gather_blank) + " .\n"
							if graph != "":
								triple = triple[:-2] + graph + " .\n"
							output_file_descriptor.write(triple)
						elif value == element_values[len(element_values)-1]:
							if "_:" not in value and "http" not in value:
								triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> \"" + value + "\" .\n"
							else:
								triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> " + value + " .\n"
							if graph != "":
								triple = triple[:-2] + graph + " .\n"
							output_file_descriptor.write(triple)
							triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>.\n"
							if graph != "":
								triple = triple[:-2] + graph + " .\n"
							output_file_descriptor.write(triple)
							gather_blank += 1
						else:
							if "_:" not in value and "http" not in value:
								triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> \"" + value + "\" .\n"
							else:
								triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> " + value + " .\n"
							if graph != "":
								triple = triple[:-2] + graph + " .\n"
							output_file_descriptor.write(triple)
							triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> " + "_:" + blank_id + str(gather_blank+1) + " .\n"
							if graph != "":
								triple = triple[:-2] + graph + " .\n"
							output_file_descriptor.write(triple)
							gather_blank += 1
			else:
				if element_values != []:
					if "" != subject_predicate:
						triple = subject_predicate + " " + object + " .\n"
						if graph != "":
							triple = triple[:-2] + graph + " .\n"
						output_file_descriptor.write(triple)
					for value in element_values:
						if value == element_values[0]:
							if "_:" not in value and "http" not in value:
								triple = object + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> \"" + value + "\" .\n"
							else:
								triple = object + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> " + value + " .\n"
							if graph != "":
								triple = triple[:-2] + graph + " .\n"
							output_file_descriptor.write(triple)
							triple = object + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> " + "_:" + blank_id + str(gather_blank) + " .\n"
							if graph != "":
								triple = triple[:-2] + graph + " .\n"
							output_file_descriptor.write(triple)
						elif value == element_values[len(element_values)-1]:
							if "_:" not in value and "http" not in value:
								triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> \"" + value + "\" .\n"
							else:
								triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> " + value + " .\n"
							if graph != "":
								triple = triple[:-2] + graph + " .\n"
							output_file_descriptor.write(triple)
							triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>.\n"
							if graph != "":
								triple = triple[:-2] + graph + " .\n"
							output_file_descriptor.write(triple)
							gather_blank += 1
						else:
							if "_:" not in value and "http" not in value:
								triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> \"" + value + "\" .\n"
							else:
								triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> " + value + " .\n"
							if graph != "":
								triple = triple[:-2] + graph + " .\n"
							output_file_descriptor.write(triple)
							triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> " + "_:" + blank_id + str(gather_blank+1) + " .\n"
							if graph != "":
								triple = triple[:-2] + graph + " .\n"
							output_file_descriptor.write(triple)
							gather_blank += 1
		else:
			list_right = string_substitution_json(gather_map.gather_list[0],".+", data, "object", "yes", iterator)
			list_left = string_substitution_json(gather_map.gather_list[1],".+", data, "object", "yes", iterator)
			for value_right in list_right:
				for value_left in list_left:
					if "" != subject_predicate:
						triple = subject_predicate + " " + object + " .\n"
						output_file_descriptor.write(triple)
					triple = object + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> \"" + value_right + "\" .\n"
					if graph != "":
						triple = triple[:-2] + graph + " .\n"
					output_file_descriptor.write(triple)
					triple = object + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> " + "_:" + blank_id + str(gather_blank) + " .\n"
					if graph != "":
						triple = triple[:-2] + graph + " .\n"
					output_file_descriptor.write(triple)
					triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> \"" + value_left + "\" .\n"
					if graph != "":
						triple = triple[:-2] + graph + " .\n"
					output_file_descriptor.write(triple)
					triple = "_:" + blank_id + str(gather_blank) + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>.\n"
					if graph != "":
						triple = triple[:-2] + graph + " .\n"
					output_file_descriptor.write(triple)
					gather_blank += 1

	elif "#Seq" in gather_map.type:
		for element in gather_map.gather_list:
			element_values = string_substitution_json(element,".+", data, "object", "yes", iterator)
			i = 1
			for value in element_values:
				triple = object + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#_" + str(i) + "> \"" + value + "\" .\n"
				output_file_descriptor.write(triple)
				i += 1
	else:
		return None
	return object

def grouping_values_json(gather_value,source,iterator):
	from jsonpath_ng import jsonpath, parse
	gather_rows = {}
	with open(source) as f:
		data = json.load(f)
		if ".*" in iterator:
			jsonpath_expr = parse(iterator.replace(".*",".[*]"))
		else:
			jsonpath_expr = parse(iterator)
		matches = [match.value for match in jsonpath_expr.find(data)]
		for element in matches:
			object = string_substitution_json(gather_value, "{(.+?)}", element, "subject", "yes", "")
			if object not in gather_rows:
				gather_rows[object] = element
			else:
				new_row = {}
				for key in gather_rows[object]:
					if gather_rows[object][key] == element[key]:
						new_row[key] = element[key]
					else:
						if isinstance(gather_rows[object][key],list) and isinstance(element[key],list):
							new_row[key] = gather_rows[object][key] + element[key]
						elif isinstance(gather_rows[object][key],list):
							new_row[key] = gather_rows[object][key] + [element[key]]
						elif isinstance(element[key],list):
							new_row[key] = [gather_rows[object][key]] + element[key]
						else:
							new_row[key] = [gather_rows[object][key]] + [element[key]]	
				gather_rows[object] = new_row
		return gather_rows