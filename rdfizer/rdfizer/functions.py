import os
import re
import datetime
import sys
import xml.etree.ElementTree as ET
import urllib
import math

def encode_char(string):
	encoded = ""
	valid_char = ["~",":","#"]
	for s in string:
		if s in valid_char:
			encoded += s
		else:
			encoded += urllib.parse.quote(s)
	return encoded

def combine_sublist(sublists, full_list):
	if sublists:
		i = 100000
		sl = []
		aux = []
		for sublist in sublists:
			if len(sublist) < i:
				i = len(sublist)
				sl = sublist
			else:
				aux.append(sublist)
		for sublist in sublists:
			if sublist not in aux and sublist != sl:
				aux.append(sublist)
		for source in sl:
			full_list[source] = ""
		return combine_sublist(aux, full_list)
	else:
		return full_list

def fully_sorted(source_list, sorted_list):
	for source in source_list:
		if source not in sorted_list:
			return True
	return False

def extract_min_tm(source_list,sorted_list):
	i = 1000
	min_key = ""
	for source in source_list:
		if len(source_list[source]) < i and source not in sorted_list:
			i = len(source_list[source])
			min_key = source
	return min_key, source_list[min_key]

def tm_interception(source1, source2):
	interception = 0
	for predicate in source1:
		if predicate in source2:
			interception += 1
	return interception

def source_sort(source_list, sorted_list, sublists):
	sublist = []
	if fully_sorted(source_list, sorted_list):
		min_key, min_value = extract_min_tm(source_list,sorted_list)
		sorted_list[min_key] = ""
		sublist.append(min_key)
		for source in source_list:
			interception = tm_interception(min_value, source_list[source])
			if 0 < interception:
				sorted_list[source] = ""
				sublist.append(source)
		sublists.append(sublist)	
		return source_sort(source_list, sorted_list, sublists)
	else:
		return combine_sublist(sublists, {})

def files_sort(triples_map_list, ordered):
	predicate_list = {}
	sorted_list = {}
	order_list = {}
	source_predicate = {}
	general_predicates = {"http://www.w3.org/2000/01/rdf-schema#subClassOf":"",
						"http://www.w3.org/2002/07/owl#sameAs":"",
						"http://www.w3.org/2000/01/rdf-schema#seeAlso":"",
						"http://www.w3.org/2000/01/rdf-schema#subPropertyOf":""}
	for tp in triples_map_list:
		if str(tp.file_format).lower() == "csv":
			if "csv" not in sorted_list:
				sorted_list["csv"] = {str(tp.data_source) : {tp.triples_map_id : tp}}
			else:
				if str(tp.data_source) in sorted_list["csv"]:
					sorted_list["csv"][str(tp.data_source)][tp.triples_map_id] = tp
				else:
					sorted_list["csv"][str(tp.data_source)] = {tp.triples_map_id : tp}
			for po in tp.predicate_object_maps_list:
				if po.predicate_map.value in general_predicates:
					predicate = po.predicate_map.value + "_" + po.object_map.value
					if predicate in predicate_list:
						predicate_list[predicate] += 1
					else:
						predicate_list[predicate] = 1
				else:
					if po.predicate_map.value in predicate_list:
						predicate_list[po.predicate_map.value] += 1
					else:
						predicate_list[po.predicate_map.value] = 1
				if "csv" not in source_predicate:
					if po.predicate_map.value in general_predicates:
						predicate = po.predicate_map.value + "_" + po.object_map.value
						source_predicate["csv"] = {str(tp.data_source) : {predicate : ""}}
					else:
						source_predicate["csv"] = {str(tp.data_source) : {po.predicate_map.value : ""}}
				else:
					if str(tp.data_source) in source_predicate["csv"]:
						if po.predicate_map.value in general_predicates:
							predicate = po.predicate_map.value + "_" + po.object_map.value
							source_predicate["csv"][str(tp.data_source)][predicate] = ""
						else:
							source_predicate["csv"][str(tp.data_source)][po.predicate_map.value] = ""
					else:
						if po.predicate_map.value in general_predicates:
							predicate = po.predicate_map.value + "_" + po.object_map.value
							source_predicate["csv"][str(tp.data_source)] = {predicate : ""}
						else:
							source_predicate["csv"][str(tp.data_source)] = {po.predicate_map.value : ""} 
		elif tp.file_format == "JSONPath":
			if "JSONPath" not in sorted_list:
				sorted_list["JSONPath"] = {str(tp.data_source) : {tp.triples_map_id : tp}}
			else:
				if str(tp.data_source) in sorted_list["JSONPath"]:
					sorted_list["JSONPath"][str(tp.data_source)][tp.triples_map_id] = tp
				else:
					sorted_list["csv"][str(tp.data_source)] = {tp.triples_map_id : tp}
			for po in tp.predicate_object_maps_list:
				if po.predicate_map.value in general_predicates:
					predicate = po.predicate_map.value + "_" + po.object_map.value
					if predicate in predicate_list:
						predicate_list[predicate] += 1
					else:
						predicate_list[predicate] = 1
				else:
					if po.predicate_map.value in predicate_list:
						predicate_list[po.predicate_map.value] += 1
					else:
						predicate_list[po.predicate_map.value] = 1
				if "JSONPath" not in source_predicate:
					if po.predicate_map.value in general_predicates:
						predicate = po.predicate_map.value + "_" + po.object_map.value
						source_predicate["JSONPath"] = {str(tp.data_source) : {predicate : ""}}
					else:
						source_predicate["JSONPath"] = {str(tp.data_source) : {po.predicate_map.value : ""}}
				else:
					if str(tp.data_source) in source_predicate["JSONPath"]:
						if po.predicate_map.value in general_predicates:
							predicate = po.predicate_map.value + "_" + po.object_map.value
							source_predicate["JSONPath"][str(tp.data_source)][predicate] = ""
						else:
							source_predicate["JSONPath"][str(tp.data_source)][po.predicate_map.value] = ""
					else:
						if po.predicate_map.value in general_predicates:
							predicate = po.predicate_map.value + "_" + po.object_map.value
							source_predicate["JSONPath"][str(tp.data_source)] = {predicate : ""}
						else:
							source_predicate["JSONPath"][str(tp.data_source)] = {po.predicate_map.value : ""}  
		elif tp.file_format == "XPath":
			if "XPath" not in sorted_list:
				sorted_list["XPath"] = {str(tp.data_source) : {tp.triples_map_id : tp}}
			else:
				if str(tp.data_source) in sorted_list["XPath"]:
					sorted_list["XPath"][str(tp.data_source)][tp.triples_map_id] = tp
				else:
					sorted_list["XPath"][str(tp.data_source)] = {tp.triples_map_id : tp} 
			for po in tp.predicate_object_maps_list:
				if po.predicate_map.value in general_predicates:
					predicate = po.predicate_map.value + "_" + po.object_map.value
					if predicate in predicate_list:
						predicate_list[predicate] += 1
					else:
						predicate_list[predicate] = 1
				else:
					if po.predicate_map.value in predicate_list:
						predicate_list[po.predicate_map.value] += 1
					else:
						predicate_list[po.predicate_map.value] = 1
				if "XPath" not in source_predicate:
					if po.predicate_map.value in general_predicates:
						predicate = po.predicate_map.value + "_" + po.object_map.value
						source_predicate["XPath"] = {str(tp.data_source) : {predicate : ""}}
					else:
						source_predicate["XPath"] = {str(tp.data_source) : {po.predicate_map.value : ""}}
				else:
					if str(tp.data_source) in source_predicate["XPath"]:
						if po.predicate_map.value in general_predicates:
							predicate = po.predicate_map.value + "_" + po.object_map.value
							source_predicate["XPath"][str(tp.data_source)][predicate] = ""
						else:
							source_predicate["XPath"][str(tp.data_source)][po.predicate_map.value] = ""
					else:
						if po.predicate_map.value in general_predicates:
							predicate = po.predicate_map.value + "_" + po.object_map.value
							source_predicate["XPath"][str(tp.data_source)] = {predicate : ""}
						else:
							source_predicate["XPath"][str(tp.data_source)] = {po.predicate_map.value : ""} 
		if tp.subject_map.rdf_class is not None:
			for rdf_type in tp.subject_map.rdf_class:
				predicate = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" + "_" + "<{}>".format(rdf_type)
				if predicate in predicate_list:
					predicate_list[predicate] += 1
				else:
					predicate_list[predicate] = 1
	if ordered.lower() == "yes":
		for source in sorted_list:
			order_list[source] = source_sort(source_predicate[source], {}, [])
	return sorted_list, predicate_list, order_list

def base36encode(number, alphabet='0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'):
    """Converts an integer to a base36 string."""
    

    base36 = ''
    sign = ''

    if number < 0:
        sign = '-'
        number = -number

    if 0 <= number < len(alphabet):
        return sign + alphabet[number]

    while number != 0:
        number, i = divmod(number, len(alphabet))
        base36 = alphabet[i] + base36

    return sign + base36

def sublist(part_list, full_list):
	for part in part_list:
		if part not in full_list:
			return False
	return True

def child_list(childs):
	value = ""
	for child in childs:
		if child not in value:
			value += child + "_"
	return value[:-1]

def child_list_value(childs,row):
	value = ""
	v = []
	for child in childs:
		if child not in v:
			if row[child] != None:
				value += row[child] + "_"
				v.append(child)
	return value[:-1]

def child_list_value_array(childs,row,row_headers):
	value = ""
	v = []
	for child in childs:
		if child not in v:
			if row[row_headers.index(child)] != None:
				value += row[row_headers.index(child)] + "_"
				v.append(child)
	return value[:-1]

def string_substitution_json(string, pattern, row, term, ignore, iterator):

	template_references = re.finditer(pattern, string)
	new_string = string
	offset_current_substitution = 0
	if iterator != "None" and "" != iterator:
		if iterator != "$.[*]":
			temp_keys = iterator.split(".")
			for tp in temp_keys:
				if "$" != tp:
					if "[*]" in tp:
						row = row[tp.split("[*]")[0]]
					else:
						row = row[tp]
	for reference_match in template_references:
		start, end = reference_match.span()[0], reference_match.span()[1]
		if pattern == "{(.+?)}":
			match = reference_match.group(1).split("[")[0]
			if "\\" in match:
				temp = match.split("{")
				match = temp[len(temp)-1]
				if match in row.keys():
					value = row[match]
				else:
					print('The attribute ' + match + ' is missing.')
					if ignore == "yes":
						return None
					print('Aborting...')
					sys.exit(1)

			elif "." in match:
				if match in row:
					value = row[match]
				else:
					temp = match.split(".")
					if "[*]" in temp[0]:
						value = row[temp[0].split("[*]")[0]]
					else:
						value = row[temp[0]]
					for t in temp:
						if t != temp[0]:
							value = value[t]
									
			else:
				if match in row:
					value = row[match]
				else:
					return None
			
			if value is not None:
				if (type(value).__name__) != "str":
					if (type(value).__name__) != "float":
						value = str(value)
					else:
						value = str(math.ceil(value))
				else:
					if re.match(r'^-?\d+(?:\.\d+)$', value) is not None:
						value = str(math.ceil(float(value))) 
				if re.search("^[\s|\t]*$", value) is None:
					value = encode_char(value)
					new_string = new_string[:start + offset_current_substitution] + value.strip() + new_string[ end + offset_current_substitution:]
					offset_current_substitution = offset_current_substitution + len(value) - (end - start)
					if "\\" in new_string:
						new_string = new_string.replace("\\", "")
						count = new_string.count("}")
						i = 0
						while i < count:
							new_string = "{" + new_string
							i += 1
						new_string = new_string.replace(" ", "")

				else:
					return None
			else:
				return None

		elif pattern == ".+":
			match = reference_match.group(0)
			if "." in match:
				if match in row:
					value = row[match]
				else:
					temp = match.split(".")
					value = row[temp[0]]
					for element in temp:
						if element in value:
							value = value[element]
			else:
				if match in row:
					value = row[match]
				else:
					return None

			if match is not None:
				if (type(value).__name__) == "int":
						value = str(value)
				if isinstance(value, dict):
					if value:
						print("Index needed")
						return None
					else:
						return None
				else:		
					if value is not None:
						if re.search("^[\s|\t]*$", value) is None:
							new_string = new_string[:start] + value.strip().replace("\"", "'") + new_string[end:]
							new_string = "\"" + new_string + "\"" if new_string[0] != "\"" and new_string[-1] != "\"" else new_string
						else:
							return None
			else:
				return None
		else:
			print("Invalid pattern")
			if ignore == "yes":
				return None
			print("Aborting...")
			sys.exit(1)

	return new_string

def string_substitution_xml(string, pattern, row, term):
	template_references = re.finditer(pattern, string)
	new_string = string
	offset_current_substitution = 0
	for reference_match in template_references:
		start, end = reference_match.span()[0], reference_match.span()[1]
		if pattern == "{(.+?)}":
			match = reference_match.group(1).split("[")[0]
			if row.attrib:
				if "@" in match:
					match = match.split("@")[1]
				if row.attrib[match] is not None:
					if re.search("^[\s|\t]*$", row[match]) is None:
						if "http" not in row[match] and "http" in new_string[:start + offset_current_substitution]:
							new_string = new_string[:start + offset_current_substitution] + encode_char(row[match].strip()) + new_string[ end + offset_current_substitution:]
						else:
							new_string = new_string[:start + offset_current_substitution] + row[match].strip() + new_string[ end + offset_current_substitution:]
						offset_current_substitution = offset_current_substitution + len(row[match]) - (end - start)

					else:
						return None
			else:
				if row.find(match) is not None:
					if re.search("^[\s|\t]*$", row.find(match).text) is None:
						new_string = new_string[:start + offset_current_substitution] + encode_char(row.find(match).text.strip()) + new_string[ end + offset_current_substitution:]
						offset_current_substitution = offset_current_substitution + len(encode_char(row.find(match).text.strip())) - (end - start)

					else:
						return None
				else:
					return None


		elif pattern == ".+":
			match = reference_match.group(0)
			if row.attrib:
				if "@" in match:
					match = match.split("@")[1]
					if row.attrib[match] is not None:
						if re.search("^[\s|\t]*$", row.attrib[match]) is None:
							new_string = new_string[:start + offset_current_substitution] + "\"" + row.attrib[match].strip() + "\"" + new_string[ end + offset_current_substitution:]
							offset_current_substitution = offset_current_substitution + len(row.attrib[match]) - (end - start)

						else:
							return None
				else:
					if re.search("^[\s|\t]*$", row.find(match).text) is None:
						new_string = new_string[:start + offset_current_substitution] + "\"" + row.find(match).text.strip() + "\"" + new_string[ end + offset_current_substitution:]
						offset_current_substitution = offset_current_substitution + len(row.find(match).text.strip()) - (end - start)

					else:
						return None
			else:
				if row.find(match) is not None:
					if re.search("^[\s|\t]*$", row.find(match).text) is None:
						new_string = new_string[:start + offset_current_substitution] + "\"" + row.find(match).text.strip() + "\"" + new_string[ end + offset_current_substitution:]
						offset_current_substitution = offset_current_substitution + len(row.find(match).text.strip()) - (end - start)

					else:
						return None
				else:
					return None
		else:
			print("Invalid pattern")
			print("Aborting...")
			sys.exit(1)

	return new_string

def string_substitution(string, pattern, row, term, ignore, iterator):

	"""
	(Private function, not accessible from outside this package)

	Takes a string and a pattern, matches the pattern against the string and perform the substitution
	in the string from the respective value in the row.

	Parameters
	----------
	string : string
		String to be matched
	triples_map_list : string
		Pattern containing a regular expression to match
	row : dictionary
		Dictionary with CSV headers as keys and fields of the row as values

	Returns
	-------
	A string with the respective substitution if the element to be subtitued is not invalid
	(i.e.: empty string, string with just spaces, just tabs or a combination of both), otherwise
	returns None
	"""
	template_references = re.finditer(pattern, string)
	new_string = string
	offset_current_substitution = 0
	if iterator != "None":
		if iterator != "$.[*]":
			temp_keys = iterator.split(".")
			for tp in temp_keys:
				if "$" != tp and tp in row:
					if "[*]" in tp:
						row = row[tp.split("[*]")[0]]
					else:
						row = row[tp]
	for reference_match in template_references:
		start, end = reference_match.span()[0], reference_match.span()[1]
		if pattern == "{(.+?)}":
			no_match = True
			if "]." in reference_match.group(1):
				temp = reference_match.group(1).split("].")
				match = temp[1]
				condition = temp[0].split("[")
				temp_value = row[condition[0]]
				if "==" in condition[1]:
					temp_condition = condition[1][2:-1].split("==")
					iterators = temp_condition[0].split(".")
					if isinstance(temp_value,list):
						for tv in temp_value:
							t_v = tv
							for cond in iterators[:-1]:
								if cond != "@":
									t_v = t_v[cond]
							if temp_condition[1][1:-1] == t_v[iterators[-1]]:
								row = t_v
								no_match = False
					else:
						for cond in iterators[-1]:
							if cond != "@":
								temp_value = temp_value[cond]
						if temp_condition[1][1:-1] == temp_value[iterators[-1]]:
							row = temp_value
							no_match = False
				elif "!=" in condition[1]:
					temp_condition = condition[1][2:-1].split("!=")
					iterators = temp_condition[0].split(".")
					match = iterators[-1]
					if isinstance(temp_value,list):
						for tv in temp_value:
							for cond in iterators[-1]:
								if cond != "@":
									temp_value = temp_value[cond]
							if temp_condition[1][1:-1] != temp_value[iterators[-1]]:
								row = t_v
								no_match = False
					else:
						for cond in iterators[-1]:
							if cond != "@":
								temp_value = temp_value[cond]
						if temp_condition[1][1:-1] != temp_value[iterators[-1]]:
							row = temp_value
							no_match = False
				if no_match:
					return None
			else:
				match = reference_match.group(1).split("[")[0]
			if "\\" in match:
				temp = match.split("{")
				match = temp[len(temp)-1]
			if "." in match:
				if match not in row.keys():
					temp_keys = match.split(".")
					match = temp_keys[len(temp_keys) - 1]
					for tp in temp_keys[:-1]:
						if tp in row:
							row = row[tp]
						else:
							return None
			if row == None:
				return None
			if match in row.keys():
				if row[match] != None:
					if (type(row[match]).__name__) != "str":
						if "float" not in (type(row[match]).__name__):
							row[match] = str(row[match])
						else:
							row[match] = str(math.ceil(row[match]))
					else:
						if re.match(r'^-?\d+(?:\.\d+)$', row[match]) is not None:
							row[match] = str(math.ceil(float(row[match])))
					if isinstance(row[match],dict):
						print("The key " + match + " has a Json structure as a value.\n")
						print("The index needs to be indicated.\n")
						return None
					else:
						if re.search("^[\s|\t]*$", row[match]) is None:
							value = row[match]
							if "http" not in value and "http" in new_string[:start + offset_current_substitution]:
								value = encode_char(value)
							new_string = new_string[:start + offset_current_substitution] + value.strip() + new_string[ end + offset_current_substitution:]
							offset_current_substitution = offset_current_substitution + len(value) - (end - start)
							if "\\" in new_string:
								new_string = new_string.replace("\\", "")
								count = new_string.count("}")
								i = 0
								while i < count:
									new_string = "{" + new_string
									i += 1
								new_string = new_string.replace(" ", "")

						else:
							return None
				else:
					return None
			else:
				print('The attribute ' + match + ' is missing.')
				if ignore == "yes":
					return None
				print('Aborting...')
				sys.exit(1)
		elif pattern == ".+":
			match = reference_match.group(0)
			if "." in match:
				if match not in row.keys():
					temp_keys = match.split(".")
					match = temp_keys[len(temp_keys) - 1]
					for tp in temp_keys[:-1]:
						if tp in row:
							row = row[tp]
						else:
							return None
			if row == None:
				return None
			if match in row.keys():
				if (type(row[match]).__name__) != "str":
						row[match] = str(row[match])
				if isinstance(row[match],dict):
					print("The key " + match + " has a Json structure as a value.\n")
					print("The index needs to be indicated.\n")
					return None
				else:
					if row[match] != None:
						if re.search("^[\s|\t]*$", row[match]) is None:
							new_string = new_string[:start] + row[match].strip().replace("\"", "'") + new_string[end:]
							new_string = "\"" + new_string + "\"" if new_string[0] != "\"" and new_string[-1] != "\"" else new_string
						else:
							return None
					else:
						return None
			else:
				print('The attribute ' + match + ' is missing.')
				if ignore == "yes":
					return None
				print('Aborting...')
				sys.exit(1)
		else:
			print("Invalid pattern")
			if ignore == "yes":
				return None
			print("Aborting...")
			sys.exit(1)
	return new_string

def string_substitution_array(string, pattern, row, row_headers, term, ignore):

	"""
	(Private function, not accessible from outside this package)

	Takes a string and a pattern, matches the pattern against the string and perform the substitution
	in the string from the respective value in the row.

	Parameters
	----------
	string : string
		String to be matched
	triples_map_list : string
		Pattern containing a regular expression to match
	row : dictionary
		Dictionary with CSV headers as keys and fields of the row as values

	Returns
	-------
	A string with the respective substitution if the element to be subtitued is not invalid
	(i.e.: empty string, string with just spaces, just tabs or a combination of both), otherwise
	returns None
	"""

	template_references = re.finditer(pattern, string)
	new_string = string
	offset_current_substitution = 0
	for reference_match in template_references:
		start, end = reference_match.span()[0], reference_match.span()[1]
		if pattern == "{(.+?)}":
			match = reference_match.group(1).split("[")[0]
			if "\\" in match:
				temp = match.split("{")
				match = temp[len(temp)-1]
			if match in row_headers:
				if row[row_headers.index(match)] is not None:
					value = row[row_headers.index(match)]
					if (type(value).__name__) != "str":
						if (type(value).__name__) != "float":
							value = str(value)
						else:
							value = str(math.ceil(value))
					else:
						if re.match(r'^-?\d+(?:\.\d+)$', value) is not None:
							value = str(math.ceil(float(value)))
					if re.search("^[\s|\t]*$", value) is None:
						if "http" not in value and "http" in new_string[:start + offset_current_substitution]:
							value = encode_char(value)
						new_string = new_string[:start + offset_current_substitution] + value.strip() + new_string[ end + offset_current_substitution:]
						offset_current_substitution = offset_current_substitution + len(value) - (end - start)
						if "\\" in new_string:
							new_string = new_string.replace("\\", "")
							count = new_string.count("}")
							i = 0
							while i < count:
								new_string = "{" + new_string
								i += 1
							new_string = new_string.replace(" ", "")

					else:
						return None
			else:
				print('The attribute ' + match + ' is missing.')
				if ignore == "yes":
					return None
				print('Aborting...')
				sys.exit(1)
				return
				# To-do:
				# Generate blank node when subject in csv is not a valid string (empty string, just spaces, just tabs or a combination of the last two)
				#if term == "subject":
				#	new_string = new_string[:start + offset_current_substitution] + str(uuid.uuid4()) + new_string[end + offset_current_substitution:]
				#	offset_current_substitution = offset_current_substitution + len(row[match]) - (end - start)
				#else:
				#	return None
		elif pattern == ".+":
			match = reference_match.group(0)
			if match in row_headers:
				if row[row_headers.index(match)] is not None:
					value = row[row_headers.index(match)]
					if type(value).__name__ == "date":
						value = value.strftime("%Y-%m-%d")
					elif type(value).__name__ == "datetime":
						value = value.strftime("%Y-%m-%d T%H:%M:%S")
					elif type(value).__name__ != "str":
						value = str(value)

					if re.search("^[\s|\t]*$", str(value)) is None:
						new_string = new_string[:start] + str(value).strip().replace("\"", "'") + new_string[end:]
						new_string = "\"" + new_string + "\"" if new_string[0] != "\"" and new_string[-1] != "\"" else new_string
					else:
						return None
				else:
					return None
			else:
				print('The attribute ' + match + ' is missing.')
				if ignore == "yes":
					return None
				print('Aborting...')
				sys.exit(1)
				return
		else:
			print("Invalid pattern")
			if ignore == "yes":
				return None
			print("Aborting...")
			sys.exit(1)

	return new_string


def string_substitution_postgres(string, pattern, row, row_headers, term, ignore):

	"""
	(Private function, not accessible from outside this package)

	Takes a string and a pattern, matches the pattern against the string and perform the substitution
	in the string from the respective value in the row.

	Parameters
	----------
	string : string
		String to be matched
	triples_map_list : string
		Pattern containing a regular expression to match
	row : dictionary
		Dictionary with CSV headers as keys and fields of the row as values

	Returns
	-------
	A string with the respective substitution if the element to be subtitued is not invalid
	(i.e.: empty string, string with just spaces, just tabs or a combination of both), otherwise
	returns None
	"""

	template_references = re.finditer(pattern, string)
	new_string = string
	offset_current_substitution = 0
	for reference_match in template_references:
		start, end = reference_match.span()[0], reference_match.span()[1]
		if pattern == "{(.+?)}":
			match = reference_match.group(1).split("[")[0]
			match = match.lower()
			if match in row_headers:
				if row[row_headers.index(match)] != None:
					value = row[row_headers.index(match)]
					if (type(value).__name__) != "str":
						if (type(value).__name__) != "float":
							value = str(value)
						else:
							value = str(math.ceil(value))
					else:
						if re.match(r'^-?\d+(?:\.\d+)$', value) is not None:
							value = str(math.ceil(float(value)))
					if re.search("^[\s|\t]*$", value) is None:
						if "http" not in value and "http" in new_string[:start + offset_current_substitution]:
							value = encode_char(value)
						new_string = new_string[:start + offset_current_substitution] + value.strip() + new_string[ end + offset_current_substitution:]
						offset_current_substitution = offset_current_substitution + len(value) - (end - start)
					else:
						return None
				else:
					return None
			else:
				print('The attribute ' + match + ' is missing.')
				if ignore == "yes":
					return None
				print('Aborting...')
				sys.exit(1)
				return
				# To-do:
				# Generate blank node when subject in csv is not a valid string (empty string, just spaces, just tabs or a combination of the last two)
				#if term == "subject":
				#	new_string = new_string[:start + offset_current_substitution] + str(uuid.uuid4()) + new_string[end + offset_current_substitution:]
				#	offset_current_substitution = offset_current_substitution + len(row[match]) - (end - start)
				#else:
				#	return None
		elif pattern == ".+":
			match = reference_match.group(0)
			match = match.lower()
			if match in row_headers:
				if row[row_headers.index(match)] is not None:
					value = row[row_headers.index(match)]
					if type(value) is int or ((type(value).__name__) == "float"):
						value = str(value)
					elif type(value).__name__ == "date":
						value = value.strftime("%Y-%m-%d")
					elif type(value).__name__ == "datetime":
						value = value.strftime("%Y-%m-%d T%H:%M:%S")

					if re.search("^[\s|\t]*$", value) is None:
						new_string = new_string[:start] + value.strip().replace("\"", "'") + new_string[end:]
						new_string = "\"" + new_string + "\"" if new_string[0] != "\"" and new_string[-1] != "\"" else new_string
					else:
						return None
				else:
					return None
			else:
				print('The attribute ' + match + ' is missing.')
				if ignore == "yes":
					return None
				print('Aborting...')
				sys.exit(1)
				return
		else:
			print("Invalid pattern")
			if ignore == "yes":
				return None
			print("Aborting...")
			sys.exit(1)

	return new_string

def shared_items(dic1, dic2):
	i = 0
	for key in dic1.keys():
		if dic2[key] is not None:
			if dic1[key] == dic2[key]:
				i += 1
	return i


def dictionary_maker_array(row, row_headers):
	dic = {}
	for key in row_headers:
		dic[key] = row[row_headers.index(key)]
	return dic


def dictionary_maker_xml(row):
	dic = {}
	for child in row:
		if len(child) != 0:
			for c in child:
				for attr in c:
					if attr in dic:
						dic[attr].append(child.attrib[attr])
					else:	
						dic[attr] = [child.attrib[attr]]
		else:
			if child in dic:
				dic[child].append(child.text)
			else:	
				dic[child] = [child.text]
	return dic

def dictionary_maker(row):
	dic = {}
	for key, value in row.items():
		dic[key] = value
	return dic

def extract_name(string):
	name = ""
	i = len(string)
	while 0 < i:
		name = string[i-1] + name
		i -= 1
		if string[i-1] == "/":
			break
	name = name.split(".")[0]
	return name

def count_characters(string):
	count = 0
	for s in string:
		if s == "{":
			count += 1
	return count

def clean_URL_suffix(URL_suffix):
    cleaned_URL=""
    if "http" in URL_suffix:
    	return URL_suffix

    for c in URL_suffix:
        if c.isalpha() or c.isnumeric() or c =='_' or c=='-' or c == '(' or c == ')':
            cleaned_URL= cleaned_URL+c
        if c == "/" or c == "\\":
            cleaned_URL = cleaned_URL+"-"

    return cleaned_URL

def string_separetion(string):
	if ("{" in string) and ("[" in string):
		prefix = string.split("{")[0]
		condition = string.split("{")[1].split("}")[0]
		postfix = string.split("{")[1].split("}")[1]
		field = prefix + "*" + postfix
	elif "[" in string:
		return string, string
	else:
		return string, ""
	return string, condition

def condition_separetor(string):
	condition_field = string.split("[")
	field = condition_field[1][:len(condition_field[1])-1].split("=")[0]
	value = condition_field[1][:len(condition_field[1])-1].split("=")[1]
	return field, value