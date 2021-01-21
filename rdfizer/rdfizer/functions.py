import os
import re
import datetime
import sys
import xml.etree.ElementTree as ET
import urllib

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
			value += row[child] + "_"
	return value[:-1]

def child_list_value_array(childs,row,row_headers):
	value = ""
	v = []
	for child in childs:
		if child not in v:
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
	print(row)
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
				if (type(value).__name__) == "int":
					value = str(value) 
				if re.search("^[\s|\t]*$", value) is None:
					value = urllib.parse.quote(value)
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

			if match is not None:
				if (type(value).__name__) == "int":
						value = str(value)
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
						new_string = new_string[:start + offset_current_substitution] + urllib.parse.quote(row[match].strip()) + new_string[ end + offset_current_substitution:]
						offset_current_substitution = offset_current_substitution + len(row[match]) - (end - start)

					else:
						return None
			else:
				if row.find(match) is not None:
					if re.search("^[\s|\t]*$", row.find(match).text) is None:
						new_string = new_string[:start + offset_current_substitution] + urllib.parse.quote(row.find(match).text.strip()) + new_string[ end + offset_current_substitution:]
						offset_current_substitution = offset_current_substitution + len(urllib.parse.quote(row.find(match).text.strip())) - (end - start)

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
			if "." in match:
				if match not in row.keys():
					temp_keys = match.split(".")
					match = temp_keys[len(temp_keys) - 1]
					for tp in temp_keys[:-1]:
						row = row[tp]
			if match in row.keys():
				if row[match] is not None:
					if (type(row[match]).__name__) == "int":
						row[match] = str(row[match])
					if isinstance(row[match],dict):
						print("The key " + match + " has a Json structure as a value.\n")
						print("The index needs to be indicated.\n")
						return None
					else: 
						if re.search("^[\s|\t]*$", row[match]) is None:
							new_string = new_string[:start + offset_current_substitution] + urllib.parse.quote(row[match].strip()) + new_string[ end + offset_current_substitution:]
							offset_current_substitution = offset_current_substitution + len(urllib.parse.quote(row[match])) - (end - start)
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
					# To-do:
					# Generate blank node when subject in csv is not a valid string (empty string, just spaces, just tabs or a combination of the last two)
					#if term == "subject":
					#	new_string = new_string[:start + offset_current_substitution] + str(uuid.uuid4()) + new_string[end + offset_current_substitution:]
					#	offset_current_substitution = offset_current_substitution + len(row[match]) - (end - start)
					#else:
					#	return None
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
						row = row[tp]
			if match in row.keys():
				if (type(row[match]).__name__) == "int":
						row[match] = str(row[match])
				if isinstance(row[match],dict):
					print("The key " + match + " has a Json structure as a value.\n")
					print("The index needs to be indicated.\n")
					return None
				else:
					if row[match] is not None:
						if re.search("^[\s|\t]*$", row[match]) is None:
							new_string = new_string[:start] + row[match].strip().replace("\"", "'") + new_string[end:]
							new_string = "\"" + new_string + "\"" if new_string[0] != "\"" and new_string[-1] != "\"" else new_string
						else:
							return None
					#	return None
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
					if (type(value) is int) or ((type(value).__name__) == "float"):
						value = str(value)
					if re.search("^[\s|\t]*$", value) is None:
						value = urllib.parse.quote(value)
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
					if type(value) is int or ((type(value).__name__) == "float"):
						value = str(value)
					elif type(value).__name__ == "date":
						value = value.strftime("%Y-%m-%d")
					elif type(value).__name__ == "datetime":
						value = value.strftime("%Y-%m-%d T%H:%M:%S")

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
					if (type(value) is int) or ((type(value).__name__) == "float"):
						value = str(value)
					if re.search("^[\s|\t]*$", value) is None:
						value = urllib.parse.quote(value)
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
		for c in child:
			for attr in c:
				if attr in dic:
					dic[attr].append(child.attrib[attr])
				else:	
					dic[attr] = [child.attrib[attr]]
				i += 0
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