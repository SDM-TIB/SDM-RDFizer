import os
import re
import datetime
import sys
import xml.etree.ElementTree as ET
import urllib
import math

def extract_base(file):
	base = ""
	f = open(file,"r")
	file_lines = f.readlines()
	for line in file_lines:
		if "@base" in line:
			base = line.split(" ")[1][1:-1]
	return base

def encode_char(string):
	encoded = ""
	valid_char = ["~",":","#"]
	for s in string:
		if s in valid_char:
			encoded += s
		else:
			encoded += urllib.parse.quote(s)
	return encoded

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
						new_string = " " + new_string
						while i < count:
							new_string = "{" + new_string
							i += 1
						#new_string = new_string.replace(" ", "")

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

def string_substitution_xml(string, pattern, row, term, iterator):
	template_references = re.finditer(pattern, string)
	new_string = string
	offset_current_substitution = 0
	temp_list = []
	for reference_match in template_references:
		start, end = reference_match.span()[0], reference_match.span()[1]
		if pattern == "{(.+?)}":
			if term == "subject":
				match = reference_match.group(1).split("[")[0]
				if "@" in match:
					match,level = match.split("@")[1],match.split("@")[0]
					if "" == level:
						if row.attrib[match] is not None:
							if re.search("^[\s|\t]*$", row.attrib[match]) is None:
								new_string = new_string[:start + offset_current_substitution] + encode_char(row.attrib[match].strip()) + new_string[ end + offset_current_substitution:]
								offset_current_substitution = offset_current_substitution + len(row.attrib[match]) - (end - start)

							else:
								return None
					else:
						if row.find(level[:-1]).attrib[match] is not None:
							if re.search("^[\s|\t]*$", row.find(level[:-1]).attrib[match]) is None:
								new_string = new_string[:start + offset_current_substitution] + encode_char(row.find(level[:-1]).attrib[match].strip()) + new_string[ end + offset_current_substitution:]
								offset_current_substitution = offset_current_substitution + len(row.find(level[:-1]).attrib[match]) - (end - start)

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
			else:
				if temp_list:
					match = reference_match.group(1).split("[")[0]
					if "@" in match:
						match,level = match.split("@")[1],match.split("@")[0]
						if "" == level:
							pass
							"""if row.attrib[match] is not None:
								if re.search("^[\s|\t]*$", row.attrib[match]) is None:
									new_string = new_string[:start + offset_current_substitution] + encode_char(row.attrib[match].strip()) + new_string[ end + offset_current_substitution:]
									offset_current_substitution = offset_current_substitution + len(row.attrib[match]) - (end - start)
									temp_list.append({"string":new_string,"offset_current_substitution":offset_current_substitution})"""
						else:
							i = 0
							if match in iterator:
								if row.attrib[match] is not None:
									if re.search("^[\s|\t]*$", child.attrib[match]) is None:
										new_string = temp_list[i]["string"][:start + temp_list[i]["offset_current_substitution"]] + encode_char(row.attrib[match].strip()) + temp_list[i]["string"][ end + temp_list[i]["offset_current_substitution"]:]
										offset_current_substitution = temp_list[i]["offset_current_substitution"] + len(row.attrib[match]) - (end - start)
										temp_list[i] = {"string":new_string,"offset_current_substitution":offset_current_substitution}
										i += 1
							else:
								for child in row.findall(level[:-1]):
									if child.attrib[match] is not None:
										if re.search("^[\s|\t]*$", child.attrib[match]) is None:
											new_string = temp_list[i]["string"][:start + temp_list[i]["offset_current_substitution"]] + encode_char(child.attrib[match].strip()) + temp_list[i]["string"][ end + temp_list[i]["offset_current_substitution"]:]
											offset_current_substitution = temp_list[i]["offset_current_substitution"] + len(child.attrib[match]) - (end - start)
											temp_list[i] = {"string":new_string,"offset_current_substitution":offset_current_substitution}
											i += 1

					else:
						i = 0
						if match in iterator:
							if re.search("^[\s|\t]*$", row.text) is None:
								new_string = temp_list[i]["string"][:start + temp_list[i]["offset_current_substitution"]] + encode_char(row.text.strip()) + temp_list[i]["string"][ end + temp_list[i]["offset_current_substitution"]:]
								offset_current_substitution = temp_list[i]["offset_current_substitution"] + len(encode_char(row.text.strip())) - (end - start)
								temp_list[i] = {"string":new_string,"offset_current_substitution":offset_current_substitution}
								i += 1
						else:
							for child in row.findall(match):
								if re.search("^[\s|\t]*$", child.text) is None:
									new_string = temp_list[i]["string"][:start + temp_list[i]["offset_current_substitution"]] + encode_char(child.text.strip()) + temp_list[i]["string"][ end + temp_list[i]["offset_current_substitution"]:]
									offset_current_substitution = temp_list[i]["offset_current_substitution"] + len(encode_char(child.text.strip())) - (end - start)
									temp_list[i] = {"string":new_string,"offset_current_substitution":offset_current_substitution}
									i += 1
				else:
					match = reference_match.group(1).split("[")[0]
					if "@" in match:
						match,level = match.split("@")[1],match.split("@")[0]
						if "" == level:
							pass
							"""if row.attrib[match] is not None:
								if re.search("^[\s|\t]*$", row.attrib[match]) is None:
									new_string = new_string[:start + offset_current_substitution] + encode_char(row.attrib[match].strip()) + new_string[ end + offset_current_substitution:]
									offset_current_substitution = offset_current_substitution + len(row.attrib[match]) - (end - start)
									temp_list.append({"string":new_string,"offset_current_substitution":offset_current_substitution})"""
						else:
							if match in iterator:
								if child.attrib[match] is not None:
									if re.search("^[\s|\t]*$", row.attrib[match]) is None:
										new_string = new_string[:start + offset_current_substitution] + encode_char(row.attrib[match].strip()) + new_string[ end + offset_current_substitution:]
										offset_current_substitution = offset_current_substitution + len(encode_char(row.attrib[match])) - (end - start)
										temp_list.append({"string":new_string,"offset_current_substitution":offset_current_substitution})
							else:
								for child in row.findall(level[:-1]):
									offset_current_substitution = 0
									new_string = string
									if child.attrib[match] is not None:
										if re.search("^[\s|\t]*$", child.attrib[match]) is None:
											new_string = new_string[:start + offset_current_substitution] + encode_char(child.attrib[match].strip()) + new_string[ end + offset_current_substitution:]
											offset_current_substitution = offset_current_substitution + len(encode_char(child.attrib[match])) - (end - start)
											temp_list.append({"string":new_string,"offset_current_substitution":offset_current_substitution})

					else:
						if match in iterator:
							if re.search("^[\s|\t]*$", row.text) is None:
								new_string = new_string[:start + offset_current_substitution] + encode_char(row.text.strip()) + new_string[ end + offset_current_substitution:]
								offset_current_substitution = offset_current_substitution + len(encode_char(row.text.strip())) - (end - start)
								temp_list.append({"string":new_string,"offset_current_substitution":offset_current_substitution})
						else:
							for child in row.findall(match):
								offset_current_substitution = 0
								new_string = string
								if re.search("^[\s|\t]*$", child.text) is None:
									new_string = new_string[:start + offset_current_substitution] + encode_char(child.text.strip()) + new_string[ end + offset_current_substitution:]
									offset_current_substitution = offset_current_substitution + len(encode_char(child.text.strip())) - (end - start)
									temp_list.append({"string":new_string,"offset_current_substitution":offset_current_substitution})
				new_string = new_string.replace("\\","")
		elif pattern == ".+":
			match = reference_match.group(0)
			string_list = []
			if "@" in match:
				match,level = match.split("@")[1],match.split("@")[0]
				if match in iterator:
					if row.attrib:
						if row.attrib[match] is not None:
							if re.search("^[\s|\t]*$", row.attrib[match]) is None:
								new_string = new_string[:start + offset_current_substitution] + "\"" + row.attrib[match].strip() + "\"" + new_string[ end + offset_current_substitution:]
								offset_current_substitution = offset_current_substitution + len(row.attrib[match]) - (end - start)
								string_list.append(new_string)
				else:
					for child in row.findall(level[:-1]):
						offset_current_substitution = 0
						new_string = string
						if child.attrib:
							if child.attrib[match] is not None:
								if re.search("^[\s|\t]*$", child.attrib[match]) is None:
									new_string = new_string[:start + offset_current_substitution] + "\"" + child.attrib[match].strip() + "\"" + new_string[ end + offset_current_substitution:]
									offset_current_substitution = offset_current_substitution + len(child.attrib[match]) - (end - start)
									string_list.append(new_string)
			else:
				if match in iterator:
					if re.search("^[\s|\t]*$", row.text) is None:
						new_string = new_string[:start + offset_current_substitution] + "\"" + row.text.strip() + "\"" + new_string[ end + offset_current_substitution:]
						offset_current_substitution = offset_current_substitution + len(row.text.strip()) - (end - start)
						string_list.append(new_string)
				else:
					for child in row.findall(match):
						offset_current_substitution = 0
						new_string = string
						if re.search("^[\s|\t]*$", child.text) is None:
							new_string = new_string[:start + offset_current_substitution] + "\"" + child.text.strip() + "\"" + new_string[ end + offset_current_substitution:]
							offset_current_substitution = offset_current_substitution + len(child.text.strip()) - (end - start)
							string_list.append(new_string)
			return string_list
		else:
			print("Invalid pattern")
			print("Aborting...")
			sys.exit(1)

	string_list = []
	if temp_list:
		for row in temp_list:
			string_list.append(row["string"])
	if string_list:
		return string_list

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
						if (type(row[match]).__name__) != "float":
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
							if "http" not in value:
								value = encode_char(value)
							new_string = new_string[:start + offset_current_substitution] + value.strip() + new_string[ end + offset_current_substitution:]
							offset_current_substitution = offset_current_substitution + len(value) - (end - start)
							if "\\" in new_string:
								new_string = new_string.replace("\\", "")
								count = new_string.count("}")
								i = 0
								new_string = " " + new_string
								while i < count:
									new_string = "{" + new_string
									i += 1				
								#new_string = new_string.replace(" ", "")

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
				if (type(row[match]).__name__) == "int":
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
						if "http" not in value:
							value = encode_char(value)
						new_string = new_string[:start + offset_current_substitution] + value.strip() + new_string[ end + offset_current_substitution:]
						offset_current_substitution = offset_current_substitution + len(value) - (end - start)
						if "\\" in new_string:
							new_string = new_string.replace("\\", "")
							count = new_string.count("}")
							i = 0
							new_string = " " + new_string
							while i < count:
								new_string = "{" + new_string
								i += 1
							#new_string = new_string.replace(" ", "")

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
						if "http" not in value:
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