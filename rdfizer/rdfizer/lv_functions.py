import io
import csv
import json
from jsonpath_ng import jsonpath, parse
from .functions import *

global lv_join_table
lv_join_table = {}

def flatten_inner_dict(lst, parent_key='', sep='.'):
    """Flatten a list of dictionaries and inner dictionaries."""
    flat_list = []
    
    for entry in lst:
        flat_entry = {}
        
        for key, value in entry.items():
            if isinstance(value, dict):  # If the value is a dict, flatten it
                for sub_key, sub_value in value.items():
                    new_key = f"{key}{sep}{sub_key}"  # Combine keys
                    flat_entry[new_key] = sub_value
            else:
                flat_entry[key] = value
        
        flat_list.append(flat_entry)
    
    return flat_list

def has_nested_dict(lst):
    for d in lst:
        if isinstance(d, dict):
            for value in d.values():
                if isinstance(value, dict):
                    return True
    return False

def remove_dicts(dict_list):
    seen = set()
    unique_dicts = []

    for d in dict_list:
        # Convert dictionary to a JSON string for comparison
        dict_str = json.dumps(d, sort_keys=True)
        if dict_str not in seen:
            seen.add(dict_str)
            unique_dicts.append(d)

    return unique_dicts

def flatten_nested_json(data):
	flattened = []

	for record in data:
		base = {k: v for k, v in record.items() if not isinstance(v, list)}
		for k, v in record.items():
			if isinstance(v, list):
				i = 0
				for i, item in enumerate(v):
					if isinstance(item, dict):
						flat = {f"{k}.{sub_k}": sub_v for sub_k, sub_v in item.items()}
					else:
						flat = {f"{k}": item, f"{k}"+".#" : str(i)}
					i += 1
					full = {**base, **flat}
					flattened.append(full)
			elif not isinstance(v, list) and k not in base:
				base[k] = v
		if not any(isinstance(v, list) for v in record.values()):
			flattened.append(base)

	return flattened


def hash_maker_lv(parent_data, parent_value, parent_id, parent, child):
	ignore = "yes"
	duplicate = "yes"
	global blank_message
	hash_table = {}
	for row in parent_data:
		if parent in row.keys():
			if row[parent] != "" and row[parent] != None:
				if row[parent] in hash_table:
					value = row[parent_value]
					if duplicate == "yes":
						if value not in hash_table[row[parent]]:
							hash_table[row[parent]].update({value: "object"})
					else:
						hash_table[row[parent]].update({value: "object"})

				else:
					value = row[parent_value]
					hash_table.update({row[parent]: {value: "object"}})

	lv_join_table.update({parent_id + "_" + child : hash_table})

def view_iterable(row,attr,iter_attr_maps):
	iterable_rows = []
	new_row = {}
	iter_row = {}
	for iter_attr in attr["attr_list"]:
		if isinstance(iter_attr,dict):
			if iter_attr["type"] == "reference":
				if ".*" in iter_attr["value"]:
					jsonpath_expr = parse(iter_attr["value"].replace(".*",".[*]"))
				else:
					jsonpath_expr = parse(iter_attr["value"])
				iter_values = [match.value for match in jsonpath_expr.find(row)]
				for iter_v in iter_values:
					iter_row[iter_attr["name"]] = iter_v
			elif iter_attr["type"] == "constant":
				iter_row[iter_attr["name"]] = iter_attr["value"]
			elif iter_attr["type"] == "template":
				iter_row[iter_attr["name"]] = string_substitution_json(iter_attr["value"], "{(.+?)}", row, "subject", "yes", "")
			elif iter_attr["type"] == "iterable":
				if ".*" in attr["iterator"]:
					jsonpath_expr = parse(iter_attr["iterator"].replace(".*",".[*]"))
				else:
					jsonpath_expr = parse(iter_attr["iterator"])
				ivalues = [match.value for match in jsonpath_expr.find(row)]
				iterable_rows = []
				for iv in ivalues:
					iter_row[iter_attr["name"]] = view_iterable(iv,iter_attr,iter_attr_maps)
			iterable_rows.append(iter_row)
		else:
			for internal in iter_attr_maps:
				if internal["name"] == iter_attr:
					if internal["type"] == "iterable" and "attr_list" not in internal:
						pass
					else:
						if internal["type"] == "reference":
							if ".*" in internal["value"]:
								jsonpath_expr = parse(internal["value"].replace(".*",".[*]"))
							else:
								jsonpath_expr = parse(internal["value"])
							iter_values = [match.value for match in jsonpath_expr.find(row)]
							for iter_v in iter_values:
								iter_row[internal["name"]] = iter_v
						elif internal["type"] == "constant":
							iter_row[iter_attr["name"]] = internal["value"]
						elif iter_attr["type"] == "template":
							iter_row[internal["name"]] = string_substitution_json(internal["value"], "{(.+?)}", row, "subject", "yes", "")
						elif internal["type"] == "iterable":
							if ".*" in attr["iterator"]:
								jsonpath_expr = parse(internal["iterator"].replace(".*",".[*]"))
							else:
								jsonpath_expr = parse(internal["iterator"])
							ivalues = [match.value for match in jsonpath_expr.find(row)]
							iterable_rows = []
							for iv in ivalues:
								iter_row[internal["name"]]  = view_iterable(iv,iter_attr,iter_attr_maps)
					iterable_rows.append(iter_row)

	new_row[attr["name"]] = iterable_rows
	iterable_rows = remove_dicts(iterable_rows)
	if len(iterable_rows) == 1:
		iterable_rows = iterable_rows[0]
	return iterable_rows

def view_projection(view_map, view_map_list, internal_maps):
	global lv_join_table
	view_source = []
	if "JSONPath" in view_map.ref_form:
		iterable = False
		inner_join_success = True
		with open(view_map.source,"r") as file:
			data = json.load(file)
			if ".*" in view_map.iterator:
				jsonpath_expr = parse(view_map.iterator.replace(".*",".[*]"))
			else:
				jsonpath_expr = parse(view_map.iterator)
			matches = [match.value for match in jsonpath_expr.find(data)]
			for row in matches:
				new_row = {}
				for attr in view_map.attr_list:
					if attr["type"] == "reference":
						if ".*" in attr["value"]:
							jsonpath_expr = parse(attr["value"].replace(".*",".[*]"))
						else:
							jsonpath_expr = parse(attr["value"])
						values = [match.value for match in jsonpath_expr.find(row)]
						if "ref_form" in attr:
							if "CSV" in attr["ref_form"]:
								csv_file = io.StringIO(values[0])
								inner_data = csv.DictReader(csv_file)
								iterable = True
								temp_row = []
								i = 0
								for row in inner_data:
									row["#"] = i
									i += 1
									temp_row.append(row)
								temp_dict = {}
								temp_dict[attr["inner_name"]] = temp_row
								new_row[attr["name"]] = temp_dict
						else:
							if len(values) > 1:
								iterable = True
								temp_row = []
								i = 0
								for v in values:
									temp_row.append(v)
								new_row[attr["name"]] = temp_row
							else:
								for v in values:
									if "[*]" in attr["value"]:
										new_row[attr["name"]] = [v]
									else:
										new_row[attr["name"]] = v
					elif attr["type"] == "constant":
						new_row[attr["name"]] = attr["value"]
					elif attr["type"] == "template":
						new_row[attr["name"]] = string_substitution_json(attr["value"], "{(.+?)}", row, "subject", "yes", "")
					elif attr["type"] == "iterable":
						iterable = True
						if ".*" in attr["iterator"]:
							jsonpath_expr = parse(attr["iterator"].replace(".*",".[*]"))
						else:
							jsonpath_expr = parse(attr["iterator"])
						values = [match.value for match in jsonpath_expr.find(row)]
						iterable_rows = []
						i = 0
						for v in values:
							iter_row = view_iterable(v,attr,internal_maps)
							if isinstance(iter_row, list):
								for ir in iter_row:
									ir["#"] = i
									i += 1
							elif isinstance(iter_row, dict):
								iter_row["#"] = i
								i += 1
							iterable_rows.append(iter_row)

						new_row[attr["name"]] = iterable_rows
					elif attr["type"] == "inner_join" or attr["type"] == "left_join":
						for parent_view in view_map_list:
							if parent_view == attr["parent_view"]:
								if parent_view + "_" + attr["child_condition"] not in lv_join_table:
									parent_source = view_projection(view_map_list[parent_view], view_map_list, internal_maps)
									hash_maker_lv(parent_source, attr["value"], parent_view, attr["parent_condition"], attr["child_condition"])
								if row[attr["child_condition"]] in lv_join_table[parent_view + "_" + attr["child_condition"]]:
									iterable = True
									if len(list(lv_join_table[parent_view + "_" + attr["child_condition"]][row[attr["child_condition"]]].keys())) == 1:
										new_row[attr["name"]] = list(lv_join_table[parent_view + "_" + attr["child_condition"]][row[attr["child_condition"]]].keys())[0]
									else:
										new_row[attr["name"]] = list(lv_join_table[parent_view + "_" + attr["child_condition"]][row[attr["child_condition"]]].keys())
								else:
									if attr["type"] == "inner_join":
										inner_join_success = False
									else:
										new_row[attr["name"]] = None
				if inner_join_success:
					view_source.append(new_row)
			i = 0
			for row in view_source:
				row["#"] = str(i)
				i += 1
			if has_nested_dict(view_source):
				view_source = flatten_inner_dict(view_source)
			if iterable:
				view_source = flatten_nested_json(view_source)
			if has_nested_dict(view_source):
				view_source = flatten_inner_dict(view_source)

	elif "CSV" in view_map.ref_form:
		iterable = False
		inner_join_success = True
		with open(view_map.source,"r") as file:
			reader = csv.DictReader(file)
			number_row = 0
			for row in reader:
				new_row = {}
				for attr in view_map.attr_list:
					if attr["type"] == "reference":
						if "ref_form" in attr:
							if "JSON" in attr["ref_form"]:
								inner_data = json.loads(row[attr["value"]])
								iterable = True
								if attr["iterator"] == "$[*]" or attr["iterator"] == "$.[*]":
									values = inner_data
								else:
									if ".*" in attr["iterator"]:
										jsonpath_expr = parse(attr["iterator"].replace(".*",".[*]"))
									else:
										jsonpath_expr = parse(attr["iterator"])
									values = [match.value for match in jsonpath_expr.find(inner_data)]
								i = 0
								if isinstance(values,list):
									for v in values:
										v["#"] = i
										i += 1
								elif isinstance(values,dict):
									values["#"] = i
									i += 1
								temp_dict = {}
								temp_dict[attr["inner_name"]] = values
								temp_dict["#"] = number_row
								new_row[attr["name"]] = temp_dict
						else:
							new_row[attr["name"]] = row[attr["value"]]
					elif attr["type"] == "constant":
						new_row[attr["name"]] = attr["value"]
					elif attr["type"] == "template":
						new_row[attr["name"]] = string_substitution(attr["value"], "{(.+?)}", row, "subject", "yes","")
					elif attr["type"] == "inner_join" or attr["type"] == "left_join":
						for parent_view in view_map_list:
							if parent_view == attr["parent_view"]:
								if parent_view + "_" + attr["child_condition"] not in lv_join_table:
									parent_source = view_projection(view_map_list[parent_view], view_map_list, internal_maps)
									hash_maker_lv(parent_source, attr["value"], parent_view, attr["parent_condition"], attr["child_condition"])
								if row[attr["child_condition"]] in lv_join_table[parent_view + "_" + attr["child_condition"]]:
									iterable = True
									if len(list(lv_join_table[parent_view + "_" + attr["child_condition"]][row[attr["child_condition"]]].keys())) == 1:
										new_row[attr["name"]] = list(lv_join_table[parent_view + "_" + attr["child_condition"]][row[attr["child_condition"]]].keys())[0]
										new_row[attr["name"]+".#"] = 0
									else:
										new_row[attr["name"]] = list(lv_join_table[parent_view + "_" + attr["child_condition"]][row[attr["child_condition"]]].keys())
								else:
									if attr["type"] == "inner_join":
										inner_join_success = False
									else:
										new_row[attr["name"]] = None
				if inner_join_success:
					view_source.append(new_row)
			i = 0
			for row in view_source:
				row["#"] = str(i)
				i += 1
			if has_nested_dict(view_source):
				view_source = flatten_inner_dict(view_source)
			if iterable:
				view_source = flatten_nested_json(view_source)
				if has_nested_dict(view_source):
					view_source = flatten_inner_dict(view_source)
			print(view_source)
	elif "XPath" in view_map.ref_form:
		pass
	elif "RMLView" in view_map.ref_form:
		data = view_projection(view_map_list[view_map.source],view_map_list)
		for row in data:
			new_row = {}
			for attr in view_map.attr_list:
				new_row[attr["name"]] = row[attr["value"]]
			view_source.append(new_row)
	return view_source