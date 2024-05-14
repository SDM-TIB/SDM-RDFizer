import re
import sys
import os
from .fnml_functions import *

def new_inner_function(row,function,triples_map):
    functions = []
    keys = []
    for func_map in triples_map.func_map_list:
        if func_map.func_map_id == function:
            for param in func_map.parameters:
                if func_map.parameters[param]["type"] == "function":
                    for fm in triples_map.func_map_list:
                        if fm.func_map_id == func_map.parameters[param]["value"]:
                            functions.append(func_map.parameters[param]["value"])
                    func_map.parameters[param]["type"] = "reference"    
                elif func_map.parameters[param]["type"] == "template":
                    if "{" in func_map.parameters[param]["value"]:
                        attr_list = func_map.parameters[param]["value"].split("{")
                        for attr in attr_list:
                            if "}" in attr:
                                keys.append(attr.split("}")[0])
                elif func_map.parameters[param]["type"] == "reference":
                    keys.append(func_map.parameters[param]["value"])
            if functions:
                temp_row = {}
                for func in functions:
                    value = new_inner_function(row,func,triples_map)
                    temp_row[func] = value
                for key in keys:
                    temp_row[key] = row[key]
                current_func = {"inputs":func_map.parameters, 
                                "function":func_map.name}
                return execute_function(temp_row,None,current_func)
            else:
                current_func = {"inputs":func_map.parameters, 
                                "function":func_map.name}
                return execute_function(row,None,current_func)  


def inner_function(row,dic,triples_map_list):

    functions = []
    keys = []
    for attr in dic["inputs"]:
        if ("reference function" in attr[1]):
            functions.append(attr[0])
        elif "template" in attr[1]:
            for value in attr[0].split("{"):
                if "}" in value:
                   keys.append(value.split("}")[0]) 
        elif "constant" not in attr[1]:
            keys.append(attr[0])
    if functions:
        temp_dics = {}
        for function in functions:
            for tp in triples_map_list:
                if tp.triples_map_id == function:
                    temp_dic = create_dictionary(tp)
                    current_func = {"inputs":temp_dic["inputs"], 
                                    "function":temp_dic["executes"],
                                    "func_par":temp_dic,
                                    "termType":True}
                    temp_dics[function] = current_func
        temp_row = {}
        for dics in temp_dics:
            value = inner_function(row,temp_dics[dics],triples_map_list)
            temp_row[dics] = value
        for key in keys:
            temp_row[key] = row[key]
        return execute_function(temp_row,None,dic)
    else:
        return execute_function(row,None,dic)

def inner_values(row,dic,triples_map_list):
    values = ""
    for inputs in dic["inputs"]:
        if "reference" == inputs[1]:
            values += str(row[inputs[0]])
        elif "template" == inputs[1]:
            for string in inputs[0].split("{"):
                if "}" in string:
                    values += str(row[string.split("}")[0]])
        elif "reference function" == inputs[1]:
            temp_dics = {}
            for tp in triples_map_list:
                if tp.triples_map_id == inputs[0]:
                    temp_dic = create_dictionary(tp)
                    current_func = {"inputs":temp_dic["inputs"], 
                                    "function":temp_dic["executes"],
                                    "func_par":temp_dic,
                                    "termType":True}
                    values += inner_values(row,temp_dic,triples_map_list)
    return values

def inner_function_exists(inner_func, inner_functions):
    for inner_function in inner_functions:
        if inner_func["id"] in inner_function["id"]:
            return False
    return True

def create_dictionary(triple_map):
    dic = {}
    inputs = []
    for tp in triple_map.predicate_object_maps_list:
        if "#" in tp.predicate_map.value:
            key = tp.predicate_map.value.split("#")[1]
            tp_type = tp.predicate_map.mapping_type
        elif "/" in tp.predicate_map.value:
            key = tp.predicate_map.value.split("/")[len(tp.predicate_map.value.split("/"))-1]
            tp_type = tp.predicate_map.mapping_type
        if "constant" in tp.object_map.mapping_type:
            value = tp.object_map.value
            tp_type = tp.object_map.mapping_type
        if "template" in tp.object_map.mapping_type:
            value = tp.object_map.value
            tp_type = tp.object_map.mapping_type
        elif "executes" in tp.predicate_map.value:
            if "#" in tp.object_map.value:
                value = tp.object_map.value.split("#")[1]
                tp_type = tp.object_map.mapping_type
            elif "/" in tp.object_map.value:
                value = tp.object_map.value.split("/")[len(tp.object_map.value.split("/"))-1]
                tp_type = tp.object_map.mapping_type
        else:
            value = tp.object_map.value
            tp_type = tp.object_map.mapping_type

        dic.update({key : value})
        if (key != "executes") and ([value,tp_type,key] not in inputs):
            inputs.append([value,tp_type,key])

    dic["inputs"] = inputs
    return dic