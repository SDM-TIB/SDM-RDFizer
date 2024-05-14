import re
import sys
import os
from .functions import *
################################################################################################
############################ Static (Do NOT change this code) ##################################
################################################################################################

global global_dic
global_dic = {}
global functions_pool

#####################################################################################################
########### ADD THE IMPLEMENTATION OF YOUR FUNCTIONS HERE FOLLOWING THE EXAMPLES ####################
#####################################################################################################

functions_pool = {"toLowerCase":"","toUpperCase":"","toUpperCaseURL":"",
                "replaceValue":"","concat2":"","uuid":"","helloworld":"",
                "escape":"","schema":"","string_replace":"",
                "parseURL":""}


## Define your functions here following examples below, the column "names" from the csv files 
## that you aim to use as the input parameters of functions are only required to be provided 
## as the keys of "global_dic"
def toLowerCase(): 
    return str(global_dic["valueParam"]).lower()

def toUpperCase(): 
    return str(global_dic["valueParam"]).upper()

def helloworld(): 
    return "Hello World!"

def string_replace(): 
    return global_dic["valueParam"].replace(global_dic["param_find"],global_dic["param_replace"])

def parseURL():
    parsed = {}
    parsed["protocolOutput"] = global_dic["stringParameter"].split("://")[0]
    if "#" in global_dic["stringParameter"]:
        parsed["stringOutput"] = global_dic["stringParameter"].split("://")[1].split("#")[1]
        parsed["domainOutput"] = global_dic["stringParameter"].split("://")[1].split("#")[0]
    else:
        parsed["stringOutput"] = global_dic["stringParameter"].split("://")[1].split("/")[len(global_dic["stringParameter"].split("://")[1].split("/"))-1]
        replace_end = "/" + parsed["stringOutput"]
        parsed["domainOutput"] = global_dic["stringParameter"].split("://")[1].replace(replace_end,"")
    return parsed

def concat2():
    value1 = global_dic["value1"]
    value2 = global_dic["value2"]
    if bool(value1) and bool(value2):
        result = str(str(value1)+str(value2))
    else:
        result = ""  
    return(result)

def uuid():
    from uuid import uuid4
    return str(uuid4())

def escape():
    if global_dic["modeParam"] == 'html':
        import html
        return html.escape(global_dic["valueParam"])
    elif global_dic["modeParam"] == 'url':
        import urllib.parse
        return urllib.parse.quote(global_dic["valueParam"])
    else:
        raise ValueError("Invalid mode. Use 'html' for HTML escaping or 'url' for URL escaping.")

def toUpperCaseURL():
    url_lower = global_dic["str"].lower()

    if url_lower.startswith('https://'):
        return global_dic["str"].upper()
    elif url_lower.startswith('http://'):
        return global_dic["str"].upper()

    # else:
    return f'http://{encode_char(global_dic["str"].upper())}'

def schema(): 
    return "https://schema.org/" + encode_char(global_dic["stringParameter"])
################################################################################################
############################ Static (Do NOT change this code) ##################################
################################################################################################

def execute_function(row,header,dic):
    if "#" in dic["function"]:
        func = dic["function"].split("#")[1]
    else:
        func = dic["function"].split("/")[len(dic["function"].split("/"))-1]
    if func in functions_pool:
        global global_dic
        global_dic = execution_dic(row,header,dic)
        if global_dic == None:
            print("Error when executing function")
            return None
        else:
            return eval(func + "()")             
    else:
        print("Invalid function")
        print("Aborting...")
        sys.exit(1)

def execution_dic(row,header,dic):
    output = {}
    for inputs in dic["inputs"]:
        if isinstance(inputs,list):
            if "constant" not in inputs:
                if "reference" in inputs[1]:
                    if isinstance(row,dict):
                        output[inputs[2]] = row[inputs[0]]
                    else:
                        output[inputs[2]] = row[header.index(inputs[0])]
                elif "template" in inputs:
                    if isinstance(row,dict):
                        output[inputs[2]] = string_substitution(inputs[0], "{(.+?)}", row, "subject", "yes", "None")
                    else:
                        output[inputs[2]] = string_substitution_array(inputs[0], "{(.+?)}", row, header, "subject", "yes")
            else:
                output[inputs[2]] = inputs[0]
        else:
            if "#" in inputs:
                param = inputs.split("#")[1]
            else:
                param = inputs.split("/")[len(inputs.split("/"))-1]
            if "constant" != dic["inputs"][inputs]["type"]:
                if "reference" == dic["inputs"][inputs]["type"]:
                    if isinstance(row,dict):
                        if dic["inputs"][inputs]["value"] in row:
                            output[param] = row[dic["inputs"][inputs]["value"]]
                        else:
                            return None
                    else:
                        if dic["inputs"][inputs]["value"] in header:
                            output[param] = row[header.index(dic["inputs"][inputs]["value"])]
                        else:
                            return None
                elif "template" == dic["inputs"][inputs]["type"]:
                    if isinstance(row,dict):
                        output[param] = string_substitution(dic["inputs"][inputs]["value"], "{(.+?)}", row, "subject", "yes", "None")
                    else:
                        output[param] = string_substitution_array(dic["inputs"][inputs]["value"], "{(.+?)}", row, header, "subject", "yes")
                    if output[param] == None:
                        return None
            else:
               output[param] = dic["inputs"][inputs]["value"] 
    return output