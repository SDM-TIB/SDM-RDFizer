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
                "parseURL":"","random":"","length":"","string_substring":"",
                "array_join":"","controls_if":"","string_md5":"","string_contains":"",
                "slugify":"","trueCondition":"","isNull":"",
                "notEqual":"","equal":"","normalizeDateTime":"","normalizeDate":"",
                "listContainsElement":""}


## Define your functions here following examples below, the column "names" from the csv files 
## that you aim to use as the input parameters of functions are only required to be provided 
## as the keys of "global_dic"
def listContainsElement():
    if str(global_dic["str"]) in global_dic["list"]:
        return True
    else:
        return False

def normalizeDate():
    from datetime import datetime
    #from dateutil import parser
    return str(datetime.strptime(str(global_dic["strDate"]), str(global_dic["pattern"])))

def normalizeDateTime():
    from datetime import datetime
    #from dateutil import parser
    return str(datetime.strptime(str(global_dic["strDate"]), str(global_dic["pattern"])))

def equal():
    if str(global_dic["valueParameter"]) == str(global_dic["valueParameter2"]):
        return True
    else:
        return False

def notEqual():
    if str(global_dic["valueParameter"]) != str(global_dic["valueParameter2"]):
        return True
    else:
        return False

def isNull():
    if str(global_dic["str"]) == "null" or str(global_dic["str"]) == "":
        return True
    else:
        return False

def trueCondition():
    if bool(global_dic["strBoolean"]):
        if "None" != str(global_dic["str"]):
            return str(global_dic["str"])
        else:
            return None
    else:
        return None

def slugify():
    from slugify import slugify
    return slugify(str(global_dic["str"]))

def string_replace():
    return str(global_dic["valueParameter"]).replace(str(global_dic["p_string_find"]),str(global_dic["p_string_replace"]))

def string_contains():
    if str(global_dic["string_sub"]) in str(global_dic["valueParameter"]):
        return True
    else:
        return False

def string_md5():
    import hashlib
    return hashlib.md5(str(global_dic["valueParameter"]).encode()).hexdigest()

def controls_if():
    if bool(global_dic["bool_b"]):
        if "any_true" in global_dic:
            if "None" == str(global_dic["any_true"]):
                return None
            else:
                return str(global_dic["any_true"])
        else:
            return None
    else:
        if "any_false" in global_dic:
            if "None" == str(global_dic["any_false"]):
                return None
            else:
                return str(global_dic["any_false"])
        else:
            return None

def array_join():
    output = ""
    for elem in global_dic["p_array_a"]:
        output += str(elem)
        if elem != global_dic["p_array_a"][len(global_dic["p_array_a"])-1]:
            output += str(global_dic["p_string_sep"])    
    return output

def string_substring(): 
    if int(global_dic["param_int_i_from"]) > len(str(global_dic["valueParameter"])) or int(global_dic["param_int_i_opt_to"]) > len(str(global_dic["valueParameter"])):
        return None
    else:
        return str(global_dic["valueParameter"])[int(global_dic["param_int_i_from"]):int(global_dic["param_int_i_opt_to"])]

def length(): 
    return str(len(str(global_dic["valueParam"])))

def toLowerCase(): 
    return str(global_dic["valueParam"]).lower()

def toUpperCase(): 
    return str(global_dic["valueParam"]).upper()

def helloworld(): 
    return "Hello World!"

#def string_replace(): 
#    return global_dic["valueParam"].replace(global_dic["param_find"],global_dic["param_replace"])

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

def random():
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
            if isinstance(dic["inputs"][inputs],list):
                output[param] = []
                for elem in dic["inputs"][inputs]:
                    if "constant" != elem["type"]:
                        if "reference" == elem["type"]:
                            if isinstance(row,dict):
                                if elem["value"] in row:
                                    output[param].append(row[elem["value"]])
                                else:
                                    return None
                            else:
                                if elem["value"] in header:
                                    output[param].append(row[header.index(elem["value"])])
                                else:
                                    return None
                        elif "template" == elem["type"]:
                            if isinstance(row,dict):
                                output[param].append(string_substitution(elem["value"], "{(.+?)}", row, "subject", "yes", "None"))
                            else:
                                output[param].append(string_substitution_array(elem["value"], "{(.+?)}", row, header, "subject", "yes"))
                            if output[param] == None:
                                return None
                    else:
                       output[param].append(elem["value"])
            else:
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
                        if output[param] == None and "controls_if" not in dic["function"]:
                            return None
                else:
                   output[param] = dic["inputs"][inputs]["value"] 
    return output