import re

class TriplesMap:

	def __init__(self, triples_map_id, data_source, subject_map, predicate_object_maps_list, ref_form=None, iterator=None, tablename=None, query=None, function=False, func_map_list=None, mappings_type=None):

		"""
		Constructor of a TriplesMap object

		Parameters
		----------
		triples_map_id : string
			URI containing the triples-map indentification
		data_source : string
			URI containing the path to the data source
		subject_map : SubjectMap object
			SubjectMap object containing the specifications of the subject
		predicate_object_maps_list : list of PredicateObjectMap objects
			List containing the PredicateObjectMap objects associated with the SubjectMap object
		ref_from : string
			URI containing the data source reference formulation

		"""
		self.triples_map_id = triples_map_id
		self.triples_map_name = re.compile("((.*?))$").search(str(self.triples_map_id)).group(0)
		self.data_source = data_source[7:] if data_source[:7] == "file://" else data_source
		self.reference_formulation = ref_form
		if self.reference_formulation != "None" and re.compile("(#[A-Za-z]+)$").search(str(self.reference_formulation)) != None:
			self.file_format = re.compile("(#[A-Za-z]+)$").search(str(self.reference_formulation)).group(0)[1:]
		else:
			if "http://w3id.org/rml/" in self.reference_formulation:
				self.file_format = self.reference_formulation.split("/")[len(self.reference_formulation.split("/"))-1]
			elif "http://www.w3.org/ns/formats/" in self.reference_formulation:
				self.file_format = self.reference_formulation.split("/")[len(self.reference_formulation.split("/"))-1]
			else:
				self.file_format = None
		self.iterator = iterator
		self.tablename = tablename
		self.query = query

		if subject_map is not None:
			self.subject_map = subject_map
		else:
			print("Subject map cannot be empty")
			print("Aborting...")
			exit(1)

		self.predicate_object_maps_list = predicate_object_maps_list
		self.function = function
		self.func_map_list = func_map_list
		self.mappings_type = mappings_type if mappings_type != None else "None"


	def __repr__(self):

		"""
		Proper string representation for the TriplesMap objects

		Returns
		-------
		Returns a string containing a human-readable representation for the TriplesMap objects
		"""

		value = "triples map id: {}\n".format(self.triples_map_name)
		value += "\tlogical source: {}\n".format(self.data_source)
		value += "\treference formulation: {}\n".format(self.reference_formulation)
		value += "\titerator: {}\n".format(self.iterator)
		value += "\tsubject map: {}\n".format(self.subject_map.value)

		for predicate_object_map in self.predicate_object_maps_list:
			value += "\t\tpredicate: {} - mapping type: {}\n".format(predicate_object_map.predicate_map.value, predicate_object_map.predicate_map.mapping_type)
			value += "\t\tobject: {} - mapping type: {} - datatype: {}\n\n".format(predicate_object_map.object_map.value, predicate_object_map.object_map.mapping_type, str(predicate_object_map.object_map.datatype))
			if predicate_object_map.object_map.mapping_type == "parent triples map":
				value += "\t\t\tjoin condition: - child: {} - parent: {} \n\n\n".format(predicate_object_map.object_map.child,predicate_object_map.object_map.parent)

		return value + "\n"

class SubjectMap:
	
	def __init__(self, subject_value, condition, subject_mapping_type, parent, child, rdf_class=None, term_type=None, graph=None,func_result=None):

		"""
		Constructor of a SubjectMap object

		Parameters
		----------
		subject_value : string
			URI containing the subject
		rdf_class : string (optional)
			URI containing the class of the subject

		"""

		self.value = subject_value
		self.condition = condition 
		self.rdf_class = rdf_class
		self.term_type = term_type
		self.subject_mapping_type = subject_mapping_type
		self.graph = graph
		self.func_result = func_result if func_result != "None" else None
		self.child = str(child) if "None" != child else None
		self.parent = str(parent) if "None" != parent else None

class PredicateObjectMap:
	
	def __init__(self, predicate_map, object_map, graph):

		"""
		Constructor of a PredicateObjectMap object

		Parameters
		----------
		predicate_map : PredicateMap object
			Object representing a predicate-map
		object_map : ObjectMap object
			Object representing a object-map

		"""

		self.predicate_map = predicate_map
		self.object_map = object_map
		self.graph = graph

class PredicateMap:

	def __init__(self, predicate_mapping_type, predicate_value, predicate_condition, func_result):

		"""
		Constructor of a PredicateMap object

		Parameters
		----------
		predicate_mapping_type : string
			String containing the type of predicate-map ("constant", "constant shortcut",
			"template" or "reference")
		predicate_value : string
			URI containi

		"""

		self.value = predicate_value
		self.mapping_type = predicate_mapping_type
		self.condition = predicate_condition
		self.func_result = func_result if func_result != "None" else None

class ObjectMap:

	def __init__(self, object_mapping_type, object_value, object_datatype, object_child, object_parent, term, language, language_map, datatype_map, func_result):

		"""
		Constructor of ObjectMap object

		Parameters
		----------
		predicate_map : PredicateMap object
			Object representing a predicate-map
		object_map : ObjectMap object
			Object representing a object-map

		"""

		self.value = object_value
		self.datatype = object_datatype if object_datatype != "None" else None 
		self.mapping_type = object_mapping_type
		self.child = object_child if "None" not in object_child  else None
		self.parent = object_parent if "None" not in object_parent else None
		self.term = term if term != "None" else None
		self.language = language if language != "None" else None
		self.language_map = language_map if language_map != "None" else None
		self.datatype_map = datatype_map if datatype_map != "None" else None
		self.func_result = func_result if func_result != "None" else None


class FunctionMap:

	def __init__(self, func_map_id, func_name, parameters):

		self.func_map_id = func_map_id
		self.name = func_name
		self.parameters = parameters