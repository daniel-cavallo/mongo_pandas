import re
import os
import json
from pandas import pandas as pd 
from pymongo import MongoClient


class Database:
	def __init__(self, mongo_instance, db):
		self._db = mongo_instance[db]
		self._collections = {}

	def __getattr__(self, collection):
		if collection not in self._collections:
			self._collections[collection] = Collection(self._db, collection)
		return self._collections[collection]
		

class Collection:
	@classmethod
	def _flat_me(cls, value, sep='.'):
	    flatten = None
	    def flat_dict(value, attribute='', sep='.'):
	        if not isinstance(value,dict):
	            flatten.update({attribute[1:]:value})
	            return
	        for a,v in value.items():
	            flat_dict(v,attribute+sep+a,sep)

	    if isinstance(value,dict):
	        flatten = {}
	        flat_dict(value, sep=sep)
	        return flatten
	    return value

	def __init__(self, db, collection):
		self._name = collection
		self._collection = db[collection]
		self._internal_field_mapper = None
		self._user_field_mapper = None
		self._disable_mapping = False

	@property
	def _field_mapper(self):
		res = self._internal_field_mapper if self._internal_field_mapper != None else {}
		if self._user_field_mapper != None:
			res.update(self._user_field_mapper)
		return res
	
	def _load_mapper(self):
		if not self._disable_mapping and self._internal_field_mapper == None:
			f_mapper = f'{os.path.dirname(__file__)}/mappings/{self._name}.json'
			self._internal_field_mapper = json.load(open(f_mapper, 'r')) if os.path.isfile(f_mapper) else {}
			self._build_mapper()

	def _build_mapper(self):
		self._field_mapper_reversed = {}
		for key,value in self._field_mapper.items():
			if not value in self._field_mapper_reversed:
				self._field_mapper_reversed[value] = []
			self._field_mapper_reversed[value].append(key)

	def _remap_query(self, query):
		self._load_mapper()
		r_query = {}
		for field,value in query.items():
			f_mapped = self._field_mapper_reversed.get(field, [field])
			if len(f_mapped) > 1:
				r_query['$or'] = list({_f:value} for _f in f_mapped)
			else:
				r_query[f_mapped[0]] = value
		return r_query

	def _remap_fields(self, fields):
		"""
		Maps pseudo fields to real fields (attributes). This process can 
		lead to produce more than one real field for one pseudo field, 
		which means the same field can be defined in different places in
		the document or the collection can have different layout documents
		and the field is defined in different places in each layout.

		Arguments:
			- fields: a mongo's projection-like document that can have pseudo
						and real fields mixed up.

		Returns:
			- a tuple with two dictionaries: a mongo's projection-like document
				with only real fields to be used with find(); and a list with
				column names for pandas dataframe.
		"""
		self._load_mapper()
		real_fields = {}
		real2pseudo_fields = {}
		for field, will_output in fields.items():
			_r_mapped = self._field_mapper_reversed.get(field, [field])
			real_fields.update(dict((_f, will_output) for _f in _r_mapped))
			if will_output:
				if len(_r_mapped) > 1:
					real2pseudo_fields.update(dict((_f,f'{field}__{_f}') for _f in _r_mapped))
				else:
					real2pseudo_fields.update({_r_mapped[0]: field})

		return (real_fields, real2pseudo_fields)

	def _build_dataframe(self, data, fields):
		df = pd.DataFrame([self._flat_me(doc) for doc in data])
		return df.rename(index=str, columns=fields)

	def find(self, query=None, fields=None, **kwargs):
		r_query = self._remap_query(query) if query else None
		r_fields,o_fields = self._remap_fields(fields) if fields else (None,None)
		result = self._collection.find(r_query, r_fields)

		if 'limit' in kwargs:
			result = result.limit(kwargs['limit'])

		return self._build_dataframe(result, o_fields) if result.count() else None

	def list_indexes(self):
		return self._collection.list_indexes()

	def count(self):
		return self._collection.count()

	def get_mappings(self):
		if not self._field_mapper:
			self._load_mapper()
		return json.dumps(self._field_mapper, indent=4)

	def add_mapping(self, mapping):
		if self._disable_mapping:
			raise RuntimeError('Mappings are disabled')

		self._user_field_mapper.update(mapping)
		self._build_mapper()

	@property
	def disable_mapping(self):
		return self._disable_mapping

	@disable_mapping.setter
	def disable_mapping(self, disable):
		self._disable_mapping = disable
		if disable:
			self._internal_field_mapper = None
			self._user_field_mapper = None


class MongoPandas:
	@classmethod
	def _extract_db_from_uri(cls, uri):
		match = re.search('[^/]/(\w+)$',uri)
		self._db = match.group(1) if match else None

	def __init__(self, mongo_uri):
		self._mongo_uri = mongo_uri
		self._instance = MongoClient(mongo_uri)
		self._dbs = {}

	def __getattr__(self, db):
		if db not in self._dbs:
			self._dbs[db] = Database(self._instance, db)
		return self._dbs[db]

