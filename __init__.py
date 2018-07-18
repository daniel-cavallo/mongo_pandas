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
		self._field_mapper = None

	def _load_mapper(self):
		if self._field_mapper == None:
			f_mapper = f'{os.path.dirname(__file__)}/mappings/{self._name}.json'
			self._field_mapper = json.load(open(f_mapper, 'r')) if os.path.isfile(f_mapper) else {}

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
		self._load_mapper()
		r_fields = {}
		for field,value in fields.items():
			f_mapped = self._field_mapper_reversed.get(field, [field])
			r_fields.update(dict((_f,value) for _f in f_mapped))
		return r_fields

	def _build_dataframe(self, data, fields):
		return pd.DataFrame([self._flat_me(doc) for doc in data], columns=fields)

	def find(self, query=None, fields=None, **kwargs):
		r_query = self._remap_query(query) if query else None
		r_fields = self._remap_fields(fields) if fields else None
		o_fields = [field for field,value in fields.items() if value == 1]
		result = self._collection.find(r_query, o_fields)

		if 'limit' in kwargs:
			result = result.limit(kwargs['limit'])

		return self._build_dataframe(result, fields) if result.count() else None

	def list_indexes(self):
		return self._collection.list_indexes()

	def count(self):
		return self._collection.count()

	def get_mappings(self):
		if not self._field_mapper:
			self._load_mapper()
		return json.dumps(self._field_mapper, indent=4)


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

