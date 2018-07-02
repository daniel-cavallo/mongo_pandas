import re
import os
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
			f_mapper = 'mappings/{}.json'.format(self._name)
			self._field_mapper = json.load(open(f_mapper, 'r')) if os.path.isfile(f_mapper) else {}
		
	def _remap_filter(self, query):
		self._load_mapper()
		return query

	def _remap_fields(self, fields):
		self._load_mapper()
		return fields

	def _build_dataframe(self, data, mapper):
		return pd.DataFrame([self._flat_me(doc) for doc in data])

	def find(self, query=None, fields=None):
		r_query = self._remap_filter(query)
		r_fields = self._remap_fields(fields)

		result = self._collection.find(r_query, r_fields)
		if result.count():
			result = self._build_dataframe(result, r_fields)
		return result

	def list_indexes(self):
		return self._collection.list_indexes()

class MongoPandas:
	def __init__(self, mongo_uri):
		self._mongo_uri = mongo_uri
		self._instance = MongoClient(mongo_uri)
		self._dbs = {}

	@classmethod
	def _extract_db_from_uri(cls, uri):
		match = re.search('[^/]/(\w+)$',uri)
		self._db = match.group(1) if match else None

	def __getattr__(self, db):
		if db not in self._dbs:
			self._dbs[db] = Database(self._instance, db)
		return self._dbs[db]
