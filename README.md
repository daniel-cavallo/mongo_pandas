# mongo_pandas
A wrapper to pymongo that returns its results in a Pandas DataFrame. This library does not implement all methods supported by pymongo, but those relevant to extract information from the database.

## How to use
After cloning this repo, import it in your project like this:
```from <path_to_library>/mongo_pandas import MongoPandas```

Instatiate the `MongoPandas.Client` class passing to the constructor a mongo URI:
```mongo_cli = MongoPandas.Client('mongodb://...')```

After that, you can start sending queries to mongo and receiving Dataframe with the data retrieved.
```
df = mongo_cli.find({'attribute':'value'})
df.head()
```

## Dataframe column naming

* The attributes in the subdocuments unique_rule_deny_as, unique_rule_deny_bs, multiple_rule_deny_bs and multiple_rule_deny_as from *loans* collection are not querable because they are not really attributes but itens in a list. What we need is to query the value associates with this item and not if it is in the list.

# TODO

* Convert output fields even when no output fileds were specified
* Paginate queries when they are too large
* Create hooks on variables to trigger the execution of a function
* Create post-find hooks to perform operations on results
