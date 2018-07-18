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
