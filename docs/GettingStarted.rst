===============
GETTING STARTED
===============
Welcome

Barin is an object document mapper. It insures data coming in 
and going out of your mongoDB database is in a format you desire.


Installation
------------
::

  $ pip install barin

Connecting to the database
--------------------------
.. code-block:: python
   :emphasize-lines: 3,5-7
   
   import pymongo
   import bson
   import barin as b
    
   metadata = b.Metadata()
   myclient = pymongo.MongoClient("mongodb://localhost:27017/")
   metadata.bind(myclient["mydatabase"])

Here we are importing pymongo, bson and barin as our standard imports.
We instantiate the barin Metadata class and store it in metadata variable.
We then establish a variable to connect to the MongoDB database. On the last 
line we bind metadata to our MongoDB collection.

Creating a Model
----------------
.. code-block:: python

    import pymongo
    import bson
    import barin as b
    from barin import schema as s

    metadata = b.Metadata()
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    metadata.bind(myclient["mydatabase"])


    # create collection object
    customers=b.collection(    
        :metadata,'customers',
        b.Field('_id',s.ObjectId, default=bson.ObjectId),
        b.Field('name',str),
        b.Field('address', str)
        )

Community
---------

Contributing
------------

Documentation Content
---------------------
User Guide

Index
-----

