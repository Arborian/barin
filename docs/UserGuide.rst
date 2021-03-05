====================
Barin ODM User Guide
====================
Welcome
=======
Barin is an object document mapper. It insures data coming in 
and going out of your mongoDB database is in a format you desire.

Installation
===============
::

  $ pip install barin

Connecting to the database
==========================
.. code-block:: python

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

Authentication
==============
Authentication Stuff Here


Mapped Classes and Documents
============================
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
        metadata,'customers',
        b.Field('_id',s.ObjectId, default=bson.ObjectId),
        b.Field('name',str),
        b.Field('address', str)
    )

    @b.cmap(customers)
    class Customer:
        def __repr__(self):
            return f'customer({self.name})'


Creating Objects
================
Creating Single Object
----------------------
.. code-block:: python

    newCustomer= customers.m.create(name='foo', address="123 lane")
    newCustomer.m.insert()

Creating Many Objects
---------------------
.. code-block:: python

    mylist = [
        {"name": "Hannah", "address": "Mountain 21"},
        {"name": "Michael", "address": "Valley 345"},
        {"name": "Sandy", "address": "Ocean blvd 2"},
        {"name": "Betty", "address": "Green Grass 1"},
        {"name": "Richard", "address": "Sky st 331"},
        {"name": "Susan", "address": "One way 98"},
        {"name": "Vicky", "address": "Yellow Garden 2"},
        {"name": "Ben", "address": "Park Lane 38"},
        {"name": "William", "address": "Central st 954"},
        {"name": "Chuck", "address": "Main Road 989"},
        {"name": "Viola", "address": "Sideway 1633"}
        ]
    customers.m.insert_many(mylist)


Querying Objects
================
find method
-----------
.. code-block:: python

    for x in customers.m.find():
        print(x)

Editing Objects
===============
Update in Place
---------------
.. code-block:: python

    customers.m.update_one(customers.name == "amy",customers.name.set('AMY'))

replace()
---------
.. code-block:: python

    x.name='bar'
    x.m.replace()

Deleting Objects
================
delete_many()
-------------
.. code-block:: python

    customers.m.delete_many({})  # deletes all customers out of database

Working with SubDocuments
=========================
Schema object and SchemaArray


