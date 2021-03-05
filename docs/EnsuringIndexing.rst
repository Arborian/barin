Indexing
================
    mapper.ensure_all_indexes

Creating an index
-----------------
.. code-block:: python

    def _create_indexes():
    model.metadata.bind(model.mongo.db)
    for c in model.metadata.collections:
        for idx in c.m.indexes:
            opts = idx.options if idx.options else ""
            log.info(f"{c.m.name}.create_index{tuple(idx.arg)}{opts}")
            idx.create(c.m.collection)


Dropping an Index
-----------------
.. code-block:: python

    @mod.command()
    def drop_indexes():
        model.metadata.bind(model.mongo.db)
        for c in model.metadata.collections:
            for iname, ival in c.m.index_information().items():
                if iname == "_id_":
                    continue
                log.info(f"Drop index {c.m.name}: {iname}")
                c.m.drop_index(iname)



    
    
Complete Example
----------------
.. code-block:: python

    import logging

    import click

    from tcpa import model

    from .cli_base import mod

    log = logging.getLogger(__name__)


    @mod.command()
    @click.option("--drop/--no-drop", default=False)
    def initdb(drop):
        model.metadata.bind(model.mongo.db)
        if drop:
            for c in model.metadata.collections:
                log.info(f"Dropping {c}")
                c.m.drop()
        _create_indexes()


    @mod.command()
    def drop_indexes():
        model.metadata.bind(model.mongo.db)
        for c in model.metadata.collections:
            for iname, ival in c.m.index_information().items():
                if iname == "_id_":
                    continue
                log.info(f"Drop index {c.m.name}: {iname}")
                c.m.drop_index(iname)


    @mod.command()
    def create_indexes():
        _create_indexes()


    def _create_indexes():
        model.metadata.bind(model.mongo.db)
        for c in model.metadata.collections:
            for idx in c.m.indexes:
                opts = idx.options if idx.options else ""
                log.info(f"{c.m.name}.create_index{tuple(idx.arg)}{opts}")
                idx.create(c.m.collection)

    