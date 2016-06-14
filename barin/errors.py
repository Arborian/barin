class BarinError(Exception): pass
class SchemaError(BarinError): pass
class QueryError(BarinError): pass
class ConflictError(QueryError): pass
