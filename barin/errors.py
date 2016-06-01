class BarinError(Exception): pass
class QueryError(BarinError): pass
class ConflictError(QueryError): pass
