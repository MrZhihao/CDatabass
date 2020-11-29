"""
Implementation of logical and physical relational operators
"""
from databass.columns import ListColumns
from ..baseops import UnaryOp
from ..exprs import *
from ..schema import *
from ..tuples import *
from ..columns import *
from ..util import cache, OBTuple
from itertools import chain

########################################################
#
# Source Operators
#
########################################################


class Source(UnaryOp):
  pass

class SubQuerySource(Source):
  """
  Allows subqueries in the FROM clause of a query
  Mainly responsible for giving the subquery an alias
  """
  def __init__(self, c, alias=None):
    super(SubQuerySource, self).__init__(c)
    self.alias = alias 

  def __iter__(self):
    for row in self.c:
      yield row

  def init_schema(self):
    """
    A source operator's schema should be initialized with the same 
    tablename as the operator's alias
    """
    self.schema = self.c.schema.copy()
    self.schema.set_tablename(self.alias)
    return self.schema

class DummyScan(Source):
  def __iter__(self):
    yield ListTuple(Schema([]))

  def init_schema(self):
    self.schema = Schema([])
    return self.schema

  def __str__(self):
    return "DummyScan()"



class Scan(Source):
  """
  A scan operator over a table in the Database singleton.
  """
  def __init__(self, tablename, alias=None):
    super(Scan, self).__init__()
    self.tablename = tablename
    self.alias = alias or tablename
    self.cols_to_scan = None

    # List of tuples (table name, attribute name)
    self.cols_to_scan = []

    from ..db import Database
    self.db = Database.db()


  def init_schema(self):
    """
    A source operator's schema should be initialized with the same 
    tablename as the operator's alias
    """
    self.schema = self.db.schema(self.tablename).copy()
    self.schema.set_tablename(self.alias)
    return self.schema

  def get_col_up_needed(self):
    li = self.p.get_col_up_needed()
    print(li)
    return li

  def get_cols_to_scan(self):
    self.cols_to_scan = list(set(filter(lambda col: col[0] == self.tablename, self.get_col_up_needed())))
    print(self.cols_to_scan)

  def __iter__(self):
    # initialize a single intermediate tuple
    irow = ListTuple(self.schema, [])

    valid_columns = [column[1] for column in self.cols_to_scan]
    self.num_rows = self.db[self.tablename + '_col'].num_rows

    self.columns = ListColumns(self.schema, self.db[self.tablename + '_col'][valid_columns])

    for row in self.__column_to_tuples(self.columns):
      irow.row = row
      yield irow
      
  def __column_to_tuples(self, inter_table):
    for row_idx in range(self.num_rows):
      row = []
      for col_idx in range(len(inter_table.columns)):
        row.append(None if not inter_table.columns[col_idx] else inter_table.columns[col_idx][row_idx].as_py())
      yield row

  def __str__(self):
    return "Scan(%s AS %s)" % (self.tablename, self.alias)

class TableFunctionSource(UnaryOp):
  """
  Scaffold for a table UDF function that outputs a relation.
  Not implemented.
  """
  def __init__(self, function, alias=None):
    super(TableFunctionSource, self).__init__(function)
    self.function = function
    self.alias = alias 

  def __iter__(self):
    raise Exception("TableFunctionSource: Not implemented")

  def __str__(self):
    return "TableFunctionSource(%s)" % self.alias



