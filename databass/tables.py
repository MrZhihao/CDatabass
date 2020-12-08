import pandas
import numbers
import os
from .stats import Stats
from .tuples import *
from .exprs import Attr
import pyarrow as pa

class Table(object):
  """
  A table consists of a schema, and a way to iterate over the rows.
  Specific subclasses can enforce the specific row representations they want 
  e.g., columnar, row-wise, bytearrays, indexes, etc
  """
  id = 0

  def __init__(self, schema):
    self.schema = schema
    self.id = Table.id
    Table.id += 1

    self._stats = None


  @staticmethod
  def from_rows(rows):
    if not rows:
      return InMemoryTable(Schema([]), rows)
    schema = Table.schema_from_rows(list(rows[0].keys()), rows)
    return InMemoryTable(schema, rows)

  @property
  def stats(self):
    if self._stats is None:
      self._stats = Stats(self)
    return self._stats


  def col_values(self, field):
    idx = self.schema.idx(Attr(field.aname))
    return [row[idx] for row in self]

  def __iter__(self):
    yield


class InMemoryTable(Table):
  """
  Row-oriented table that stores its data as an array in memory.
  """
  def __init__(self, schema, rows):
    super(InMemoryTable, self).__init__(schema)
    self.rows = rows
    self.attr_to_idx = { a.aname: i 
        for i,a in enumerate(self.schema)}

  def __iter__(self):
    for row in self.rows:
      yield ListTuple(self.schema, row)


class InMemoryColumnarTable(Table):
  """
  Column-oriented table that stores its data by arrow table in memory.
  """
  def __init__(self, schema, table):
    super(InMemoryColumnarTable, self).__init__(schema)
    self.num_rows = table.num_rows
    self.columns = table.columns
    for idx in range(len(self.columns)):
      if self.columns[idx].type.equals(pa.int64()):
        self.columns[idx] = self.columns[idx].cast(pa.float64())
      if self.columns[idx].type.equals(pa.string()):
        self.columns[idx] = self.columns[idx].dictionary_encode()

    self.attr_to_idx = { a.aname: i 
        for i,a in enumerate(self.schema)}
  
  def __getitem__(self, info):
    if not isinstance(info, list):
      info = [info]
    if isinstance(info[0], int):
      info = set(info)
      return [self.columns[idx] if idx in info else None for idx in range(len(self.columns))]
    elif isinstance(info[0], str):
      info = set(info)
      return [self.columns[idx] if self.schema[idx].aname in info else None for idx in range(len(self.columns))]
    else:
      raise Exception('Invalid fields for table')
