from pyarrow import ChunkedArray

class ListColumns(object):
  """
  columnar table base object()
  """
  def __init__(self, schema, cols=None, mask=None):
    self.schema = schema
    self.columns = cols
    self.mask = mask

  def copy(self):
    return ListColumns(self.schema.copy(), list(self.columns))

  def __hash__(self):
    raise Exception("__hash__ not implement in ListColumns")
 
  def __getitem__(self, idx):
    return self.columns[idx]

  def __setitem__(self, idx, val):
    self.columns[idx] = val

  def __len__(self):
    return len(self.columns)

  def num_rows(self):
    num_rows = -1
    for column in self.columns:
      if isinstance(column, ChunkedArray):
        num_rows = column.length()
        break
      if column:
        num_rows = max(num_rows, 1)
    return num_rows

  def is_terminate(self):
    return self.columns is None
  
  def __iter__(self):
    return iter(self.columns)

  def __str__(self):
    return "(%s)" % ", ".join(map(str, self.columns))