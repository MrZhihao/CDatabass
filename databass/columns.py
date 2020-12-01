class ListColumns(object):
  """
  columnar table base object
  """
  def __init__(self, schema, cols=None):
    self.schema = schema
    self.columns = cols or []
    if len(self.columns) < len(self.schema.attrs):
      self.columns += [None] * (len(self.schema.attrs) - len(self.columns))

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

  def __str__(self):
    return "(%s)" % ", ".join(map(str, self.columns))