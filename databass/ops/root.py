from ..baseops import *
from ..tuples import *

class Sink(UnaryOp):
  def init_schema(self):
    self.schema = self.c.schema
    return self.schema


class Yield(Sink):
  def __iter__(self):
    return iter(self.c)

class Collect(Sink):
  def __iter__(self):
    return [row for row in self.c]

class Print(Sink):
  def __iter__(self):
    for row in self.c:
      print(row)
    yield 

class Yield_Col(Sink):
  def __iter__(self):
    handin_res = self.c.hand_in_result()
    
    if not handin_res.is_terminate():
      irow = ListTuple(self.schema, [])

      for row_idx in range(handin_res.num_rows()):
        row = []
        for col_idx in range(len(handin_res)):
          row.append(handin_res[col_idx][row_idx].as_py())
        irow.row = row
        yield irow


