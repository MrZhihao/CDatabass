from ..baseops import *
from ..exprs import *
from ..db import Database
from ..schema import *
from ..tuples import *
from ..util import cache, OBTuple
from itertools import chain
from ..columns import ListColumns

class Filter(UnaryOp):
  def __init__(self, c:Op, cond:Expr):
    """
    @c            child operator
    @cond         boolean Expression 
    """
    super(Filter, self).__init__(c)
    self.cond = cond
    
  def get_col_up_needed(self, info=None):
    seen = set(self.p.get_col_up_needed())
    for attr in self.cond.referenced_attrs:
      seen.add((attr.real_tablename, attr.aname))
    return list(seen)

  def __iter__(self):
    for row in self.c:
      if self.cond(row):
        yield row
  
  def hand_in_result(self):
    handin_res = self.c.hand_in_result()
    mask = self.cond(handin_res.columns)
    return ListColumns(self.schema, [col.filter(mask) if col else None for col in handin_res])


