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
    if handin_res.is_terminate():
      return ListColumns(self.schema, None)

    if isinstance(self.c, Filter):
      cond_mask = self.cond(handin_res.columns)
      new_mask = compute.and_(handin_res.mask, cond_mask)
    else:
      new_mask = self.cond(handin_res.columns)
    
    if len(new_mask.unique()) == 1 and compute.equal(new_mask[0], False).as_py():
      return ListColumns(self.schema, None)
    
    if isinstance(self.p, Filter):
      handin_res.mask = new_mask
      return handin_res
    else:
      return ListColumns(self.schema, [col.filter(new_mask) if col else None for col in handin_res])


