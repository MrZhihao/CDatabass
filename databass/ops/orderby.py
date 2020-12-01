from ..baseops import *
from ..exprs import *
from ..db import Database
from ..schema import *
from ..tuples import *
from ..util import cache, OBTuple
from itertools import chain
from ..columns import ListColumns
import numpy as np
import pandas as pd

class OrderBy(UnaryOp):
  """
  """

  def __init__(self, c, order_exprs, ascdescs):
    """
    @c            child operator
    @order_exprs  list of Expression objects
    @ascdescs     list of "asc" or "desc" strings, same length as @order_exprs 
    """
    super(OrderBy, self).__init__(c)
    self.order_exprs = order_exprs
    self.ascdescs = ascdescs

  def get_col_up_needed(self):
    seen = set(self.p.get_col_up_needed())
    for e in self.order_exprs:
      for attr in e.referenced_attrs:
        seen.add((attr.real_tablename, attr.aname))
    return list(seen)

  def hand_in_result(self):
    """
    OrderBy needs to accumulate all of its child
    operator's outputs before sorting by the order expressions.
    """
    order = [x == "asc" for x in self.ascdescs]
    
    handin_res = self.c.hand_in_result()
    
    sortby_keys = np.array([expr(handin_res).to_numpy() for expr in self.order_exprs]).T
    sortby_keys_df = pd.DataFrame(sortby_keys)

    col_idxes = sortby_keys_df.sort_values(by=list(range(len(order))), ascending=order, kind="mergesort").index.to_list()
    return ListColumns(self.schema, [col.take(col_idxes) if col else None for col in handin_res])

  def __str__(self):
    args = ", ".join(["%s %s" % (e, ad) 
      for (e, ad) in  zip(self.order_exprs, self.ascdescs)])
    return "ORDERBY(%s)" % args


