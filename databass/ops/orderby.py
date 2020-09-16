from ..baseops import *
from ..exprs import *
from ..db import Database
from ..schema import *
from ..tuples import *
from ..util import cache, OBTuple
from itertools import chain

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

  def __iter__(self):
    """
    OrderBy is a blocking operator that needs to accumulate all of its child
    operator's outputs before sorting by the order expressions.

    Note: each row from the child operator may not be a distinct Tuple object
    """
    order = [1 if x == "asc" else -1 for x in self.ascdescs]

    def keyf(row):
      vals = tuple(expr(row) for expr in self.order_exprs)
      return OBTuple(vals, order)

    rows = [row.copy() for row in self.c]
    rows.sort(key=keyf)
    for row in rows:
      yield row

  def __str__(self):
    args = ", ".join(["%s %s" % (e, ad) 
      for (e, ad) in  zip(self.order_exprs, self.ascdescs)])
    return "ORDERBY(%s)" % args


