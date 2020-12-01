from ..baseops import *
from ..exprs import *
from ..db import Database
from ..schema import *
from ..tuples import *
from ..util import cache, OBTuple
from itertools import chain
from ..columns import ListColumns
from pyarrow import compute

class From(NaryOp):
  """
  Logical FROM operator. 
  Optimizer will expand it into a join tree
  """

  def __init__(self, cs, predicates):
    super(From, self).__init__(cs)
    self.predicates = predicates

  def to_str(self, ctx):
    name = "From(%s)" % " and ".join(map(str, self.predicates))
    with ctx.indent(name):
      for c in self.cs:
        c.to_str(ctx)

class Join(BinaryOp):
  pass

class ThetaJoin(Join):
  """
  Theta Join is tuple-nested loops join
  """
  def __init__(self, l, r, cond=Literal(True)):
    """
    @l    left (outer) subplan of the join
    @r    right (inner) subplan of the join
    @cond an Expr object whose output will be interpreted
          as a boolean
    """
    super(ThetaJoin, self).__init__(l, r)
    self.cond = cond
  
  def get_col_up_needed(self, info=None):
    seen = set(self.p.get_col_up_needed())
    for attr in self.cond.referenced_attrs:
      seen.add((attr.real_tablename, attr.aname))
    return list(seen)

  def __iter__(self):
    # initialize a single intermediate tuple
    irow = ListTuple(self.schema, [])

    for lrow in self.l:
      for rrow in self.r:
        # populate intermediate tuple with values
        irow.row[:len(lrow.row)] = lrow.row
        irow.row[len(lrow.row):] = rrow.row

        if self.cond(irow):
          yield irow

  def hand_in_result(self):
    l_tb, r_tb = self.l.hand_in_result(), self.r.hand_in_result()

    left_col_pos = list(chain(*[[l_pos]*r_tb.num_rows() for l_pos in range(l_tb.num_rows())]))
    right_col_pos = list(chain(*[list(range(r_tb.num_rows())) for _ in range(l_tb.num_rows())]))

    column_res = []
    for l_col in l_tb:
      column_res.append(l_col.take(left_col_pos) if l_col else None)
  
    for r_col in r_tb:
      column_res.append(r_col.take(right_col_pos) if r_col else None)
    
    mask = self.cond(ListColumns(self.schema, column_res))

    # if type(mask) == bool:
    #   return ListColumns(self.schema, column_res if mask else [None] * len(self.schema))

    return ListColumns(self.schema, [col.filter(mask) if col else None for col in column_res])

  def __str__(self):
    return "THETAJOIN(ON %s)" % (str(self.cond))


