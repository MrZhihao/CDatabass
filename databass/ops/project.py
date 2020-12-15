from databass.columns import ListColumns
from ..baseops import *
from ..exprs import *
from ..db import Database
from ..schema import *
from ..tuples import *
from ..util import cache, OBTuple, columnar_to_tuples
from itertools import chain
from .root import *

########################################################
#
# Projection Operator
#
########################################################

class Project(UnaryOp):
  def __init__(self, c, exprs, aliases=[]):
    """
    @p            parent operator
    @exprs        list of Expr objects
    @aliases      name of the fields defined by the above exprs
    """
    super(Project, self).__init__(c)
    self.exprs = exprs
    self.aliases = list(aliases) or []

  def init_schema(self):
    """
    Figure out the alias and type of each expression in the target list
    and use it to set my schema
    """
    self.schema = Schema([])
    for alias, expr in zip(self.aliases, self.exprs):
      typ = expr.get_type()
      self.schema.attrs.append(Attr(alias, typ))

    return self.schema

  def get_col_up_needed(self):
    # TODO optimize subquery
    # if self.p and not isinstance(self.p, Sink):
    #   # self.p.get_col_up_needed()
    #   # process [a,b,c,d] a.referenced_attrs
    #   # self.exprs self.schema, clear unused alias
    #   # edit schema
    seen = set()
    for attr in chain(*[e.referenced_attrs for e in self.exprs]):
      seen.add((attr.real_tablename, attr.aname))
    return list(seen)

  def hand_in_result(self):
    if self.c == None:
      return ListColumns(self.schema, [None] * len(self.schema))
    
    handin_res = self.c.hand_in_result()
    if handin_res.is_terminate():
      return ListColumns(self.schema, None)
    
    res_columns = []
    for exp in self.exprs:
      res_columns.append(exp(handin_res))
    return ListColumns(self.schema, res_columns)

  def __str__(self):
    args = ", ".join(["%s AS %s" % (e, a) 
      for (e, a) in  zip(self.exprs, self.aliases)])
    return "Project(%s)" % args





