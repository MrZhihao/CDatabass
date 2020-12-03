from ..baseops import *
from ..exprs import *
from ..db import Database
from ..schema import *
from ..tuples import *
from ..util import cache, OBTuple
from ..udfs import *
from itertools import chain
from ..columns import ListColumns
from pyarrow import compute, Table
import pandas as pd


########################################################
#
# Aggregation Operators
#
########################################################



class GroupBy(UnaryOp):
  def __init__(self, c, group_exprs, project_exprs=None, aliases=None):
    """
    @c           child operator
    @group_exprs list of Expression objects
    """
    super(GroupBy, self).__init__(c)

    # Grouping expressions defined in the GROUP BY clause
    self.group_exprs = group_exprs

    # Attrs referenced in group_exprs. 
    # They are made accessible to the project_exprs
    # to compute arbitrary expressions over them e.g.,
    #   
    #    SELECT a+b, sum(c)
    #    ..
    #    GROUP BY b / a
    #
    # self.group_attrs would be: a, b
    # 
    # This is nonstandard, since a given group could have
    # >1 distinct group_attrs values.  For instance:
    #
    #    a b
    #    1 2
    #    2 4
    #
    # Both tuples have a GROUP BY value of 2, so in this case, 
    # a+b will be evaluated on the last tuple of the group (2, 4)
    self.group_attrs = []

    # Schema for group_attrs so we can populate a temporary tuple 
    # to pass into the non-agg projection expressions.
    # From GROUP BY b / a above, it would be [a, b] (or [b, a])
    self.group_term_schema = None

    # The actual output expressions of the groupby e.g., a+b, sum(c)
    self.project_exprs = project_exprs or []
    self.aliases = aliases or []


  def init_schema(self):
    """
    * initialize and set self.schema.
    * set self.group_attrs and self.group_term_schema
    """
    self.schema = Schema([])

    for alias, expr in zip(self.aliases, self.project_exprs):
      typ = expr.get_type()
      self.schema.attrs.append(Attr(alias, typ))

    # collect Attrs from group_exprs
    seen = {}
    for attr in chain(*[e.referenced_attrs for e in self.group_exprs]):
      attr = attr.copy()
      seen[(attr.tablename, attr.aname)] = attr

    self.group_attrs = list(seen.values())
    self.group_term_schema = Schema(self.group_attrs)
    return self.schema

  def get_col_up_needed(self, info=None):
    seen = set(self.p.get_col_up_needed())
    
    for attr in chain(*[e.referenced_attrs for e in self.project_exprs]):
      seen.add((attr.real_tablename, attr.aname))

    for attr in self.group_attrs:
        seen.add((attr.real_tablename, attr.aname))

    return list(seen)

  
  def hand_in_result(self):
    """
    GroupBy works as follows:
    
    * Contruct and populate hash table with key defined by the group_exprs expressions  
    * Iterate through each bucket, compose and populate all tuples that conforms to 
      this operator's output schema (see self.init_schema)
    """
    handin_res = self.c.hand_in_result()

    # hash(key): [attr_pos, gr]
    hashtable = defaultdict(lambda: [None, None, []])

    # schema for non-aggregation project exprs
    termrow = ListColumns(self.group_term_schema)

    groupval_cols = []

    for expr in self.group_exprs:
      groupval_cols.append(expr(handin_res))

    for idx in range(groupval_cols[0].length()):
      groupval = tuple([col[idx] for col in groupval_cols])
      key = hash(groupval)
      if not hashtable[key][0]:
        hashtable[key][0] = groupval
        hashtable[key][1] = [attr(handin_res)[idx] for attr in self.group_attrs]
      hashtable[key][2].append(idx)

    group_list_columns = ListColumns(handin_res.schema, [])
    
    res_rows = []

    for _, (key, attrvals, group) in list(hashtable.items()):
      group_list_columns = [col.take(group) if col else None for col in handin_res]
      row = []
      for expr in self.project_exprs:
        if expr.is_type(AggFunc):
          row.append(expr(group_list_columns).as_py())
        else:
          termrow.columns = attrvals
          row.append(expr(termrow).as_py())
      res_rows.append(row)
    
    return ListColumns(self.schema, Table.from_pandas(pd.DataFrame(res_rows)).columns)

    # Another potential approach with iterating each row
    # unique_groupval_cols = [compute.unique(col) for col in groupval_cols]b
    # mask_groupcol_hts = []

    # for idx in range(len(unique_groupval_cols)):
    #   unique_groupval_col = unique_groupval_cols[idx]
    #   mask_groupcol_ht = {}
    #   for unique_val in unique_groupval_col:
    #     mask_groupcol_ht[unique_val] = compute.equal(groupval_cols[idx], unique_val)
    
    # mask_groupval_ht = {}
    

  def __str__(self):
    args = list(map(str, self.group_exprs))
    args.append("|")
    for e, alias in zip(self.project_exprs, self.aliases):
      args.append("%s as %s" % (e, alias))
    s = "GROUPBY(%s)" % ", ".join(args)
    return s

  def to_str(self, ctx):
    with ctx.indent(str(self)):
      self.c.to_str(ctx)



