from databass.columns import ListColumns
from ..baseops import *
from ..exprs import *
from ..db import Database
from ..schema import *
from ..tuples import *
from ..util import cache, OBTuple
from .join import *
from itertools import chain

   
class HashJoin(Join):
  """
  Hash Join
  """
  def __init__(self, l, r, join_attrs):
    """
    @l    left table of the join
    @r    right table of the join
    @join_attrs two attributes to join on, hash join checks if the 
                attribute values from the left and right tables are
                the same.  Suppose:
                
                  l = iowa, r = iowa, join_attrs = ["STORE", "storee"]

                then we return all pairs of (l, r) where 
                l.STORE = r.storee
    """
    super(HashJoin, self).__init__(l, r)
    self.join_attrs = join_attrs

  def get_col_up_needed(self, info=None):
    needed_attrs = set(self.p.get_col_up_needed())
    for attr in self.join_attrs:
      needed_attrs.add((attr.real_tablename, attr.aname))
    return list(needed_attrs)


  def build_hash_index(self, child_cols, idx):
    """
    @child_cols columnars to construct an index over
    @attr attribute name to build index on

    Loops through a columnar iterator and creates an index based on
    the attr value
    """
    right_ht = defaultdict(list)
    for pos, val in enumerate(child_cols[idx]):
      right_ht[val].append(pos)
    return right_ht

  def hand_in_result(self):
    lidx = self.join_attrs[0].idx
    ridx = self.join_attrs[1].idx

    l_tb, r_tb = self.l.hand_in_result(), self.r.hand_in_result()

    index = self.build_hash_index(r_tb, ridx)

    left_col_pos, right_col_pos = [], []

    for l_pos, l_key in enumerate(l_tb[lidx]):
      # probe the hash index
      matched_r = index[l_key]

      left_col_pos.extend([l_pos] * len(matched_r))
      right_col_pos.extend(matched_r)

    column_res = []
    for l_col in l_tb:
      column_res.append(l_col.take(left_col_pos) if l_col else None)
  
    for r_col in r_tb:
      column_res.append(r_col.take(right_col_pos) if r_col else None)
    
    return ListColumns(self.schema, column_res)
    


