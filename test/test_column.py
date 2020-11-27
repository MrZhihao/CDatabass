import random
import pandas as pd
import numpy as np
from .conftest import *
from databass import *
from databass.tables import InMemoryTable

simple_qs = [
    "SELECT a FROM data"
]

phase1 = lambda v: "test_phase1"
phase2 = lambda v: "test_phase2"

# run this to test: pytest test/test_column.py -k "test_q"
@pytest.mark.parametrize("q", simple_qs, ids=phase1)
@pytest.mark.usefixtures('context')
def test_q(context, q):
  run_query(context, q)