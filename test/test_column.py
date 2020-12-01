import random
import pandas as pd
import numpy as np
from .conftest import *
from databass import *
from databass.tables import InMemoryTable

simple_qs = [
    "SELECT d1.a, d2.b FROM data AS d1, data AS d2",
    "SELECT d1.a, d2.b FROM data AS d1, data AS d2 WHERE d1.a=d2.a",
    # "SELECT d1.a, d2.b FROM data AS d1, (SELECT * FROM data AS d1) AS d2 WHERE d1.a=d2.a",
    #"SELECT a, a+b FROM data AS d1",
    #"SELECT a, a+b FROM data AS d1 WHERE a > 5",
]

# run this to test: pytest test/test_column.py -k "test_q" -s --disable-warnings
@pytest.mark.parametrize("q", simple_qs)
@pytest.mark.usefixtures('context')
def test_q(context, q):
  run_query(context, q)