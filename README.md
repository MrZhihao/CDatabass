
<img src="./docs/logo-small.png" width=200 />

The project is originated from [Databass](https://github.com/w6113/databass-public) - a query compilation engine built for Columbia's database courses [W6113](https://w6113.github.io). CDatabass aims to migrate the whole execution model from row-based to column-based for read-only optimization, built on [Apache Arrow](https://arrow.apache.org). For the sake of taking advantages of column-oriented execution model, CDatabass introduces late materialization and compression techniques for specific scenarios.
## Major Modifications

The original Databass system is split into parser, operator definitons and interpretor, optimizer, and compilation. The most work was done for parser, operator and interpretor parts. But CDatabass didn't touch compilation part and the optimizer only got a minor edit. For more design details, you may dive into the [project paper](./docs/Columnar_Extension_for_Databass.pdf).

## Getting Started

Installation using Python 3

    git clone git@github.com:MrZhihao/CDatabass.git

	# create a virtualenv
	cd CDatabass
	python3 -m venv env
	source env/bin/activate

    # install the needed python packages
    pip install -r requirements.txt


The repo includes incomplete test cases that you can run using `pytest` and the used dataset is TCP-H-small.
For quick hacks, and to see how databass compiles different query plans, I use the scaffold in test.py:

    python test.py


### Run Tests

To run tests, use the `pytest` python test framework by specifying which tests in the `test/` directory to run:

    pytest test/*.py


