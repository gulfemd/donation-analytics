# Donation Analytics

## Requirements
This project requires Python 3.

In Ubuntu, Mint and Debian you can install Python 3 like this:

```
  $ sudo apt-get install python3 python3-pip
```
Required libraries can be downloaded:

```
  pip3 install pandas
  pip3 install argparse
  pip3 install pytest
```
## How to use

2 different scripts:

`main.py` : main script to be called (see instructions on `run.sh`)

`tests.py` : file that contains unit-tests implemented (to run: `python3 -m pytest tests.py`)

## Specifications

`chunksize` : Since there is not any upper limit about the size of the input file, and it's possible that we might have some problems about fitting all file into the memory, this script is reading the file as chunks where each of them will contain `100000` lines.

`recorded_donors` : This is a dictionary which holds unique donor ids (donor name + ' ' + zip_code ) as keys and the earliest year that we have seen a transaction from this donor so far as values.

`recipient_based_history` : This is a nested dictionary with the form:
    {'CMTE_ID' : {'ZIP_CODE': {year: [values]}}}

These two dictionaries provides us constant time access to the previously seen data. But, since we still need to go over the file, the complexity is O(n).




