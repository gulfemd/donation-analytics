'''
Run:

python3 main.py --stream /Users/gulfemdemir/Developer/donation-analytics/input/itcont.txt \
--percentile /Users/gulfemdemir/Developer/donation-analytics/input/percentile.txt \
--out /Users/gulfemdemir/Developer/donation-analytics/output/repeat_donors.txt

'''

from argparse import ArgumentParser
import logging
from os import path
from collections import defaultdict
from datetime import datetime
import pandas as pd
import math

logger = logging.getLogger(__name__)

# all fields that will be reported in the input file based on The Federal Election Commission's data dictionary
FEC_COLUMNS = ['CMTE_ID','AMNDT_IND','RPT_TP','TRANSACTION_PGI','IMAGE_NUM','TRANSACTION_TP','ENTITY_TP','NAME',
    'CITY','STATE','ZIP_CODE','EMPLOYER','OCCUPATION','TRANSACTION_DT','TRANSACTION_AMT','OTHER_ID','TRAN_ID',
    'FILE_NUM','MEMO_CD','MEMO_TEXT','SUB_ID']
# fields that are required for analysis out of above columns
REQUIRED_COLUMNS = ['CMTE_ID','NAME','ZIP_CODE','TRANSACTION_DT','TRANSACTION_AMT','OTHER_ID']

# constants
DATE_FORMAT = '%m%d%Y'
ZIP_CODE_LEN = 5
DTYPE_DICT = {
    'CMTE_ID' : str,
    'NAME' : str,
    'ZIP_CODE' : str,
    'TRANSACTION_DT' : str,
}
CHUNKSIZE = 100000

def apply_filters(row):
    '''
    Filters out the transactions with invalid ZIP_CODE or malformatted TRANSACTION_DT
    '''
    zip_code = date = None
    if len(str(row.ZIP_CODE)) < ZIP_CODE_LEN:
        return zip_code, date
    zip_code = str(row.ZIP_CODE)[:ZIP_CODE_LEN]
    try:
        date = datetime.strptime(str(row.TRANSACTION_DT), DATE_FORMAT).date()
    except:
        return zip_code, date

    return zip_code, date

def is_repeat_donor(row, zip_code, date, recorded_donors):
    '''
    Identifies whether given row(transaction) is made by a repeat donor by checking all previously seen donors from recorded_donors dictionary
    '''
    donor_id = row.NAME + ' ' + zip_code

    if donor_id in recorded_donors:
        previous_donation_year = recorded_donors[donor_id]

        if previous_donation_year < date.year:
            return True

    recorded_donors[donor_id] = date.year
    return False

def has_input(filepath):
    '''
    Checks if the file at the given path exists.
    '''
    return path.exists(filepath)

def get_percentile(in_filepath):
    '''
    Processes the given percentile file.
    '''
    logger.warn('Checking if {} exists...'.format(in_filepath))

    if not has_input(in_filepath):
        logger.error('Could not find {}...'.format(in_filepath))
        raise IOError('No input file')
    else:
        with open(in_filepath) as fp:
            percentile = fp.readline().strip()
            logger.warn('Percentile file detected, percentile: {}'.format(percentile))
            return int(percentile)

def calculate(percentile, values):
    '''
    Calculates and returns three required fields for the output data:
        running percentile of contributions,
        total amount of contributions,
        total number of transactions
    '''
    ordinal_rank = math.ceil((percentile / 100) * len(values))
    percentile_value = round(values[ordinal_rank - 1])
    total_number = len(values)
    total_amount = sum(values)

    return [percentile_value, total_amount, total_number]

def process_stream(in_filepath, percentile, out_filepath):

    recorded_donors = {}
    recipient_based_history = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    with open(in_filepath) as fp, open(out_filepath, 'w') as op:
        chunks = pd.read_csv(fp, sep='|', header=None, names=FEC_COLUMNS, usecols=REQUIRED_COLUMNS, chunksize=CHUNKSIZE, dtype=DTYPE_DICT)

        # iterate over chunks (parts of the input file) due to memory reasons
        for chunk in chunks:
            # filter the row if any of the following conditions hold
            # any of these subset of fields is empty or,
            # OTHER_ID is not empty
            # TRANSACTION_AMT is smaller than zero -> Minus donation?
            chunk = chunk.dropna(subset=['CMTE_ID','NAME','ZIP_CODE','TRANSACTION_DT','TRANSACTION_AMT'])
            chunk = chunk[chunk['OTHER_ID'].isnull()]
            chunk = chunk[chunk['TRANSACTION_AMT'] > 0]

            # iterates over each row in the chunk, and checks whether it's repeat_donor or not,
            # and if so, calculates required values
            for row in chunk.itertuples():
                zip_code, date = apply_filters(row)
                if date is not None and zip_code is not None:
                    if is_repeat_donor(row, zip_code, date, recorded_donors):

                        logger.warn('REPEAT donor detected: {}'.format(str(row)))

                        to_print = [row.CMTE_ID, zip_code, date.year]
                        recipient_based_history[row.CMTE_ID][zip_code][date.year].append(row.TRANSACTION_AMT)
                        to_print += calculate(percentile, recipient_based_history[row.CMTE_ID][zip_code][date.year])

                        op.write('|'.join(map(str, to_print)))
                        op.write('\n')

    return

def main(args):
    stream_input = args.stream
    percentile = get_percentile(args.percentile)
    output = args.out

    logger.warn('Checking if {} exists...'.format(stream_input))
    if has_input(stream_input):
        logger.warn('Input file detected, processing...')
        return process_stream(
            stream_input,
            percentile,
            output
        )

    return 1

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument(
        '--stream',
        help='Path to the itcont.txt file',
        required=True
    )
    parser.add_argument(
        '--percentile',
        help='Path to the percentile.txt file',
        required=True
    )
    parser.add_argument(
        '--out',
        help='Path to the output for repeat_donors.txt',
        required=True
    )

    args = parser.parse_args()

    exit(main(args))