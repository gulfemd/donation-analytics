'''
Run:

python3 main.py --stream /Users/gulfemdemir/Developer/donation-analytics/input/itcont.txt \
--percentile /Users/gulfemdemir/Developer/donation-analytics/input/percentile.txt \
--out /Users/gulfemdemir/Developer/donation-analytics/output/repeat_donors.txt

'''

from argparse import ArgumentParser
import logging
from os import path

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

def main(args):
    stream_input = args.stream
    percentile = get_percentile(args.percentile)
    output = args.out

    logger.warn('Checking if {} exists...'.format(stream_input))
    if has_input(stream_input):
        logger.warn('Input file detected, processing...')

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