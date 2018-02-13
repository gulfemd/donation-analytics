'''
Run:

python3 main.py --stream /Users/gulfemdemir/Developer/donation-analytics-master-older/input/itcont.txt \
--percentile /Users/gulfemdemir/Developer/donation-analytics-master-older/input/percentile.txt \
--out /Users/gulfemdemir/Developer/donation-analytics-master-older/output/repeat_donors.txt

'''

from argparse import ArgumentParser

logger = logging.getLogger(__name__)

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