'''
Run:

python3 main.py --stream /Users/gulfemdemir/Developer/donation-analytics-master-older/input/itcont.txt \
--percentile /Users/gulfemdemir/Developer/donation-analytics-master-older/input/percentile.txt \
--out /Users/gulfemdemir/Developer/donation-analytics-master-older/output/repeat_donors.txt

'''

from argparse import ArgumentParser

def main(args):
    print(args.stream)
    print(args.percentile)
    print(args.out)

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