import os
import sys
import json
from argparse import ArgumentParser

p = ArgumentParser()
p.add_argument('--instrument')
p.add_argument('--dia', action='store_const', const=True, default=False)
p.add_argument('--raw')

p.add_argument('--outdir')
p.add_argument('-c', required=True)
p.add_argument('-name')
p.add_argument('-with-weblog')
p.add_argument('-with-trace', action='store_const', const=True, default=False)
p.add_argument('-resume', action='store_const', const=True, default=False)
args = p.parse_args(sys.argv[1:])

os.makedirs(args.outdir)


if not os.path.exists(args.raw):
    print('Cannot find raw file')
    sys.exit(1)

with open('.nextflow.log', 'w') as fp:
    pass

with open('trace.txt', 'w') as fp:
    fp.write('\t'.join(['exit', 'name']))

qcout = {
        'nrpsms': 1,
        'p_error': {
            'q1': 1,
            'q2': 2,
            'q3': 3,
            },
        'missed_cleavages': {
            1: 1,
            2: 3,
            },
        }

with open(os.path.join(args.outdir, 'qc.json'), 'w') as fp:
    json.dump(qcout, fp)
