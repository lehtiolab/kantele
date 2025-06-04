import os
import sys
from glob import glob
import shutil
from argparse import ArgumentParser

p = ArgumentParser()
p.add_argument('--multi')
p.add_argument('--flag', action='store_const', const=True, default=False)
p.add_argument('--num')
p.add_argument('--name')
p.add_argument('--fp1')
p.add_argument('--fp2')

p.add_argument('--isobaric')
p.add_argument('--input')
p.add_argument('--sampletable')
p.add_argument('--runid')
p.add_argument('--outdir')
p.add_argument('-profile')
p.add_argument('-name')
p.add_argument('-with-weblog')
p.add_argument('-with-trace', action='store_const', const=True, default=False)
p.add_argument('-resume', action='store_const', const=True, default=False)
args = p.parse_args(sys.argv[1:])

os.makedirs(args.outdir)


# raws is a 'fn;fn2;fn3;...' or '*', but we use 1 file (or *) for testing so can use glob
lines = []
with open(args.input) as fp:
    header = next(fp).strip().split('\t')
    for line in fp:
        lines.append(line.strip().split('\t'))

with open(os.path.join(args.outdir, '.nextflow.log'), 'w') as fp:
    pass
