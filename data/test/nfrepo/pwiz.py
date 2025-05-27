import os
import sys
from glob import glob
import shutil
from argparse import ArgumentParser

p = ArgumentParser()
p.add_argument('--raws')
p.add_argument('--filters')
p.add_argument('--outdir')
p.add_argument('--container')
p.add_argument('--options')
p.add_argument('-profile')
p.add_argument('-with-trace', action='store_const', const=True, default=False)
p.add_argument('-resume', action='store_const', const=True, default=False)
args = p.parse_args(sys.argv[1:])

os.makedirs(args.outdir)

# raws is a fn, or '*'
for fn in glob(args.raws):
    newfn = os.path.basename(f'{os.path.splitext(fn)[0]}.mzML')
    with open(os.path.join(args.outdir, newfn), 'w') as fp:
        fp.write(f'{newfn} mzML file')
