import subprocess
import os
import sys
import argparse
import time
ap = argparse.ArgumentParser()
ap.add_argument('--folder', '-f', help="Folder to watch")
ap.add_argument('--model', '-m',
                help="Data Module to implement. Ex: SMS, VOICE, NETWORK")
ap.add_argument('--delimiter', help="Delimiter")

args = vars(ap.parse_args())
folder = args['folder']
model = args['model']
delimiter = args['delimiter']

# first run smartSync
try:
    subprocess.call('start py -3 smartSync.py --folder {0} --model {1} --delimiter "{2}"'.format(
        folder, model, delimiter), shell=True)
    subprocess.call('start py -3 coreSync.py --folder {0} --model {1} --delimiter "{2}"'.format(
        folder, model, delimiter), shell=True)
except Exception as e:
    print(e)
    # input()
