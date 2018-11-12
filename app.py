import sys
import os
import argparse
import subprocess
import psycopg2
import datetime
import config
ap = argparse.ArgumentParser()
ap.add_argument("--sms", help="directory with sms data")
ap.add_argument('--voice', help="directory with voice data")
ap.add_argument('--network', help="directory with network data")
ap.add_argument('--delimiter', help="dilimeter character")
args = vars(ap.parse_args())
if args['sms'] == None:
    args['sms'] = config.default_sms_folder
if args['voice'] == None:
    args['voice'] = config.default_voice_folder
if args['network'] == None:
    args['network'] = config.default_network_folder
if args['delimiter'] == None:
    args['delimiter'] = "|"
today = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

try:
    conn = psycopg2.connect(config.cdr_parser_beeline_connect_string)
    cursor = conn.cursor()
    # update info
    sql = "UPDATE app_info  SET started_at = '{0}', sms_smart_sync_first_file = 'None', voice_smart_sync_first_file = 'None', network_smart_sync_first_file = 'None' where id = 1".format(
        today)
    # print(sql)
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()
except:
    sys.exit('Can\'t connect to database')


for arg in args:
    if arg != 'delimiter':
        print('start py -3 model.py --folder {0} --delimiter "{1}" --model {2}'.format(
            args[arg], args['delimiter'], arg))

        subprocess.call('py -3 model.py --folder {0} --delimiter "{1}" --model {2}'.format(
            args[arg], args['delimiter'], arg), shell=True)

    # Run SmartCore.py in par
