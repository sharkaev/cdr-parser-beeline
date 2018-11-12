# Module to parse all files from folder
import argparse
import os
import time
import sys
from file_parser import Parser
import psycopg2
import config
import pprint


def get_first_file(model, cursor):
    """Function to get first inserted file by smartSync due this session"""
    sql = "select {0}_smart_sync_first_file from app_info where id=1".format(
        model)
    cursor.execute(sql)
    data = cursor.fetchone()
    return data[0]


def get_all_inserted_files_from_db(model, cursor):
    """Function to get all inserted files from this model"""
    sql = "select filename from {0}_files".format(model)
    cursor.execute(sql)
    data = cursor.fetchall()
    return [record[0] for record in data]


def get_all_files_in_folder(folder):
    all_files_in_folder = os.listdir(folder)
    full_list = [i for i in all_files_in_folder]
    #time_sorted_tuple = sorted(full_list, key=os.path.getmtime)
    return full_list


def dataInsert(filename, file_parser, folder, model):
    start_time = time.time()
    path_to_temp_data = file_parser.get_tmp_data_by_model(filename)
    file_parser.insert_tmp_data_to_db(path_to_temp_data)
    hrtime = time.time() - start_time
    file_parser.insert_file(filename=filename, folder=folder, hrtime=hrtime)
    print(filename, hrtime)


def get_files_to_parse(start_file=None, end_file=None):
    all_files_in_folder = os.listdir(folder)
    full_list = (os.path.join(folder, i) for i in all_files_in_folder)
    time_sorted_list = sorted(full_list, key=os.path.getmtime)
    start_file_index = None
    end_file_index = None
    # verify  start file

    if start_file is not None:
        if start_file in time_sorted_list:
            start_file_index = time_sorted_list.index(start_file)
        elif os.path.join(folder, start_file) in time_sorted_list:
            start_file_index = time_sorted_list.index(
                os.path.join(folder, start_file))

    # verify end file
    if end_file is not None:
        if end_file in time_sorted_list:
            end_file_index = time_sorted_list.index(end_file)
        elif os.path.join(folder, end_file) in time_sorted_list:
            end_file_index = time_sorted_list.index(
                os.path.join(folder, end_file))

    if start_file_index is not None and end_file_index is None:
        files_to_return = time_sorted_list[start_file_index:]
    elif end_file_index is not None and start_file_index is None:
        files_to_return = time_sorted_list[end_file_index]
    elif start_file_index is not None and end_file_index is not None:
        files_to_return = time_sorted_list[start_file_index:end_file_index+1]
    else:
        files_to_return = time_sorted_list

    return files_to_return


ap = argparse.ArgumentParser()
ap.add_argument("--folder", help="directory to watch")
ap.add_argument("--model", help="Model to implement")
ap.add_argument("--delimiter", help="Delimiter for file data")
ap.add_argument("--start_file", help="start parse from this file")
ap.add_argument("--end_file", help="end parse with this file")
args = vars(ap.parse_args())
folder = args['folder']
model = args['model']
delimiter = args['delimiter']
dbname = ''
if model == 'sms':
    dbname = 'sms_files'
elif model == 'voice':
    dbname = 'voice_files'
elif model == 'network':
    dbname = 'network_files'

conn = psycopg2.connect(config.cdr_parser_beeline_connect_string)
cursor = conn.cursor()

start_file = None if args['start_file'] is None else args['start_file']
end_file = None if args['end_file'] is None else args['end_file']

file_parser = Parser(model=model, dbname=dbname,
                     delimiter=delimiter, engine='core')

print('Core sync is running', model, 'folder: ', folder)
while start_file is None or start_file == 'None':
    start_file = get_first_file(model=model, cursor=cursor)
    time.sleep(5)
try:
    all_files_in_folder = get_all_files_in_folder(folder)
    all_inserted_files = get_all_inserted_files_from_db(
        model=model, cursor=cursor)
    needed_files = []
    inserted_files = 0
    for f in all_files_in_folder:
        if f not in all_inserted_files:
            needed_files.append(f)
        else:
            inserted_files += 1
    # run parser
    for f in needed_files:
        try:
            dataInsert(filename=os.path.join(folder, f), file_parser=file_parser,
                       folder=folder, model=model)
        except Exception as e:
            print(e)
except Exception as e:
    print(e)
finally:
    cursor.close()
    conn.close()
