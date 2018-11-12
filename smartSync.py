from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler, FileSystemEventHandler
import argparse
from time import sleep
import config
from file_parser import Parser
import time

ap = argparse.ArgumentParser()
ap.add_argument('--folder', '-f', type=str,
                help="Folder to watch", required=True)
ap.add_argument('--delimiter', '-d', type=str, help="Delimiter Symbol")
ap.add_argument('--model', '-m', type=str, help="Delimiter Symbol")
args = vars(ap.parse_args())
model = args['model']
folder = args['folder']
delimiter = args['delimiter']

dbname = ''
if model == 'sms':
    dbname = 'sms_files'
elif model == 'voice':
    dbname = 'voice_files'
elif model == 'network':
    dbname = 'network_files'

file_parser = Parser(model=model, dbname=dbname,
                     delimiter=delimiter, engine='smart')


print('Started watching ', folder, 'folder')
first_file = None


def dataInsert(filename):
    start_time = time.time()
    global file_parser, folder, model, first_file
    path_to_temorary_data = file_parser.get_tmp_data_by_model(filename)
    file_parser.insert_tmp_data_to_db(path_to_temorary_data)
    hrtime = time.time() - start_time
    file_parser.insert_file(filename=filename, folder=folder, hrtime=hrtime)
    # update model_smart_first_file
    if first_file is None:
        file_parser.update_first_smart_sync_file(filename, folder)
        first_file = filename
    print(filename, hrtime)


class DataHandler(FileSystemEventHandler):
    def on_moved(self, event):
        try:
            dataInsert(filename=event.dest_path)
        except Exception as e:
            print(e)

    def on_created(self, event):
        try:
            time.sleep(0.1)
            dataInsert(filename=event.dest_path)
        except Exception as e:
            print(e)


observer = Observer()
observer.schedule(DataHandler(), folder, recursive=True)
observer.start()

try:
    while True:
        sleep(0.1)
except KeyboardInterrupt:
    observer.stop()
observer.join()
