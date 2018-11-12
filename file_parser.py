import pandas as pd
import config
import psycopg2
from datetime import datetime
import os
import sys


class Parser:
    def __init__(self, model, dbname, delimiter, engine):
        """ Initialize Parser Object"""
        self.model = model
        self.dbname = dbname
        self.delimiter = delimiter
        self.engine = engine
        self.header = [i for i in config.json_models[model]['columns'].keys()]
        self.voice_header = [
            i for i in config.json_models['voice']['columns'].keys()]
        self.voice_header.append('direction_key')
        self.sms_header = [
            i for i in config.json_models['sms']['columns'].keys()]
        self.sms_header.append('direction_key')

        self.connection = psycopg2.connect(config.parser_connect_string)
        self.cursor = self.connection.cursor()
        self.app_connection = psycopg2.connect(
            config.cdr_parser_beeline_connect_string)
        self.app_cursor = self.app_connection.cursor()

    def update_first_smart_sync_file(self, filename, folder):
        """Update first {model}_smart_sync_file"""

        file_name = filename.replace(folder+"\\", '')
        sql = "UPDATE app_info  SET {0}_smart_sync_first_file = '{1}' where id = 1".format(
            self.model, file_name)

        self.app_cursor.execute(sql)
        self.app_connection.commit()

    def insert_file(self, filename, folder, hrtime):
        """Function to insert data about file stat"""
        file_stat = os.stat(filename)
        file_size = round(file_stat.st_size / float(1 << 20), 3)
        file_length = sum(1 for line in open(filename))
        file_name = filename.replace(folder+'\\', '')
        file_created_at = datetime.fromtimestamp(
            file_stat.st_ctime).strftime('%Y-%m-%d %H:%M:%S')

        sql = "INSERT INTO {0} (filename, amount_of_rows, hrtime, file_created_at, file_size) values ('{1}', '{2}', '{3}', '{4}', '{5}')".format(
            self.dbname, file_name, file_length, hrtime, file_created_at, file_size)
        self.app_cursor.execute(sql)
        self.app_connection.commit()

    def get_tmp_data_by_model(self, filename):
        """Function to create tmp  files by module and engine"""
        model = self.model
        usecols = [i for i in config.json_models[model]['columns'].values()]
        try:
            df = pd.read_csv(filename, index_col=None, header=None, sep=self.delimiter,
                             usecols=usecols)[usecols]

            path_to_temorary_data = 'data/tmp/{0}_{1}.csv'.format(
                self.engine, self.model)
            df.columns = self.header
            dtypes = config.json_models[self.model]['dtypes']

            for dtype in dtypes:
                if dtypes[dtype] == 'datetime':
                    df[dtype] = pd.to_datetime(
                        df[dtype], format="%Y%m%d%H%M%S")
            df.to_csv(path_to_temorary_data, index=False, sep=self.delimiter)
            return path_to_temorary_data
        except Exception as e:
            print(e)

    def is_beeline(self, phone_number):
        if len(phone_number) == 12:
            if phone_number[:3] == "996":
                if phone_number[3:5] == "77" or phone_number[3:5] == "20":
                    return True
            else:
                return False
        else:
            return False

    def insert_tmp_data_to_db(self, filename):
        """Function to insert tmp_data from filename to db"""
        if self.model == 'sms':
            tmp_data = []
            df = pd.read_csv(filename, sep=self.delimiter)
            df[['ctn', 'b_phone_num']] = df[['ctn', 'b_phone_num']].astype(str)

            for row in df.itertuples():
                r_dt = getattr(row, 'dt')
                r_calling_party_number = getattr(row, 'ctn')
                r_called_party_number = getattr(row, 'b_phone_num')
                r_sms_length = getattr(row, 'sms_length')
                calling_party_number_is_beeline = self.is_beeline(
                    r_calling_party_number)
                called_party_number_is_beeline = self.is_beeline(
                    r_called_party_number)
                if called_party_number_is_beeline is True and called_party_number_is_beeline is False:
                    # direction key 1 if ctn is beeline
                    tmp_tuple = (
                        r_dt,
                        r_calling_party_number,
                        r_called_party_number,
                        r_sms_length,
                        1
                    )

                    tmp_data.append(tmp_tuple)
                elif calling_party_number_is_beeline is False and called_party_number_is_beeline is True:
                    # direction key 2 if ctn is not beeline and b_phone_num is beeline
                    tmp_tuple_2 = (
                        r_dt,
                        r_called_party_number,
                        r_calling_party_number,
                        r_sms_length,
                        2

                    )
                    tmp_data.append(tmp_tuple_2)
                elif calling_party_number_is_beeline is True and called_party_number_is_beeline is True:
                    tmp_tuple_3 = (
                        r_dt,
                        r_calling_party_number,
                        r_called_party_number,
                        r_sms_length,
                        1

                    )

                    tmp_data.append(tmp_tuple_3)

                    tmp_tuple_4_ = (
                        r_dt,
                        r_called_party_number,
                        r_calling_party_number,
                        r_sms_length,
                        2

                    )

                    tmp_data.append(tmp_tuple_4_)

            df = pd.DataFrame(data=list(tmp_data), columns=self.sms_header)

            df.to_csv(filename, index=False, sep=self.delimiter)

        elif self.model == 'voice':
            tmp_data = []
            df = pd.read_csv(filename, sep=self.delimiter)
            df[['ctn', 'b_phone_num']] = df[['ctn', 'b_phone_num']].astype(str)
            dtypes = config.json_models[self.model]['dtypes']

            for row in df.itertuples():
                r_dt = getattr(row, 'dt')
                r_calling_party_number = getattr(row, 'ctn')
                r_called_party_number = getattr(row, 'b_phone_num')
                r_duration_nval = getattr(row, 'duration_nval')
                r_calling_cell_id = getattr(row, 'calling_cell_id')
                r_called_cell_id = getattr(row, 'called_cell_id')

                calling_party_number_is_beeline = self.is_beeline(
                    r_calling_party_number)
                called_party_number_is_beeline = self.is_beeline(
                    r_called_party_number)
                if called_party_number_is_beeline is True and called_party_number_is_beeline is False:
                    # direction key 1 if ctn is beeline
                    tmp_tuple = (
                        r_dt,
                        r_calling_party_number,
                        r_called_party_number,
                        r_duration_nval,
                        r_calling_cell_id,
                        r_called_cell_id,
                        1
                    )

                    tmp_data.append(tmp_tuple)
                elif calling_party_number_is_beeline is False and called_party_number_is_beeline is True:
                    # direction key 2 if ctn is not beeline and b_phone_num is beeline
                    tmp_tuple_2 = (
                        r_dt,
                        r_called_party_number,
                        r_calling_party_number,
                        r_duration_nval,
                        r_calling_cell_id,
                        r_called_cell_id,
                        2

                    )
                    tmp_data.append(tmp_tuple_2)
                elif calling_party_number_is_beeline is True and called_party_number_is_beeline is True:
                    tmp_tuple_3 = (
                        r_dt,
                        r_calling_party_number,
                        r_called_party_number,
                        r_duration_nval,
                        r_calling_cell_id,
                        r_called_cell_id,
                        1

                    )

                    tmp_data.append(tmp_tuple_3)

                    tmp_tuple_4_ = (
                        r_dt,
                        r_called_party_number,
                        r_calling_party_number,
                        r_duration_nval,
                        r_calling_cell_id,
                        r_called_cell_id,
                        2

                    )

                    tmp_data.append(tmp_tuple_4_)

            df = pd.DataFrame(data=list(tmp_data), columns=self.voice_header)

            df.to_csv(filename, index=False, sep=self.delimiter)
        with open(filename, 'r') as f:
            next(f)
            self.cursor.copy_from(f, self.model, sep=self.delimiter)
            self.connection.commit()
