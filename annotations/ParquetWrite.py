import logging
import os
import traceback

import pandas as pd
from dateutil import parser

from parquet_write_automation.cloud.scripts.main.cloud_factory import CloudFactory
from parquet_write_automation.datastore.scripts.main.db import Db
from parquet_write_automation.exception.scripts.main.exceptions import StorageInvalidCredential, StorageNotReachable, \
    StorageFileNotFound, ReportInvalidSchema, DataCountMismatch, InvalidJobConfig


class ParquetWrite:
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    logger.addHandler(handler)

    def __init__(self):
        try:
            self.gcs = CloudFactory.get_cloud_storage('gcs')
            self.datastore_client = self.gcs.gcs_data_store_client()
            self.storage_client = self.gcs.gcs_auth_client()
            self.upload_client = self.gcs.gcs_auth_client()
            self.db = Db()
        except Exception:
            ex = StorageInvalidCredential("Invalid credentials")
            logging.exception(ex)
            raise ex

    def get_blob_for_merged_report(self, input_bucket_name:str, input_path:str, input_file_name:str)->str:
        """

        :param input_bucket_name: input bucket name
        :param input_path: input gcs path
        :param input_file_name: input file name
        :return: downloaded local file path
        """
        try:
            input_bucket = self.storage_client.bucket(input_bucket_name)
            logging.info(input_path)
            input_blob_file_name = input_bucket.blob(input_path)
            input_blob_file_name.download_to_filename(input_file_name)
            return input_file_name
        except Exception as e:
            logging.exception("exception occur in creating blob for merged report:-  {}".format(traceback.format_exc()))
            raise e

    def put_parquet_file_to_gcs(self, input_file_name:str, column_name_for_report:list, column_data_type_for_report:'dataframe schema',
                                output_format:'parquet/csv/excel', output_file_name:str, output_file_path:str, output_bucket:str, delimiter'/,*,&,@ etc')->int:
        """

        :param input_file_name: input file name in local file system
        :param column_name_for_report: list of columns for data frame
        :param column_data_type_for_report:schema of data frame
        :param output_format: output format of file
        :param output_file_name: output file name
        :param output_file_path: output file path
        :param output_bucket: output bucket name
        :return: number of rows
        """
        df = pd.read_csv(input_file_name, sep=delimiter, names=column_name_for_report,
                         dtype=column_data_type_for_report, engine='python')
        total_row = df.shape[0]

        if output_format == 'parquet':
            df.to_parquet(output_file_name, engine='pyarrow', compression='snappy',
                          allow_truncated_timestamps=True)
        elif output_format == 'csv':
            df.to_csv(output_file_name, index=False)
        elif output_format == 'excel':
            df.to_excel(output_file_name, index=False)
        else:
            logging.info("No proper format to write")
        output_bucket_name = self.upload_client.bucket(output_bucket)
        logging.info(output_file_path)
        output_blob = output_bucket_name.blob(output_file_path)
        logging.info(output_blob)
        output_blob.upload_from_filename(output_file_name)
        return total_row


    def write_merge_report_as_parquet(self, dag_id:'unique dag identifier', run_id:str, report_date:datetime.datetime,
                                      output_file_name:str, output_bucket:str, output_path:str, output_format:'parquet/csv/excel',
                                      column_name_for_report:str, column_data_type_for_report:'data type of column',
                                      report_type, kind:'namespace', input_path_field_name:str, filter_map:dict,
                                      input_bucket_field_name:str, row_count_field_name:str,
                                      airflow_task_id: 'unique name for task ', delimiter:'/,*,&,@ etc',  run_mode='normal'):

        """
        This method reads the merged report from the input path location convert it output format and write back to gcs at
        given output location

        :param row_count_field_name: row count field name in data store entity
        :param input_bucket_field_name: bucket name field name in data store entity
        :param delimiter: delimiter of data frame
        :param airflow_task_id: airflow task id
        :param filter_map: filter map(dict) to filter entity
        :param input_path_field_name: input path field name in entity
        :param kind: kind on which query is done
        :param run_mode: run mode (normal or manual impute)
        :param output_format: output format of file
        :param report_type: report type (ex - POS )
        :param output_path: gcs path where merged file will be stored
        :param output_bucket: output bucket name
        :param report_date: report download date
        :param run_id: dag's run id
        :param dag_id: dag's dag id
        :param output_file_name: file name where the merged report will be written
        :param output_format: format for output file
        :param column_data_type_for_report: data types of report
        :param column_name_for_report: column names of report
        :return: None
        """
        try:
            task_entries = self.db.get_datastore_entries(self.datastore_client, filter_map, kind)
            logging.info(task_entries)

            if len(task_entries) > 0 and \
                    task_entries[0]['status'] == 'success':
                merge_entry = task_entries[0]
            else:
                raise InvalidJobConfig("No input file found", report_date)

            input_bucket_name = merge_entry.get(input_bucket_field_name)
            input_path = merge_entry.get(input_path_field_name)
            job_id = merge_entry.get('job_id')
            logging.info(input_path)
            if input_path is None:
                raise StorageFileNotFound("No input file path found for"
                                          " field {} , kind {}  and filter {}".format(
                    input_path_field_name, kind, filter_map))

            row_count_in_part_file = merge_entry.get(row_count_field_name)

            input_file_name = input_path.split('/')[-1]

            downloaded_merged_report = self.get_blob_for_merged_report(input_bucket_name,
                                                                       input_path,
                                                                       input_file_name)
            output_file_path = os.path.join(output_path, output_file_name)

            total_row = self.put_parquet_file_to_gcs(downloaded_merged_report, column_name_for_report,
                                                     column_data_type_for_report, output_format,
                                                     output_file_name, output_file_path, output_bucket, delimiter)
            if total_row == row_count_in_part_file or run_mode == 'manual':
                if run_mode == 'manual':
                logging.info("Row count in merged file in manual run is {}".format(total_row))
                
              self.db.handle_report_parquet_write_task(self.datastore_client, dag_id, run_id, merge_entry.key.id,
                                                         output_file_name, report_date, column_name_for_report,
                                                         column_data_type_for_report, output_file_path, None,
                                                         '', 'success', output_bucket, total_row,
                                                         row_count_in_part_file, report_type, kind, filter_map,
                                                         airflow_task_id, job_id)

         
            else:
                ex = DataCountMismatch("Row count mismatch exception",
                                       abs(total_row - row_count_in_part_file))
                raise ex

        except Exception as e:
            if e.__class__.__name__ == StorageFileNotFound.__name__:
                ex = StorageFileNotFound("File not found in GCS")
                logging.exception(ex)
                raise ex
            elif e.__class__.__name__ == ReportInvalidSchema.__name__:
                ex = ReportInvalidSchema("Invalid Schema", "")
                logging.exception(ex)
                raise ex
            elif e.__class__.__name__ == StorageNotReachable.__name__:
                ex = StorageNotReachable("Storage Not reachable", "")
                logging.exception(ex)
                raise ex
            else:
                logging.error(e)
                raise e
