from google.cloud import datastore
import datetime
import logging
import os


class Db:
    report_download_namespace = 'ReportDownloadTask'
    merge_namespace = 'MergeReportTask'
    parquet_write_namespace = 'ParquetWriteTask'
    parquet_write_config_namespace = 'ParquetWriteConfig'
    merge_config_namespace = 'MergeReportConfig'
    manual_parquet_write_config = "ManualParquetWriteConfig"
    manual_run_merge_config = 'ManualMergeReportConfig'

    def get_datastore_client(self, credentials:'dsClientCredentials') -> google.cloud.datastore.Client:
        """
        get_datastore_client method return the data store client
        :param credentials: gcs auth credential to get data store client
        :return: data store client
        """
        return datastore.Client(credentials=credentials)

    def get_report_task_entry(self, client : google.cloud.datastore.Client, dag_id :'unique dag identifier', run_id:str, airflow_task_id : 'unique name for task ', name_space:'unique name') -> list:

        """
        get_report_task_entry is used to query the entry for task
        :param client: data store client
        :param dag_id: dag's id
        :param run_id: dag's run id
        :param name_space: name space which consist all the task entry
        :return: list of the entry for given dag_id, run_id, report_name and report date
        """
        query = client.query(kind=name_space)
        query.add_filter('dag_id', '=', dag_id)
        query.add_filter('run_id', '=', run_id)
        query.add_filter('airflow_task_id', '=', airflow_task_id)
        return list(query.fetch())

    def get_datastore_entries(self, client : google.cloud.datastore.Client, filter_map : dict , kind :'namespace') -> list:

        """
        get_report_task_entry is used to query the entry for task
        :param filter_map: filter map is dictionary where keys are filters and value are filter's value
        :param client: data store client
        :param kind: name space which consist all the task entry
        :return: list of the entry for given dag_id, run_id, report_name and report date
        """
        query = client.query(kind=kind)
        for key in filter_map.keys():
            query.add_filter(key, '=', filter_map[key])
        return list(query.fetch())

    def put_parquet_write_task_entry(self, client :google.cloud.datastore.Client, task_entry, dag_id:'unique dag identifier', run_id :str, report_merged_task_ids: 'unique name for task', report_name:str,
                                     report_date: datetime.datetime, report_column:str, report_column_datatype:'data type of column', gcs_output_file_path:str,
                                     exception:str,
                                     exception_details:'detailed_text', job_status:'success/failure', output_bucket:str,
                                     total_row:int, row_count_in_part_file:int, commit_id:str, report_type:'type of report',
                                     kind:'namespace', filter_map: dict , airflow_task_id: 'unique name for task ', job_id:'str/int') ->'created a new parquet write task':
        """
        put_parquet_write_task_entry is used to insert the task instance for parquet_write task
        :param job_id: job id of wrl
        :param airflow_task_id: airflow task id
        :param filter_map: filter map passed as argument
        :param kind: kind passed as argument
        :param commit_id: github commit id sha
        :param client: datastore client
        :param task_entry: Entity which consist all the task instances
        :param dag_id: dag's id
        :param run_id: dag's run id
        :param report_merged_task_ids: task id of the instance of merged report stored in gcs
        :param report_name: parquet write report name
        :param report_date: report date
        :param report_column: column name of the report
        :param report_column_datatype: column data type of the report
        :param gcs_output_file_path: path where parquet file will be stored
        :param exception: exception type occured
        :param exception_details: exception details(traceback)
        :param job_status: status (success if no exception occur else fail )
        :param output_bucket: bucket name where the parquet file will be stored
        :param total_row: total row in merge file
        :param row_count_in_part_file: total row in part file
        :param report_type: report type
        :return:
        """
        task_entry['dag_id'] = dag_id
        task_entry['run_id'] = run_id
        task_entry['report_name'] = report_name
        task_entry['report_date'] = report_date
        task_entry['report_merged_ids'] = report_merged_task_ids
        task_entry['gcs_output_file_path'] = gcs_output_file_path
        task_entry['modified_at'] = datetime.datetime.utcnow()
        task_entry['column'] = report_column
        task_entry['column_datatype'] = report_column_datatype
        task_entry['exceptions'] = exception
        task_entry['exception_details'] = exception_details
        task_entry['status'] = job_status
        task_entry['output_bucket'] = output_bucket
        task_entry['total_row_in_merged_report'] = total_row
        task_entry['row_sum_of_part_files'] = row_count_in_part_file
        task_entry['github_commit_sha'] = commit_id
        task_entry['report_type'] = report_type
        task_entry['filter_kind'] = kind
        task_entry['filter_map'] = filter_map
        task_entry['airflow_task_id'] = airflow_task_id
        task_entry['job_id'] = job_id

        print(task_entry)
        client.put(task_entry)

    def handle_report_parquet_write_task(self, client:google.cloud.datastore.Client, dag_id:'unique dag identifier', run_id : str, report_merged_task_id: 'unique name for task ', output_file_name:str,
                                         report_date:datetime.datetime, report_column;str, report_column_datatype:'data type of column', gcs_output_file_path :str,
                                         exception:str, exception_details:'detailed_text', job_status:'success/failure', output_bucket:, total_row:int,
                                         row_count_in_part_file:int, report_type:'type of report',
                                         kind:'namespace', filter_map: dict , airflow_task_id: 'unique name for task ', job_id:'str/int')->'create or update parquet write task':
        """
        handle_report_parquet_write_task is used to check if the task instance for the given param is available in the
        datastore or not. If it is already there then it is being updated else a new instance is being inserted
        :param job_id: job id of wrl
        :param airflow_task_id: airflow task id
        :param filter_map: filter map passed as dag argument
        :param kind: kind passed as argument
        :param client: datastore client
        :param dag_id: dag's id
        :param run_id: dag's run id
        :param report_merged_task_id: task id of the instance of merged report stored in gcs
        :param output_file_name: parquet write report name
        :param report_date: report date
        :param report_column: column name of the report
        :param report_column_datatype: column data type of the report
        :param gcs_output_file_path: path where parquet file will be stored
        :param exception: exception type occured
        :param exception_details: exception details(traceback)
        :param job_status: status (success if no exception occur else fail )
        :param output_bucket: bucket name where the parquet file will be stored
        :param total_row: total row in merge file
        :param row_count_in_part_file: total row in part file
        :param report_type: report_type
        :return:
        """
        existing_entries = self.get_report_task_entry(client, dag_id, run_id, airflow_task_id, self.parquet_write_namespace)
        commit_id = ''
        if 'GITHUB_SHA' in os.environ:
            commit_id = os.environ['GITHUB_SHA']

        if len(existing_entries) > 0:
            task_entry = existing_entries[0]
        else:
            task_key = client.key(self.parquet_write_namespace)
            task_entry = datastore.Entity(key=task_key, exclude_from_indexes=('exception_details',))
            task_entry['created_at'] = datetime.datetime.utcnow()
        self.put_parquet_write_task_entry(client, task_entry, dag_id, run_id, report_merged_task_id, output_file_name,
                                          report_date, report_column, report_column_datatype, gcs_output_file_path,
                                          exception, exception_details, job_status, output_bucket, total_row,
                                          row_count_in_part_file, commit_id, report_type, kind, filter_map,
                                          airflow_task_id, job_id)
