import mock
import google.auth.credentials
import os
import time
import pytest
from download_automation_walmart_mx.datastore.scripts.main.db import Db

@pytest.fixture
def db():
    datastore = Db()
    return datastore

def get_local_datastore_client(db):
    credentials = mock.Mock(spec=google.auth.credentials.Credentials)
    return db.get_datastore_client(credentials)


def delete_tasks(client, namespace):
    query = client.query(kind=namespace)
    tasks = list(query.fetch())
    for task in tasks:
        client.delete(task.key)


def test_report_download_task(db):
    client = get_local_datastore_client(db)
    delete_tasks(client, 'ReportDownloadTask')
    time.sleep(1)
    tasks = db.get_report_task_entry(client, 'dag1_id', 'run1_id', 'airflow_task_id', 'ReportDownloadTask')
    assert len(tasks) == 0

 db.handle_report_parquet_write_task(client, 'dag1_id', 'run1_id', 'merge1_id' , 'outputfilename', '2020-07-27',
                                        'wrl', 'column',  'reportcolumn', 'columndatatype' ,'output path', 'exception', 'exceptiondetails',
                                        'fail', 'outbucketname', 'row', 'rowcount', 'reporttype' , 'kind' , 'filtermap' , 'airflow_task_id', 'job_id' )
    time.sleep(1)
    tasks = db.get_report_task_entry(client, 'dag1_id', 'run1_id', 'airflow_task_id', 'ReportDownloadTask')
    # print(tasks)
    assert len(tasks) == 1
    assert tasks[0].get('status') == 'fail'

   db.handle_report_parquet_write_task(client, 'dag1_id', 'run1_id', 'merge1_id' , 'outputfilename', '2020-07-27',
                                        'wrl', 'column',  'reportcolumn', 'columndatatype' ,'output path', 'exception', 'exceptiondetails',
                                        'success', 'outbucketname', 'row', 'rowcount', 'reporttype' , 'kind' , 'filtermap' , 'airflow_task_id', 'job_id' )
    time.sleep(1)
    tasks = db.get_report_task_entry(client, 'dag1_id', 'run1_id', 'airflow_task_id', 'ReportDownloadTask')
    assert len(tasks) == 1
    assert tasks[0].get('status') == 'success'

    # Creating a second entry for merge testing
 db.handle_report_parquet_write_task(client, 'dag1_id', 'run1_id', 'merge_id' , 'outputfilename', '2020-07-27',
                                        'wrl', 'column',  'reportcolumn', 'columndatatype' ,'output path', 'exception', 'exceptiondetails',
                                        'success', 'outbucketname', 'row', 'rowcount', 'reporttype' , 'kind' , 'filtermap' , 'airflow_task_id', 'job_id' )
    time.sleep(1)
    tasks = db.get_report_task_entries(client, 'dag1_id', 'run1_id', 'airflow_task_id', 'ReportDownloadTask')
    assert len(tasks) == 2


def test_merge_report_download_task(db):
    client = get_local_datastore_client(db)
    delete_tasks(client, 'MergeReportTask')
    time.sleep(1)
    tasks = db.get_report_task_entry(client, 'dag2_id', 'run2_id', 'airflow_task_id', 'MergeReportTask')
    print(tasks)
    assert len(tasks) == 0

 db.handle_report_parquet_write_task(client, 'dag2_id', 'run2_id', 'merge2_id' , 'outputfilename', '2020-07-27',
                                        'wrl', 'column',  'reportcolumn', 'columndatatype' ,'output path', 'exception', 'exceptiondetails',
                                        'fail', 'outbucketname', 'row', 'rowcount', 'reporttype' , 'kind' , 'filtermap' , 'airflow_task_id', 'job_id' )
    time.sleep(1)
    tasks = db.get_report_task_entry(client, 'dag2_id', 'run2_id', 'airflow_task_id', 'MergeReportTask')
    """print(tasks)"""
    assert len(tasks) == 1
    assert tasks[0].get('status') == 'fail'

 db.handle_report_parquet_write_task(client, 'dag2_id', 'run2_id', 'merge2_id' , 'outputfilename', '2020-07-27',
                                        'wrl', 'column',  'reportcolumn', 'columndatatype' ,'output path', 'exception', 'exceptiondetails',
                                        'success', 'outbucketname', 'row', 'rowcount', 'reporttype' , 'kind' , 'filtermap' , 'airflow_task_id', 'job_id' )
    time.sleep(1)
    tasks = db.get_report_task_entry(client, 'dag2_id', 'run2_id', 'airflow_task_id', 'MergeReportTask')
    """print(tasks)"""
    assert len(tasks) == 1
    assert tasks[0].get('status') == 'success'


def test_parquet_write_task(db):
    client = get_local_datastore_client(db)
    delete_tasks(client, 'ParquetWriteTask')
    time.sleep(1)
    tasks = db.get_report_task_entry(client, 'dag3_id', 'run3_id', 'airflow_task_id', 'ParquetWriteTask')
    print(tasks)
    assert len(tasks) == 0

 db.handle_report_parquet_write_task(client, 'dag3_id', 'run3_id', 'merge3_id' , 'outputfilename', '2020-07-27',
                                        'wrl', 'column',  'reportcolumn', 'columndatatype' ,'output path', 'exception', 'exceptiondetails',
                                        'fail', 'outbucketname', 'row', 'rowcount', 'reporttype' , 'kind' , 'filtermap' , 'airflow_task_id', 'job_id' )
    time.sleep(1)
    tasks = db.get_report_task_entry(client, 'dag3_id', 'run3_id', 'airflow_task_id', 'ParquetWriteTask')
    """print(tasks)"""
    assert len(tasks) == 1
    assert tasks[0].get('status') == 'fail'

 db.handle_report_parquet_write_task(client, 'dag3_id', 'run3_id', 'merge3_id' , 'outputfilename', '2020-07-27',
                                        'wrl', 'column',  'reportcolumn', 'columndatatype' ,'output path', 'exception', 'exceptiondetails',
                                        'success', 'outbucketname', 'row', 'rowcount', 'reporttype' , 'kind' , 'filtermap' , 'airflow_task_id', 'job_id' )
    time.sleep(1)
    tasks = db.get_report_task_entry(client, 'dag3_id', 'run3_id', 'airflow_task_id', 'ParquetWriteTask')
    """print(tasks)"""
    assert len(tasks) == 1
    assert tasks[0].get('status') == 'success'


def test_parquet_write__config_task(db):
    client = get_local_datastore_client(db)
    delete_tasks(client, 'ParquetWriteConfig')
    time.sleep(1)
    tasks = db.get_report_task_entry(client, 'dag4_id', 'run4_id', 'airflow_task_id', 'ParquetWriteConfig)
    print(tasks)
    assert len(tasks) == 0

 db.handle_report_parquet_write_task(client, 'dag4_id', 'run4_id', 'merge4_id' , 'outputfilename', '2020-07-27',
                                        'wrl', 'column',  'reportcolumn', 'columndatatype' ,'output path', 'exception', 'exceptiondetails',
                                        'fail', 'outbucketname', 'row', 'rowcount', 'reporttype' , 'kind' , 'filtermap' , 'airflow_task_id', 'job_id' )
    time.sleep(1)
    tasks = db.get_report_task_entry(client, 'dag4_id', 'run4_id', 'airflow_task_id', 'ParquetWriteConfig)
    """print(tasks)"""
    assert len(tasks) == 1
    assert tasks[0].get('status') == 'fail'

 db.handle_report_parquet_write_task(client, 'dag4_id', 'run4_id', 'merge4_id' , 'outputfilename', '2020-07-27',
                                        'wrl', 'column',  'reportcolumn', 'columndatatype' ,'output path', 'exception', 'exceptiondetails',
                                        'success', 'outbucketname', 'row', 'rowcount', 'reporttype' , 'kind' , 'filtermap' , 'airflow_task_id', 'job_id' )
    time.sleep(1)
    tasks = db.get_report_task_entry(client, 'dag4_id', 'run4_id', 'airflow_task_id', 'ParquetWriteConfig')
    """print(tasks)"""
    assert len(tasks) == 1
    assert tasks[0].get('status') == 'success'


def test_parquet_write_task(db):
    client = get_local_datastore_client(db)
    delete_tasks(client, 'MergeReportConfig')
    time.sleep(1)
    tasks = db.get_report_task_entry(client, 'dag5_id', 'run5_id', 'airflow_task_id', 'MergeReportConfig')
    print(tasks)
    assert len(tasks) == 0

 db.handle_report_parquet_write_task(client, 'dag5_id', 'run5_id', 'merge5_id' , 'outputfilename', '2020-07-27',
                                        'wrl', 'column',  'reportcolumn', 'columndatatype' ,'output path', 'exception', 'exceptiondetails',
                                        'fail', 'outbucketname', 'row', 'rowcount', 'reporttype' , 'kind' , 'filtermap' , 'airflow_task_id', 'job_id' )
    time.sleep(0.2)
    tasks = db.get_report_task_entry(client, 'dag5_id', 'run5_id', 'airflow_task_id', 'MergeReportConfig')
    """print(tasks)"""
    assert len(tasks) == 1
    assert tasks[0].get('status') == 'fail'

 db.handle_report_parquet_write_task(client, 'dag5_id', 'run5_id', 'merge5_id' , 'outputfilename', '2020-07-27',
                                        'wrl', 'column',  'reportcolumn', 'columndatatype' ,'output path', 'exception', 'exceptiondetails',
                                        'success', 'outbucketname', 'row', 'rowcount', 'reporttype' , 'kind' , 'filtermap' , 'airflow_task_id', 'job_id' )
    time.sleep(0.2)
    tasks = db.get_report_task_entry(client, 'dag5_id', 'run5_id', 'airflow_task_id', 'MergeReportConfig')
    """print(tasks)"""
    assert len(tasks) == 1
    assert tasks[0].get('status') == 'success'
    
