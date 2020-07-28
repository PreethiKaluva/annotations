"""
this file contain logic to
download / upload files from gcs
"""
import json
import os
from google.cloud import storage
from google.cloud import datastore
from parquet_write_automation.cloud.scripts.main.cloud import Cloud


class StorageCredentialNotFound(Exception):
    """
    Storage credentials not found for gcs
    """
    pass


def get_secrets(path_:str)-> dict:
    """
    get secrets from vault mounted json file
    :param path_: path to credentails file
    :return:dict_
    """
    default_key = ['SA']
    default_key.sort()
    try:
        if not os.path.exists(path_):
            raise StorageCredentialNotFound("No credential file "
                                            "found in path {}".format(path_))

        with open(path_) as fp:
            dict_ = json.load(fp)
        keys = list(dict_.keys())
        keys.sort()
        if keys != default_key:
            raise StorageCredentialNotFound("Needed credentials keys: {}"
                                            " but found keys: {}".format(default_key,
                                                                         keys))
        return dict_

    except json.JSONDecodeError:
        raise StorageCredentialNotFound("Invalid json format"
                                        " for credential")
    except Exception as e:
        raise e



class Gcs(Cloud):
    """
    class to handle all gcs related operation
    """

    def __init__(self):
        self.client = self.gcs_auth_client()


 PATH = "/vault/secrets/gcp_credentials.json"
        try:
            cred = get_secrets(PATH)
            key = cred["SA"]
        except KeyError as ke:
            raise StorageCredentialNotFound("Storage credentials not"
                                            " mounted for gcs ")
        gcs_sa = json.loads(key)
        with open('gcs-sa.json', 'w') as json_file:
            json.dump(gcs_sa, json_file)


    @staticmethod
    def gcs_auth_client()->google.cloud.storage.Client:
        """
        google.cloud.storage.client.Client
        this function creates and return gcs client
        :return: gcs client
        """       
        return storage.Client.from_service_account_json('gcs-sa.json')

    @staticmethod
    def gcs_data_store_client()->google.cloud.datastore.Client:
        """
        this function creates and return gcs client
        :return: gcs client
        """      
        return datastore.Client.from_service_account_json('gcs-sa.json')

    def download_file(self, cloud_path:str, bucket_name:str, destination:str=None)->str:
        """
        this function downloads file from gcs and put it into destination
        mention in function called , if not mentioned it will create a
        sub directory downloads inside current working directory and put
        it there. Also it return the path where downloaded files resides
        :param cloud_path: (str) gcs path for file to download
        :param bucket_name: (str) gcs bucket name
        :param destination: (str) destination where file will be downloaded
        :return: (str) path where downloaded file will reside
        """
        blob=get_blob(bucket_name,cloud_path)
        if not destination:
            destination = os.path.join(os.path.dirname(os.path.abspath()), 'download')
        file_name = cloud_path.split('/')[-1]
        destination_file_path = os.path.join(destination, file_name)
        blob.download_to_filename(destination_file_path)
        return destination_file_path

    def upload_file(self, file_path:str, bucket_name:str, cloud_path:str):
        """
        this function will upload file to gcs path
        :param file_path: (str) local file path
        :param bucket_name: (str) gcs bucket name
        :param cloud_path: (str) gcs path where file will be uploaded
        :return: None
        """
        blob=get_blob(bucket_name,cloud_path)
        blob.upload_from_filename(file_path)

    def get_blob(bucket_name:str,cloud_path:str)->google.cloud.storage.blob.Blob:
        """
        this function creates a blob for a given bucket_name and cloud_path
        :param bucket_name: (str) gcs bucket name
        :param cloud_path: (str) gcs path where file will be uploaded
        :return: blob
        """
        client = self.client
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(cloud_path)
        return blob
        

    def download_files(self, cloud_path:str, bucket_name:str, destination:str)->list:
        """
        this function download list of files mention in directory structure
        in gcs (apart from bucket) example : gs://bucket_name/A/B/C/1.txt ,
        gs://bucket_name/A/B/C/2.txt then cloud_path wil be A/B/C and it will
        download 1.txt and 2.txt
        :param cloud_path: path of directory structure inside a bucket
        :param bucket_name: bucket name
        :param destination: local path where file will be store
        :return: List[str] list of downloaded file  local path
        """
        client = self.client
        blobs = client.list_blobs(bucket_or_name=bucket_name, prefix=cloud_path, delimiter='/')
        if not destination:
            destination = os.path.join(os.path.dirname(os.path.abspath()), 'download')
        downloaded_files = []
        for blob in blobs:

            if not blob.name.rstrip().endswith("/"):
                file_name = blob.name.replace('/', '_')
                destination_file_path = os.path.join(destination, file_name)
                blob.download_to_filename(destination_file_path)
                downloaded_files.append(destination_file_path)
        return downloaded_files
