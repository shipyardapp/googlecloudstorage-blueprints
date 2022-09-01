import os
import json
import argparse
import re
import tempfile
import sys
import shipyard_utils as shipyard
from google.cloud import storage
from google.cloud.exceptions import *


EXIT_CODE_INVALID_CREDENTIALS = 200
EXIT_CODE_INVALID_BUCKET = 201
EXIT_CODE_FILE_NOT_FOUND = 205


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket-name', dest='bucket_name', required=True)
    parser.add_argument(
        '--source-file-name-match-type',
        dest='source_file_name_match_type',
        default='exact_match',
        choices={
            'exact_match',
            'regex_match'},
        required=False)
    parser.add_argument('--source-folder-name',
                        dest='source_folder_name', default='', required=False)
    parser.add_argument('--source-file-name',
                        dest='source_file_name', required=True)
    parser.add_argument(
        '--service-account',
        dest='gcp_application_credentials',
        default=None,
        required=True)
    return parser.parse_args()


def set_environment_variables(args):
    """
    Set GCP credentials as environment variables if they're provided via keyword
    arguments rather than seeded as environment variables. This will override
    system defaults.
    """
    credentials = args.gcp_application_credentials
    try:
        json_credentials = json.loads(credentials)
        fd, path = tempfile.mkstemp()
        print(f'Storing json credentials temporarily at {path}')
        with os.fdopen(fd, 'w') as tmp:
            tmp.write(credentials)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path
        return path
    except Exception:
        print('Using specified json credentials file')
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials
        return


def find_google_cloud_storage_file_names(bucket, prefix=''):
    """
    Fetched all the files in the bucket which are returned in a list as
    Google Blob objects
    """
    return list(bucket.list_blobs(prefix=prefix))



def get_gclient(args):
    """
    Attempts to create the Google Cloud Storage Client with the associated
    environment variables
    """
    try:
        gclient = storage.Client()
    except Exception:
        print(f'Error accessing Google Cloud Storage with service account '
              f'{args.gcp_application_credentials}')
        sys.exit(EXIT_CODE_INVALID_CREDENTIALS)

    return gclient


def get_bucket(*,
               gclient,
               bucket_name):
    """
    Fetches and returns the bucket from Google Cloud Storage
    """
    try:
        bucket = gclient.get_bucket(bucket_name)
    except NotFound as e:
        print(f'Bucket {bucket_name} does not exist\n {e}')
        sys.exit(EXIT_CODE_INVALID_BUCKET)

    return bucket


def get_storage_blob(bucket, source_folder_name, source_file_name):
    """
    Fetches and returns the single source file blob from the buck on
    Google Cloud Storage
    """
    source_path = source_file_name
    if source_folder_name != '':
        source_path = f'{source_folder_name}/{source_file_name}'
    blob = bucket.get_blob(source_path)
    try:
        blob.exists()
        return blob
    except Exception as e:
        print(f'File {source_path} does not exist')
        sys.exit(EXIT_CODE_FILE_NOT_FOUND)



def delete_google_cloud_storage_file(blob):
    """
    Deletes a selected file from Google cloud storage
    """
    blob_bucket, blob_name = blob.bucket.name, blob.name
    blob.delete()
    print(f"Blob {blob_bucket}/{blob_name} delete ran successfully")


def gcp_find_matching_files(file_blobs, file_name_re):
    """
    Return a list of all file_names that matched the regular expression.
    """
    matching_file_names = []
    for blob in file_blobs:
        if re.search(file_name_re, blob.name):
            matching_file_names.append(blob)

    return matching_file_names


def main():
    args = get_args()
    tmp_file = set_environment_variables(args)
    bucket_name = args.bucket_name
    source_file_name = args.source_file_name
    source_folder_name = shipyard.files.clean_folder_name(args.source_folder_name)
    source_file_name_match_type = args.source_file_name_match_type

    gclient = get_gclient(args)
    bucket = get_bucket(gclient=gclient, bucket_name=bucket_name)

    if source_file_name_match_type == 'regex_match':
        file_names = find_google_cloud_storage_file_names(
            bucket=bucket, prefix=source_folder_name)
        matching_file_names = gcp_find_matching_files(file_names,
                                                  re.compile(source_file_name))
        print(f'{len(matching_file_names)} files found. Preparing to delete...')

        for index, blob in enumerate(matching_file_names):
            print(f'deleting file {index+1} of {len(matching_file_names)}')
            delete_google_cloud_storage_file(blob=blob)
    else:
        blob = get_storage_blob(bucket=bucket,
                                source_folder_name=source_folder_name,
                                source_file_name=source_file_name)
        delete_google_cloud_storage_file(blob=blob)
    if tmp_file:
        print(f'Removing temporary credentials file {tmp_file}')
        os.remove(tmp_file)


if __name__ == '__main__':
    main()
