import os
import re
import json
import tempfile
import argparse
import sys
import shipyard_utils as shipyard
from google.cloud import storage
# from google.cloud.exceptions import *
try:
    import exit_codes as ec
except BaseException:
    from . import exit_codes as ec


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source-bucket-name', dest='source_bucket_name', required=True)
    parser.add_argument('--destination-bucket-name', dest='destination_bucket_name', required=True)
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
        '--destination-file-name',
        dest='destination_file_name',
        default=None,
        required=False)
    parser.add_argument(
        '--destination-folder-name',
        dest='destination_folder_name',
        default='',
        required=False)
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


def download_google_cloud_storage_file(blob, destination_file_name=None):
    """
    Download a selected file from Google Cloud Storage to local storage in
    the current working directory.
    """
    local_path = os.path.normpath(f'{os.getcwd()}/{destination_file_name}')

    blob.download_to_filename(local_path)

    print(f'{blob.bucket.name}/{blob.name} successfully downloaded to {local_path}')

    return


def get_gclient(args):
    """
    Attempts to create the Google Cloud Storage Client with the associated
    environment variables
    """
    try:
        gclient = storage.Client()
    except Exception:
        print(f'Error accessing Google Cloud Storage with service account ',
              f'{args.gcp_application_credentials}')
        sys.exit(ec.EXIT_CODE_INVALID_CREDENTIALS)

    return gclient


def get_bucket(*,
               gclient,
               bucket_name):
    """
    Fetches and returns the bucket from Google Cloud Storage
    """
    try:
        bucket = gclient.get_bucket(bucket_name)
    except Exception as e:
        print(f'Bucket {bucket_name} does not exist\n {e}')
        sys.exit(ec.EXIT_CODE_INVALID_BUCKET)

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
        sys.exit(ec.EXIT_CODE_FILE_NOT_FOUND)


def move_google_cloud_storage_file(source_bucket, source_blob_path, 
                                destination_bucket, destination_blob_path):
    """
    Moves blobs between directories or buckets. First copies the  
    blob to destination then deletes the source blob from the old location.
    """
    # get source blob
    source_blob = source_bucket.blob(source_blob_path)

    # copy to destination
    dest_blob = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_path)
    # delete in old destination
    source_blob.delete()
    
    print(f'File moved from {source_blob} to {dest_blob}')


def main():
    args = get_args()
    tmp_file = set_environment_variables(args)
    source_bucket_name = args.source_bucket_name
    destination_bucket_name = args.destination_bucket_name
    source_file_name = args.source_file_name
    source_folder_name = shipyard.files.clean_folder_name(args.source_folder_name)
    source_file_name_match_type = args.source_file_name_match_type

    destination_folder_name = shipyard.files.clean_folder_name(args.destination_folder_name)
    destination_file_name = args.destination_file_name

    gclient = get_gclient(args)
    source_bucket = get_bucket(gclient=gclient, bucket_name=source_bucket_name)
    destination_bucket = get_bucket(gclient=gclient, bucket_name=destination_bucket_name)
    if source_file_name_match_type == 'regex_match':
        try:
            blobs = find_google_cloud_storage_file_names(
                bucket=source_bucket, prefix=source_folder_name)
            file_names = list(map(lambda x: x.name,blobs))
            matching_file_names = shipyard.files.find_all_file_matches(file_names,re.compile(source_file_name))
            
            print(f'{len(matching_file_names)} files found. Preparing to move...')
        except Exception as e:
            print(f"Error in finding regex matches. Please make sure a valid regex is entered")
            sys.exit(ec.EXIT_CODE_FILE_NOT_FOUND)

        for index, blob in enumerate(matching_file_names,1):
            destination_full_path = shipyard.files.determine_destination_full_path(
                destination_folder_name = destination_folder_name,
                destination_file_name = destination_file_name,
                source_full_path = blob,
                file_number= None if len(matching_file_names) == 1 else index
            )
            print(f'moving file {index} of {len(matching_file_names)}')
            move_google_cloud_storage_file(
                source_bucket=source_bucket, source_blob_path=blob,
                destination_bucket=destination_bucket, destination_blob_path=destination_full_path
            )
    else:
        
        blob = get_storage_blob(bucket=source_bucket,
                                source_folder_name=source_folder_name,
                                source_file_name=source_file_name)
        dest_file = shipyard.files.determine_destination_file_name(source_full_path = blob.name, destination_file_name= destination_file_name)
        destination_full_path = shipyard.files.determine_destination_full_path(
            destination_folder_name = destination_folder_name,
            destination_file_name = dest_file,
            source_full_path = blob
        ) 
        move_google_cloud_storage_file(
            source_bucket=source_bucket, source_blob_path=blob.name,
            destination_bucket=destination_bucket, destination_blob_path=destination_full_path
        )
    if tmp_file:
        print(f'Removing temporary credentials file {tmp_file}')
        os.remove(tmp_file)


if __name__ == '__main__':
    main()
