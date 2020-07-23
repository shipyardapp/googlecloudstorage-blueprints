import os
import re
import json
import tempfile
import argparse

from google.cloud import storage
from google.cloud.exceptions import *


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


def extract_file_name_from_source_full_path(source_full_path):
    """
    Use the file name provided in the source_file_name variable. Should be run only
    if a destination_file_name is not provided.
    """
    destination_file_name = os.path.basename(source_full_path)
    return destination_file_name


def enumerate_destination_file_name(destination_file_name, file_number=1):
    """
    Append a number to the end of the provided destination file name.
    Only used when multiple files are matched to, preventing the destination file from being continuously overwritten.
    """
    if re.search(r'\.', destination_file_name):
        destination_file_name = re.sub(
            r'\.', f'_{file_number}.', destination_file_name, 1)
    else:
        destination_file_name = f'{destination_file_name}_{file_number}'
    return destination_file_name


def determine_destination_file_name(
    *,
    source_full_path,
    destination_file_name,
        file_number=None):
    """
    Determine if the destination_file_name was provided, or should be extracted from the source_file_name,
    or should be enumerated for multiple file downloads.
    """
    if destination_file_name:
        if file_number:
            destination_file_name = enumerate_destination_file_name(
                destination_file_name, file_number)
        else:
            destination_file_name = destination_file_name
    else:
        destination_file_name = extract_file_name_from_source_full_path(
            source_full_path)

    return destination_file_name


def clean_folder_name(folder_name):
    """
    Cleans folders name by removing duplicate '/' as well as leading and trailing '/' characters.
    """
    folder_name = folder_name.strip('/')
    if folder_name != '':
        folder_name = os.path.normpath(folder_name)
    return folder_name


def combine_folder_and_file_name(folder_name, file_name):
    """
    Combine together the provided folder_name and file_name into one path variable.
    """
    combined_name = os.path.normpath(
        f'{folder_name}{"/" if folder_name else ""}{file_name}')
    combined_name = os.path.normpath(combined_name)

    return combined_name


def determine_destination_name(
        destination_folder_name,
        destination_file_name,
        source_full_path,
        file_number=None):
    """
    Determine the final destination name of the file being downloaded.
    """
    destination_file_name = determine_destination_file_name(
        destination_file_name=destination_file_name,
        source_full_path=source_full_path,
        file_number=file_number)
    destination_name = combine_folder_and_file_name(
        destination_folder_name, destination_file_name)
    return destination_name


def find_google_cloud_storage_file_names(bucket, prefix=''):
    """
    Fetched all the files in the bucket which are returned in a list as
    Google Blob objects
    """
    return list(bucket.list_blobs(prefix=prefix))


def find_matching_files(file_blobs, file_name_re):
    """
    Return a list of all file_names that matched the regular expression.
    """
    matching_file_names = []
    for blob in file_blobs:
        if re.search(file_name_re, blob.name):
            matching_file_names.append(blob)

    return matching_file_names


def download_google_cloud_storage_file(blob, destination_file_name=None):
    """
    Download a selected file from Google Cloud Storage to local storage in
    the current working directory.
    """
    local_path = os.path.normpath(f'{os.getcwd()}/{destination_file_name}')

    blob.download_to_filename(local_path)

    print(f'{blob.bucket.name}/{blob.name} successfully downloaded to {local_path}')

    return


def get_gclient():
    """
    Attempts to create the Google Cloud Storage Client with the associated
    environment variables
    """
    try:
        gclient = storage.Client()
    except Exception as e:
        print(f'Error accessing Google Cloud Storage with service account '
              f'{args.gcp_application_credentials}')
        raise(e)

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
        raise(e)

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
        raise(e)


def main():
    args = get_args()
    tmp_file = set_environment_variables(args)
    bucket_name = args.bucket_name
    source_file_name = args.source_file_name
    source_folder_name = clean_folder_name(args.source_folder_name)
    source_full_path = combine_folder_and_file_name(
        folder_name=source_folder_name, file_name=source_file_name)
    source_file_name_match_type = args.source_file_name_match_type

    destination_folder_name = clean_folder_name(args.destination_folder_name)
    if not os.path.exists(destination_folder_name) and \
            (destination_folder_name != ''):
        os.makedirs(destination_folder_name)

    gclient = get_gclient()
    bucket = get_bucket(gclient=gclient, bucket_name=bucket_name)

    if source_file_name_match_type == 'regex_match':
        file_names = find_google_cloud_storage_file_names(
            bucket=bucket, prefix=source_folder_name)
        matching_file_names = find_matching_files(file_names,
                                                  re.compile(source_file_name))
        print(f'{len(matching_file_names)} files found. Preparing to download...')

        for index, blob in enumerate(matching_file_names):
            destination_name = determine_destination_name(
                destination_folder_name=destination_folder_name,
                destination_file_name=args.destination_file_name,
                source_full_path=blob.name, file_number=index + 1)

            print(f'Downloading file {index+1} of {len(matching_file_names)}')
            download_google_cloud_storage_file(
                blob=blob, destination_file_name=destination_name)
    else:
        blob = get_storage_blob(bucket=bucket,
                                source_folder_name=source_folder_name,
                                source_file_name=source_file_name)
        destination_name = determine_destination_name(
            destination_folder_name=destination_folder_name,
            destination_file_name=args.destination_file_name,
            source_full_path=source_full_path)

        download_google_cloud_storage_file(
            blob=blob, destination_file_name=destination_name)
    if tmp_file:
        print(f'Removing temporary credentials file {tmp_file}')
        os.remove(tmp_file)


if __name__ == '__main__':
    main()
