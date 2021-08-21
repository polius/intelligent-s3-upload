import os
import sys
import json
import argparse
from time import time
from datetime import timedelta
from Core import Core

def cls():
    os.system('cls' if os.name == 'nt' else 'clear')

def parse_args():
    parser = argparse.ArgumentParser(description='Intelligent S3 Upload')
    parser.add_argument('--path', required=True, help="The absolute file path (can be either a folder or a file) to upload to Amazon S3")
    args = parser.parse_args()
    args.path = os.path.normpath(args.path)
    return args

def load_credentials():
    with open('credentials.json') as file_name:
        return json.load(file_name)

def validate_credentials(credentials):
    if len(credentials['aws_access_key_id']) == 0:
        raise Exception("The 'aws_access_key_id' cannot be empty")
    if len(credentials['aws_secret_access_key']) == 0:
        raise Exception("The 'aws_secret_access_key' cannot be empty")
    if len(credentials['region_name']) == 0:
        raise Exception("The 'region_name' cannot be empty")
    if len(credentials['bucket_name']) == 0:
        raise Exception("The 'bucket_name' cannot be empty")
    if len(credentials['storage_class']) == 0:
        raise Exception("The 'storage_class' cannot be empty.\nAllowed values: 'STANDARD'|'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'GLACIER'|'DEEP_ARCHIVE'")
    if credentials['skip_s3_existing_files'] not in [True, False]:
        raise Exception("The 'skip_existing_files' should be 'true' or 'false'")

def main():
    # Clear Screen
    cls()
    print("+==================================================================+")
    print("‖  Intelligent S3 Upload                                           ‖")
    print("+==================================================================+")

    # Check Python Version
    if sys.version_info[0] < 3:
        print("Python 3 required to run this application")
        sys.exit()

    # Parse Arguments
    args = parse_args()

    try:
        # Store the Start Time
        start_time = time()

        # Load & Validate Credentials
        credentials = load_credentials()
        validate_credentials(credentials)

        # Init Core Class
        core = Core(credentials, args.path)  

        # Start Uploading Process
        core.upload()

    except Exception as e:
        print(e)
    except KeyboardInterrupt as e:
        print("\n- Upload process interrupted.")
        if len(str(e)) != 0:
            print("- Total bytes uploaded: {}".format(e))
    finally:
        overall_time = str(timedelta(seconds=time() - start_time))
        print("- Overall Time: {}".format(overall_time))


if __name__ == "__main__":
    main()
