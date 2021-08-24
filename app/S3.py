import boto3
from botocore.client import ClientError

class S3:
    def __init__(self, credentials):
        # Init Variables
        self.storage_class = credentials['storage_class']
        self.bucket_name = credentials['bucket_name']
        if credentials['bucket_path'] not in ['', '/']: 
            self.bucket_prefix = credentials['bucket_path'] if credentials['bucket_path'].endswith('/') else credentials['bucket_path'] + '/'
        self.bucket_prefix = self.bucket_prefix if not self.bucket_prefix.startswith('/') else self.bucket_prefix[1:]
        self.skip_existing_files = credentials['skip_s3_existing_files']
        self.server_side_encryption = credentials['server_side_encryption']
        self.retry_attempts = 5

        # Init S3 Connection
        session = boto3.Session(
            aws_access_key_id=credentials['aws_access_key_id'],
            aws_secret_access_key=credentials['aws_secret_access_key'],
            region_name=credentials['region_name']
        )
        self._s3 = session.resource('s3').meta.client

        # Check S3 Connection
        self.__check_s3_connection()

    def connection(self):
        # Return S3 Connection
        return self._s3

    def check_s3_key_exists(self, item):
        response = self._s3.list_objects_v2(Bucket=self.bucket_name, Prefix=item['key'])

        for obj in response.get('Contents', []):
            if obj['Key'] == item['key'] and obj['Size'] == item['size']:
                return True
        return False

    ###################
    # PRIVATE METHODS #
    ###################
    def __check_s3_connection(self):
        try:
            self._s3.head_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            raise Exception("- The bucket '{}' does not exist or you have no access.".format(self.bucket_name))
