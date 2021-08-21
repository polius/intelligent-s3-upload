import os
import sys
import threading
from time import sleep
from S3 import S3

class S3Upload:
    def __init__(self, credentials, path):
        # Init AWS Variables
        self._path = path

        # Init S3 Class
        self._s3 = S3(credentials)

    def upload(self, item, progress):
        # Init Variables
        self._file_path = item['file_path']
        self._key = {
            'name': self._s3.bucket_prefix + os.path.basename(self._file_path), 
            'size': item['file_size'], 
            'size_parsed': item['file_size_parsed']
        }
        self._progress = progress

        # Check if the current file already exists in the bucket
        if self._s3.skip_existing_files is True and self._s3.check_s3_key_exists(self._key):
            overall_progress = progress['i'] / progress['n'] * 100
            print("[{:.2f}%] [{}] {}. File currently exists in S3.".format(overall_progress, self._key['size_parsed'], self._key['name']))
            return False
        else:
            self.__upload_file()
            return True

    ###################
    # PRIVATE METHODS #
    ###################
    def __upload_file(self):
        exception = None
        for i in range(self._s3.retry_attempts):
            try:
                extra_args = {'StorageClass': self._s3.storage_class}
                if self._s3.server_side_encryption:
                    extra_args['ServerSideEncryption'] = 'AES256'
                self._s3.connection().upload_file(Filename=self._file_path, Bucket=self._s3.bucket_name, Key=(self._key['name']), Callback=ProgressBar(self._key, self._progress), ExtraArgs=extra_args)
                return
            except Exception as e:
                exception = str(e)
                print("[Attempt {}/{}] Upload process failed. Retrying in 5 seconds...".format(i+1, self._s3.retry_attempts))
                sleep(5)
        raise Exception("- Upload process failed after {} attempts.\nError: {}".format(self._s3.retry_attempts, exception))


class ProgressBar:
    def __init__(self, key, progress):
        self._key = key
        self._progress = progress
        self._bytes_processed = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._bytes_processed += bytes_amount
            percentage = float(self._bytes_processed) / float(self._key['size']) * 100
            overall_progress = (float(self._progress['i']-1) / float(self._progress['n']) * 100) + (percentage / float(self._progress['n']))
            sys.stdout.write("\r[{:.2f}%] [{}] {} ({:.2f}%)".format(overall_progress, self._key['size_parsed'], self._key['name'], percentage))
            
            if percentage == 100:
                sys.stdout.write('\n')
            sys.stdout.flush()
