import sys
import threading
from time import sleep

class S3Upload:
    def __init__(self, s3, path):
        self._path = path
        self._s3 = s3

    def upload(self, item, progress):
        # Check if the current file already exists in the bucket
        if self._s3.skip_existing_files and self._s3.check_s3_key_exists(item):
            overall_progress = progress['i'] / progress['n'] * 100
            print("[{:.2f}%] [{}] {}. File currently exists in S3.".format(overall_progress, item['size_parsed'], item['key']))
            return False
        else:
            self.__upload_file(item, progress)
            return True

    ###################
    # PRIVATE METHODS #
    ###################
    def __upload_file(self, item, progress):
        exception = None
        for i in range(self._s3.retry_attempts):
            try:
                extra_args = {'StorageClass': self._s3.storage_class}
                if self._s3.server_side_encryption:
                    extra_args['ServerSideEncryption'] = 'AES256'
                self._s3.connection().upload_file(Filename=item['path'], Bucket=self._s3.bucket_name, Key=item['key'], Callback=ProgressBar(item, progress), ExtraArgs=extra_args)
                return
            except Exception as e:
                exception = str(e)
                print("[Attempt {}/{}] Upload process failed. Retrying in 3 seconds...".format(i+1, self._s3.retry_attempts))
                sleep(3)
        raise Exception("- Upload process failed after {} attempts.\nError: {}".format(self._s3.retry_attempts, exception))


class ProgressBar:
    def __init__(self, item, progress):
        self._item = item
        self._progress = progress
        self._bytes_processed = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._bytes_processed += bytes_amount
            percentage = float(self._bytes_processed) / float(self._item['size']) * 100
            overall_progress = (float(self._progress['i']-1) / float(self._progress['n']) * 100) + (percentage / float(self._progress['n']))
            sys.stdout.write("\r[{:.2f}%] [{}] {} ({:.2f}%)".format(overall_progress, self._item['size_parsed'], self._item['key'], percentage))
            
            if percentage == 100:
                sys.stdout.write('\n')
            sys.stdout.flush()
