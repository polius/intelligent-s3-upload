#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
from time import sleep
from S3 import S3

class S3MultipartUpload:
    def __init__(self, credentials, path):
        # Init AWS Variables
        self._path = path

        # Init S3 Class
        self._s3 = S3(credentials)   

    def upload(self, item, progress):
        # Init Variables
        self._file_path = item['file_path']
        self._key = {
            'name': self._s3.bucket_prefix + item['file_path'][len(self._path):], 
            'size': item['file_size'], 
            'size_parsed': item['file_size_parsed']
        }
        self._progress = progress

        # Check if the current file already exists in the bucket
        if self._s3.skip_existing_files is True and self._s3.check_s3_key_exists(self._key):
            overall_progress = self._progress['i'] / self._progress['n'] * 100
            print("[{:.2f}%] [{}] {}. File currently exists in S3.".format(overall_progress, self._key['size_parsed'], self._key['name']))
            return False
        else:
            # Abort uncompleted multipart uploads
            self.__abort()
            # Create new multipart upload
            mpu_id = self.__create()
            # Upload parts
            parts = self.__upload(mpu_id)
            # Complete multipart upload
            self.__complete(mpu_id, parts)
            return True

    ###################
    # PRIVATE METHODS #
    ###################
    def __abort(self):
        exception = None
        for i in range(self._s3.retry_attempts):
            try:
                mpus = self._s3.connection().list_multipart_uploads(Bucket=self._s3.bucket_name)
                aborted = []
                if "Uploads" in mpus:
                    for u in mpus["Uploads"]:
                        aborted.append(self._s3.connection().abort_multipart_upload(Bucket=self._s3.bucket_name, Key=u["Key"], UploadId=u["UploadId"]))
                return aborted
            except Exception as e:
                exception = str(e)
                print("[Attempt {}/{}] Multipart abort process failed. Retrying in 5 seconds...".format(i+1, self._s3.retry_attempts))
                sleep(5)
        raise Exception("- Multipart abort process failed with key '{}' after {} attempts.\nError: {}".format(self._key['name'], self._s3.retry_attempts, exception))

    def __create(self):
        exception = None
        for i in range(self._s3.retry_attempts):
            try:
                mpu = self._s3.connection().create_multipart_upload(Bucket=self._s3.bucket_name, Key=self._key['name'], StorageClass=self._s3.storage_class)
                mpu_id = mpu["UploadId"]
                return mpu_id
            except Exception as e:
                exception = str(e)
                print("[Attempt {}/{}] Multipart create process failed. Retrying in 5 seconds...".format(i+1, self._s3.retry_attempts))
                sleep(5)
        raise Exception("- Multipart create process failed with key '{}' after {} attempts.\nError: {}".format(self._key['name'], self._s3.retry_attempts, exception))

    def __upload(self, mpu_id):
        exception = None
        part_size = int(self._key['size'] / (10000 * 1024**2)) + (self._key['size'] % (10000 * 1024**2) > 0)
        part_size = 5 * 1024**2 if part_size < 5 else part_size * 1024**2
        total_parts = int(self._key['size'] / part_size) + (self._key['size'] % part_size > 0)
        parts = []
        part_number = 1
        uploaded_bytes = 0
        with open(self._file_path, "rb") as f:
            while True:
                data = f.read(part_size)
                uploaded_bytes += len(data)
                for r in range(self._s3.retry_attempts):
                    try:
                        if not len(data):
                            return parts

                        # Show Status Message
                        part_progress = float(uploaded_bytes) / float(self._key['size']) * 100
                        overall_progress = (float(self._progress['i']-1) / float(self._progress['n']) * 100) + (part_progress / self._progress['n'])
                        sys.stdout.write("\r[{:.2f}%] [{}] {} ({:.2f}%) [Part {}/{}]".format(overall_progress, self._key['size_parsed'], self._key['name'], part_progress, part_number, total_parts))
                        if part_progress == 100:
                            sys.stdout.write('\n')
                        sys.stdout.flush()

                        # Upload the part
                        part = self._s3.connection().upload_part(Body=data, Bucket=self._s3.bucket_name, Key=self._key['name'], UploadId=mpu_id, PartNumber=part_number)
                        parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                        part_number += 1
                        break

                    except Exception as e:
                        exception = str(e)
                        print("\n[Attempt {}/{}] Multipart upload process failed. Retrying in 5 seconds...".format(r+1, self._s3.retry_attempts))
                        sleep(5)
                        if r == self._s3.retry_attempts - 1:
                            raise Exception("- Multipart upload process failed after {} attempts.\nError: {}".format(self._s3.retry_attempts, exception))

    def __complete(self, mpu_id, parts):
        exception = None
        for i in range(self._s3.retry_attempts):
            try:
                result = self._s3.connection().complete_multipart_upload(Bucket=self._s3.bucket_name, Key=self._key['name'], UploadId=mpu_id, MultipartUpload={"Parts": parts})
                return result
            except Exception as e:
                exception = str(e)
                print("[Attempt {}/{}] Multipart complete process failed. Retrying in 5 seconds...".format(i+1, self._s3.retry_attempts))
                sleep(5)
        raise Exception("- Multipart complete process failed with key '{}' after {} attempts.\nError:{}".format(self._s3.key['name'], self._s3.retry_attempts, exception))
