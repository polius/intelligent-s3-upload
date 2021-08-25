import sys
from time import sleep

class S3MultipartUpload:
    def __init__(self, s3, path):
        self._path = path
        self._s3 = s3

    def upload(self, item, progress):
        # Check if the current file already exists in the bucket
        if self._s3.skip_existing_files is True and self._s3.check_s3_key_exists(item):
            overall_progress = progress['i'] / progress['n'] * 100
            print("[{:.2f}%] [{}] {}. File currently exists in S3.".format(overall_progress, item['size_parsed'], item['key']))
            return False
        else:
            # Abort uncompleted multipart uploads
            self.__abort(item)
            # Create new multipart upload
            mpu_id = self.__create(item)
            # Upload parts
            parts = self.__upload(item, progress, mpu_id)
            # Complete multipart upload
            self.__complete(item, mpu_id, parts)
            return True

    ###################
    # PRIVATE METHODS #
    ###################
    def __abort(self, item):
        exception = None
        for i in range(self._s3.retry_attempts):
            try:
                mpus = self._s3.connection().list_multipart_uploads(Bucket=self._s3.bucket_name)
                if "Uploads" in mpus:
                    for u in mpus["Uploads"]:
                        self._s3.connection().abort_multipart_upload(Bucket=self._s3.bucket_name, Key=u["Key"], UploadId=u["UploadId"])
                    if len(mpus["Uploads"]) == 1000:
                        self.__abort(item)
            except Exception as e:
                exception = str(e)
                print("[Attempt {}/{}] Multipart abort process failed. Retrying in 3 seconds...".format(i+1, self._s3.retry_attempts))
                sleep(3)
        if exception:
            raise Exception("- Multipart abort process failed with key '{}' after {} attempts.\nError: {}".format(item['key'], self._s3.retry_attempts, exception))

    def __create(self, item):
        exception = None
        for i in range(self._s3.retry_attempts):
            try:
                if self._s3.server_side_encryption:
                    mpu = self._s3.connection().create_multipart_upload(Bucket=self._s3.bucket_name, Key=item['key'], StorageClass=self._s3.storage_class, ServerSideEncryption='AES256')
                else:
                    mpu = self._s3.connection().create_multipart_upload(Bucket=self._s3.bucket_name, Key=item['key'], StorageClass=self._s3.storage_class)
                mpu_id = mpu["UploadId"]
                return mpu_id
            except Exception as e:
                exception = str(e)
                print("[Attempt {}/{}] Multipart create process failed. Retrying in 3 seconds...".format(i+1, self._s3.retry_attempts))
                sleep(3)
        raise Exception("- Multipart create process failed with key '{}' after {} attempts.\nError: {}".format(item['key'], self._s3.retry_attempts, exception))

    def __upload(self, item, progress, mpu_id):
        exception = None
        part_size = int(item['size'] / (10000 * 1024**2)) + (item['size'] % (10000 * 1024**2) > 0)
        part_size = 5 * 1024**2 if part_size < 5 else part_size * 1024**2
        total_parts = int(item['size'] / part_size) + (item['size'] % part_size > 0)
        parts = []
        part_number = 1
        uploaded_bytes = 0
        with open(item['path'], "rb") as f:
            while True:
                data = f.read(part_size)
                uploaded_bytes += len(data)
                for r in range(self._s3.retry_attempts):
                    try:
                        if not len(data):
                            return parts

                        # Show Status Message
                        part_progress = float(uploaded_bytes) / float(item['size']) * 100
                        overall_progress = (float(progress['i']-1) / float(progress['n']) * 100) + (part_progress / progress['n'])
                        sys.stdout.write("\r[{:.2f}%] [{}] {} ({:.2f}%) [Part {}/{}]".format(overall_progress, item['size_parsed'], item['key'], part_progress, part_number, total_parts))
                        if part_progress == 100:
                            sys.stdout.write('\n')
                        sys.stdout.flush()

                        # Upload the part
                        part = self._s3.connection().upload_part(Body=data, Bucket=self._s3.bucket_name, Key=item['key'], UploadId=mpu_id, PartNumber=part_number)
                        parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                        part_number += 1
                        break

                    except Exception as e:
                        exception = str(e)
                        print("\n[Attempt {}/{}] Multipart upload process failed. Retrying in 3 seconds...".format(r+1, self._s3.retry_attempts))
                        sleep(3)
                        if r == self._s3.retry_attempts - 1:
                            raise Exception("- Multipart upload process failed after {} attempts.\nError: {}".format(self._s3.retry_attempts, exception))

    def __complete(self, item, mpu_id, parts):
        exception = None
        for i in range(self._s3.retry_attempts):
            try:
                result = self._s3.connection().complete_multipart_upload(Bucket=self._s3.bucket_name, Key=item['key'], UploadId=mpu_id, MultipartUpload={"Parts": parts})
                return result
            except Exception as e:
                exception = str(e)
                print("[Attempt {}/{}] Multipart complete process failed. Retrying in 3 seconds...".format(i+1, self._s3.retry_attempts))
                sleep(3)
        raise Exception("- Multipart complete process failed with key '{}' after {} attempts.\nError:{}".format(self._s3.key['name'], self._s3.retry_attempts, exception))
