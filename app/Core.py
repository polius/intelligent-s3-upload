import os
from S3 import S3
from S3Upload import S3Upload
from S3MultipartUpload import S3MultipartUpload

class Core:
    def __init__(self, credentials, path):
        # Init Variables
        self._path = path
        # Init Classes
        self._s3 = S3(credentials)
        self._s3u = S3Upload(self._s3, path)
        self._s3mpu = S3MultipartUpload(self._s3, path)

    def upload(self):
        print("- Scanning files...")
        # Check if the resource exists
        if not os.path.exists(self._path):
           raise Exception("- The provided path '{}' does not exist".format(self._path))

        # Get all resources
        resources = self.__get_resources(self._path)

        # Start the upload process
        if len(resources) == 0:
            print("- There's no resources to upload.")
        else:
            print("- Starting the upload process...")
            total_files = total_bytes = 0
            try:
                for i, item in enumerate(resources):
                    # Track progress
                    progress = {"i": i+1, "n": len(resources)}

                    # Upload file
                    if item['size'] <= (5 * 1024**2):
                        # S3 Upload if file is 5MB or less 
                        file_uploaded = self._s3u.upload(item, progress)
                    else:
                        # S3 Multipart Upload
                        file_uploaded = self._s3mpu.upload(item, progress)

                    # Track total_bytes
                    if file_uploaded:
                        total_files += 1
                        total_bytes += item['size']

            except KeyboardInterrupt:
                raise KeyboardInterrupt(self.__parse_size(total_bytes))
            else:
                print("- Upload process finished successfully.")
                print("- Total files uploaded: {}".format(total_files))
                print("- Total bytes uploaded: {}".format(self.__parse_size(total_bytes)))

    ###################
    # PRIVATE METHODS #
    ###################
    def __get_resources(self, path, prefix=''):
        if os.path.isfile(path):
            size = os.stat(path).st_size
            return [{'path': path, 'size': size, 'size_parsed': self.__parse_size(size), 'key': os.path.join(self._s3.bucket_prefix, prefix, os.path.basename(path))}]
        resources = list()
        for item in os.listdir(path):
            resources += self.__get_resources(os.path.join(path, item), os.path.join(prefix, os.path.basename(path)))
        return resources

    def __parse_size(self, size):
        if size < 1024:
            return "{:.2f} B".format(size)
        elif size < 1024**2:
            return "{:.2f} KB".format(size / 1024)
        elif size < 1024**3:
            return "{:.2f} MB".format(size / 1024**2)
        elif size < 1024**4:
            return "{:.2f} GB".format(size / 1024**3)
        elif size < 1024**5:
            return "{:.2f} TB".format(size / 1024**4)
        elif size < 1024**6:
            return "{:.2f} PB".format(size / 1024**5)
        elif size < 1024**7:
            return "{:.2f} EB".format(size / 1024**6)
