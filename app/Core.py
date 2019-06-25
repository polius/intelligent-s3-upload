#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from S3Upload import S3Upload
from S3MultipartUpload import S3MultipartUpload

class Core:
    def __init__(self, credentials, path):
        # Init Variables
        self._path = path
        # Init Amazon S3 Classes
        self._s3u = S3Upload(credentials, path)
        self._s3mpu = S3MultipartUpload(credentials, path)

    def upload(self):
        print("- Scanning files...")

        # Check if the resource exists
        if not (os.path.exists(self._path)):
            raise Exception("- The provided path '{}' does not exist".format(self._path))

        # Get all resources
        if os.path.isfile(self._path):
            file_size = os.stat(self._path).st_size
            resources = [{'file_path': self._path, 'file_size': file_size, 'file_size_parsed': self.__parse_file_size(file_size)}]
        else:
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
                    if item['file_size'] <= (5 * 1024**2):
                        # S3 Upload if file is 5MB or less 
                        file_uploaded = self._s3u.upload(item, progress)
                    else:
                        # S3 Multipart Upload
                        file_uploaded = self._s3mpu.upload(item, progress)

                    # Track total_bytes
                    if file_uploaded:
                        total_files += 1
                        total_bytes += item['file_size']

            except KeyboardInterrupt:
                raise KeyboardInterrupt(self.__parse_file_size(total_bytes))
            else:
                print("- Upload process finished successfully.")
                print("- Total files uploaded: {}".format(total_files))
                print("- Total bytes uploaded: {}".format(self.__parse_file_size(total_bytes)))

    ###################
    # PRIVATE METHODS #
    ###################
    def __get_resources(self, path):
        resources = list()
        # Iterate over all the directories
        for item in os.listdir(path):
            # Get item full path
            item = os.path.join(path, item)
            # If entry is a directory then get the list of files in this directory 
            if os.path.isdir(item):
                resources += self.__get_resources(item)
            else:
                item_size = os.stat(item).st_size
                resources.append({'file_path': item, 'file_size': item_size, 'file_size_parsed': self.__parse_file_size(item_size)})
        return resources

    def __parse_file_size(self, file_size):
        if file_size < 1024:
            return "{:.2f} B".format(file_size)
        elif file_size < 1024**2:
            return "{:.2f} KB".format(file_size / 1024)
        elif file_size < 1024**3:
            return "{:.2f} MB".format(file_size / 1024**2)
        elif file_size < 1024**4:
            return "{:.2f} GB".format(file_size / 1024**3)
        elif file_size < 1024**5:
            return "{:.2f} TB".format(file_size / 1024**4)
