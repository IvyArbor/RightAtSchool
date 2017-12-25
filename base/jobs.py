from base.auditing import Auditable
from base.databases import DB
from abc import ABC, abstractmethod
import pygrametl
from pygrametl.datasources import SQLSource, CSVSource

class Job(Auditable):
    def __init__(self, conf, args = [], user_name=''):
        self.conf = conf
        self.args = args
        self.user_name = user_name
        self.verbose = "-v" in self.args
        self.debug = "-d" in self.args
        DB.configure(self.conf)

    def run(self):
        self.file_name = True
        self.configure()
        if not self.file_name: return
        self.configureAudit()

        if self.verbose:
            print('Running job:', self.__class__.__name__)
            # print('Configuration:', self.conf)
            print('Arguments:', self.args)

        try:

            self.beforeJobAudit()

            counts = {
            'extract': 0,
            'insert': 0,
            'update': 0,
            'error': 0,
            }

            # Wrap the connection to use by pygrametl
            self.target_connection_wrap = pygrametl.ConnectionWrapper(connection=self.target_connection)
            source = self.getSource()
            target = self.getTarget()

            for row in source:
                # Catch any incompatible data warnings
                counts['extract'] += 1
                try:
                    prepared = self.prepareRow(row)

                    if self.verbose:
                        print('Inserting row:', prepared)

                    self.insertRow(target, prepared)
                    is_insert = True # where to get this info
                    if is_insert:
                        counts['insert'] += 1
                    else:
                        counts['update'] += 1

                    if self.debug:
                        # Commit in debug mode so we can see inserted rows
                        self.target_connection_wrap.commit()
                        input('Row inserted successfully. Press Enter to continue...')
                except Exception as e:
                    counts['error'] += 1 # Should we log smth here
                    print(e)
                    self.logWarning(row, counts['extract'])
                    if self.verbose or self.debug:
                        print('Row could not be inserted due to an error.', row)

                    if self.debug:
                        input('Press Enter to continue...')

            self.afterJobAudit(counts)
            if self.verbose:
                print("Commit the target database")
            self.target_connection.commit()
            self.target_connection.close()

        except Exception as e:
            self.logError(e)

        self.close()

    def getDatabaseConnection(self, name):
        return DB.connect(name)

    @abstractmethod
    def configure(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def getSource(self):
        pass

    @abstractmethod
    def getTarget(self):
        pass

    def prepareRow(self, row):
        return row

    def insertRow(self, target, row):
        target.insert(row)


class SQLJob(Job):
    def getSource(self):
        self.source_connection = self.getDatabaseConnection(self.source_database)
        query = self.getSqlQuery()
        name_mapping = self.getColumnMapping()
        source = SQLSource(connection=self.source_connection, query=query, names=name_mapping)
        return source

    def close(self):
        """Release database resource"""
        self.source_connection.close()

    @abstractmethod
    def getSqlQuery(self):
        pass

    @abstractmethod
    def getColumnMapping(self):
        pass


import boto3
from fnmatch import fnmatch
class FileJob(Job):
    def pick_file_to_process(self, folder = None, pattern = None):
        self.bucket_name = self.conf['s3']['to-process']['bucket']
        self.archive_bucket_name = self.conf['s3']['archive']['bucket']
        self.bucket_folder = folder
        files = self.list_files(pattern)

        self.source_database = 'S3'
        if len(files) == 0:
            self.file_name = None
            self.source_table = ''
        else:
            self.file_name = files[0]
            self.source_table = self.file_name

        if self.verbose:
            print("Files found on S3:", files)
            if self.file_name:
                print("File to be processed:", self.file_name)
            else:
                print("No file to process!!!")

        return self.file_name

    def list_files(self, pattern = None):
        prefix = self.bucket_folder + '/' if self.bucket_folder else ''
        s3r = boto3.resource('s3')
        bucket = s3r.Bucket(name=self.bucket_name)
        files = [o.key for o in bucket.objects.filter(Prefix=prefix) if o.key != prefix]
        if pattern:
            files = [f for f in files if fnmatch(f, prefix + pattern)]
        return files

    def archive_file(self):
        if self.conf['s3']['archive']['enabled']:
            s3r = boto3.resource('s3')
            copy_bucket = s3r.Bucket(name=self.bucket_name)
            paste_bucket = s3r.Bucket(name=self.archive_bucket_name)
            paste_bucket.copy({
                'Bucket': self.bucket_name,
                'Key': self.file_name
            }, self.file_name)
            object_to_delete = s3r.Object(self.bucket_name, self.file_name)
            object_to_delete.delete()


from readers.streamreader import S3StreamReader
from readers.textreader import TextReader
class TextJob(FileJob):
    def getStream(self, bucket_name, file_name):
        stream = S3StreamReader(bucket_name, file_name)
        return stream

    def getSource(self):
        if not self.file_name: return []

        stream = self.getStream(self.bucket_name, self.file_name)
        self.column_mapping = self.getColumnMapping()
        reader = TextReader(stream, self.column_mapping)
        # Get a reader of S3
        # Get a text parser using the column specs
        # Return an iterator of rows
        return reader.rows()

    def close(self):
        """Here we should archive the file instead"""
        self.archive_file()

from readers.streamreader import LocalStreamReader
from readers.excelreader import ExcelReader
from readers.s3reader import S3Reader
class ExcelJob(FileJob):
    def getSource(self):
        if not self.file_name: return []
        reader = LocalStreamReader(self.file_name)
        #reader = S3Reader(self.bucket_name, self.file_name)
        self.column_mapping = self.getColumnMapping()
        source = ExcelReader(reader, self.sheet_name, self.column_mapping, self.first_data_row)
# self.reader = ExcelReader(self.file_name, self.sheet_name, self.column_mapping, self.first_data_row)
        # Get a reader of S3
        # Get a text parser using the column specs
        # Return an iterator of rows
        return source.rows()

    # @abstractmethod
    # def getColumnMapping(self):
    #     pass

    def close(self):
        self.reader.close()
        self.archive_file()


from readers.streamreader import LocalStreamReader
from readers.xlsreader import XlsReader
from readers.s3reader import S3Reader
class XlsJob(FileJob):
    def getSource(self):
        if not self.file_name: return []
        reader = LocalStreamReader(self.file_name)
        #reader = S3Reader(self.bucket_name, self.file_name)
        self.column_mapping = self.getColumnMapping()
        source = XlsReader(reader, self.sheet_name, self.column_mapping, self.first_data_row)
        # self.reader = ExcelReader(self.file_name, self.sheet_name, self.column_mapping, self.first_data_row)
        # Get a reader of S3
        # Get a text parser using the column specs
        # Return an iterator of rows
        return source.rows()

    # @abstractmethod
    # def getColumnMapping(self):
    #     pass

    def close(self):
        self.reader.close()
        self.archive_file()

from readers.streamreader import LocalStreamReader
from readers.csvreader import CSVReader
class CSVJob(FileJob):
    def getSource(self):
        if not self.file_name:
            return []

        #stream = self.getStream()
        stream = LocalStreamReader(self.file_name)
        self.column_mapping = self.getColumnMapping()

        self.reader = CSVReader(
            stream,
            column_mapping=self.column_mapping,
            delimiter=self.delimiter,
            quotechar=self.quotechar,
            ignore_firstline=self.ignore_firstline)

        return self.reader.rows()

    def close(self):
        self.reader.close()
        #self.archive_file()


from readers.streamreader import SFTPStreamReader
import paramiko
class SFTCSVJob(FileJob):
    def getSource(self):
        if not self.file_path: return []

        sftp = "sftp"
        if self.sftp == "ATS": sftp = "sftp_ats"

        reader = SFTPStreamReader(self.file_path,
                                  self.conf[sftp]["hostname"],
                                  22,
                                  self.conf[sftp]["username"],
                                  self.conf[sftp]["password"])

        self.column_mapping = self.getColumnMapping()
        source = CSVReader(reader, column_mapping=self.column_mapping, delimiter=self.delimiter, quotechar=self.quotechar)

        return source.rows()


    def archive_file(self):
        sftp = "sftp"
        if self.sftp == "ATS": sftp = "sftp_ats"

        # Using paramiko to read the file
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh.connect(self.conf[sftp]["hostname"], username=self.conf[sftp]["username"], password=self.conf[sftp]["password"])

        # now move the file to the sudo required area!
        if sftp != "sftp_ats":
            archive_destination = self.file_path.replace("/mnt/sftp-filetransfer-bucket/", "/mnt/sftp-filetransfer-bucket-archive/")
            stdin, stdout, stderr = ssh.exec_command(
                "sudo -S -p '' mv {} {}".format(self.file_path, archive_destination))
            stdin.write(self.conf[sftp]["password"] + "\n")
            stdin.flush()

    def close(self):
        self.reader.close()
        self.archive_file()

import requests
import json
from readers.jsonreader import JSONReader, JSONReaderCypherWorx, JSONReaderQuickBooks

class JSONJob(Job):
    def getSource(self):
        if not self.url: return []

        response = requests.get(self.url, auth=(self.auth_user, self.auth_password))
        self.column_mapping = self.getColumnMapping()

        source = JSONReader(response, self.column_mapping, object_key=self.object_key)

        return source.rows()

class JSONCypherWorxJob(Job):
    def getSource(self):
        if not self.url: return []

        response = requests.get(self.url, auth=(self.auth_user, self.auth_password))
        self.column_mapping = self.getColumnMapping()

        source = JSONReaderCypherWorx(response, self.column_mapping, data=self.data)

        return source.rows()

class JSONQuickBooks(Job):
    def getSource(self):
        if not self.url: return []

        response = requests.request("GET", self.url, headers=self.headers, params=self.querystring)
        self.column_mapping = self.getColumnMapping()

        source = JSONReaderQuickBooks(response, self.column_mapping, object_key=self.object_key)
        return source.rows()
