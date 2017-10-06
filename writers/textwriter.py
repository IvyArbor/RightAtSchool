from tempfile import TemporaryFile
from boto3.s3.transfer import S3Transfer
import boto3
import os

class TextWriter(object):
    '''Reads lines of a file and splits them to the map'''

    def __init__(self, bucket_name, bucket_folder, file_name, column_mapping):
        self.bucket_name = bucket_name
        self.bucket_folder = bucket_folder
        self.file_name = file_name
        self.column_mapping = column_mapping
        self.tmp_file = TemporaryFile(mode='w', delete=False)

    def format(self, length, type, value):
    	if value == None:
    		value = ''
    	elif type == 'integer':
    		value = str(value).zfill(length)
    	elif type == "date":
    		value = "{:%Y%m%d}".format(value)
    	elif type == "decimal":
    		value = float(value)
    		sign = "-" if value < 0 else " "
    		value = str(abs(int(value * 100))).zfill(length-1) + sign
    	return ("{:"+str(length)+"}").format(value[:length])

    def insert(self, row, process = True):
        if process:
            line = []
            for name, length, type in self.column_mapping:
                value = row[name]
                line.append(self.format(length, type, row[name]))
            row = ''.join(line) + "\n"
        self.tmp_file.write(row)

    def close(self):
        # Send self.tmp_file to AWS
        s3r = boto3.resource('s3')
        bucket = s3r.Bucket(name=self.bucket_name)
        prefix = self.bucket_folder + '/' if self.bucket_folder else ''
        filename = self.tmp_file.name
        self.tmp_file.close()
        bucket.upload_file(filename, prefix + self.file_name, ExtraArgs={'ServerSideEncryption': "AES256"})
        os.remove(filename)
