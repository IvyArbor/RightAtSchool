class LocalStreamReader(object):
    """ Reads a file from a local disk by chunks
    Iterates through the lines of the content
    """

    def __init__(self, file_path, chunk_size = 1048576): # 1M = 1048576
        self.chunk_size = chunk_size
        self.stream = open(file_path, 'r')

    def nextChunk(self):
        data = self.stream.read(self.chunk_size)
        return data

    def lines(self):
        buffer = ''
        """Lazy generator to read a file by chunks and yield non-empty lines.
        """
        while True:
            data = self.nextChunk()
            if not data:
                self.stream.close()
                if buffer: yield buffer
                break
            buffer = buffer + data
            lines = buffer.split('\n')
            for line in lines[:-1]:
                if line: yield line.rstrip('\r')
            buffer = lines[-1]


#python SDK for Amazon Web Services (AWS)
import boto3

class S3StreamReader(LocalStreamReader):
    """ Reads a file from S3 downloading the file by chunks
    Iterates through the lines of the content
    """

    def __init__(self, bucket_name, key, chunk_size = 1048576):
        self.chunk_size = chunk_size
        self.bucket_name = bucket_name
        self.key = key

        # Using low-level client so we can read data by chunks
        s3_client = boto3.client('s3')
        a = s3_client.get_object(Bucket=self.bucket_name, Key=self.key)
        self.stream = a['Body'] # StreamingBody

    def nextChunk(self):
        data = self.stream.read(self.chunk_size).decode('ISO-8859-1')
        return data



from io import BytesIO
import zipfile
class S3ZipReader(object):
    """ Reads a file from a zip file on S3
    Iterates through the lines of the content
    """

    def __init__(self, bucket_name, key, zipped_filename):
        self.bucket_name = bucket_name
        self.key = key
        self.zipped_filename = zipped_filename

        # Using low-level client so we can read data by chunks
        s3_client = boto3.client('s3')
        a = s3_client.get_object(Bucket=self.bucket_name, Key=self.key)
        self.stream = a['Body'] # StreamingBody

    def getFile(self):
        return BytesIO(self.stream.read())

    def lines(self):
        binary = self.getFile()
        with zipfile.ZipFile(binary, mode='r') as zipf:
            with zipf.open(self.zipped_filename) as myfile:
                contents = myfile.read()
                for line in contents.splitlines():
                    yield line.decode('ISO-8859-1')
