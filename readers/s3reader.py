from io import BytesIO
import boto3

class S3Reader(object):
    """ Reads a file from a local disk by chunks
    Iterates through the lines of the content
    """

    def __init__(self, bucket_name, key):
        self.bucket_name = bucket_name
        self.key = key

        # Using low-level client so we can read data by chunks
        s3_client = boto3.client('s3')
        a = s3_client.get_object(Bucket=self.bucket_name, Key=self.key)
        self.stream = a['Body'] # StreamingBody

    def getFile(self):
        return BytesIO(self.stream.read())
