from base.jobs import Job
import paramiko
from fnmatch import fnmatch
import boto3
from datetime import date
from tempfile import TemporaryFile, SpooledTemporaryFile

class SFTPCopy(Job):
    def run(self):
        if self.verbose:
            print("Running copy job SFTP -> S3")
        # Here we will not run the same process
        # But we will have access to auditing if needed
        # use self.conf['sftp'] for configuration
        cfg = self.conf['sftp']
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        #<Parameterize:> host, user and password into config file


        if self.verbose:
            print("Connecting to SFTP")

        ssh_client.connect(hostname=cfg['hostname'],username=cfg['username'],password=cfg['password'])
        sftp = ssh_client.open_sftp()

        sftpChannel = sftp.get_channel()
        sftpTimeout = sftpChannel.gettimeout()

        if self.verbose:
            print("TImeout:", sftpTimeout)

        # sftpChannel = sftp.get_channel()
        # sftpTimeout = sftpChannel.gettimeout()
        #
        # print("Timeout:" + str(sftpTimeout))

        if self.verbose:
            print("Connected. Listing files in", cfg['directory'])

        fileList = sftp.listdir_attr(path=cfg['directory'])
        pattern = cfg['filename_pattern']
        today = date.today()
        filtered_file_list = [f for f in fileList if \
                                    fnmatch(f.filename, pattern) \
                                    and date.fromtimestamp(f.st_mtime) >= today]

        s3r = boto3.resource('s3')
        # s3c = boto3.client('s3')
        bucket_name = self.conf['s3']['to-process']['bucket']
        bucket = s3r.Bucket(bucket_name)

        if self.verbose:
            print("Filtered file list: ", filtered_file_list)

        for file in filtered_file_list:
            # temp_file = SpooledTemporaryFile(max_size=1073741824, mode='w+b')
            if self.verbose:
                print("Downloading file: ", file.filename)
            with SpooledTemporaryFile() as temp_file:
                sftp.getfo(cfg['directory'] + '/' + file.filename, temp_file)
                if self.verbose:
                    print("Uploading file: ", file.filename)
                bucket.upload_fileobj(temp_file, file.filename, ExtraArgs = {'ServerSideEncryption':'AES256'})

        sftp.close()

    # The following methods will not be called from the parent run()
    # We will call them here only if we need to, but we need to implement them
    #  since they are declared abstract in the Job class
    def configure(self):
        pass

    def close(self):
        pass

    def getSource(self):
        pass

    def getTarget(self):
        pass
