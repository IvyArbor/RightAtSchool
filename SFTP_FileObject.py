#Used for SFTP connection
import paramiko
#Used for datetime conversion
import datetime
#Used for file system items
import os
#Used for AWS
import boto3


#SFTP Configuration (START)

#Connect to paramiko SSHclient which includes SFTP functionality
ssh_client = paramiko.SSHClient()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#<Parameterize:> host, user and password into config file
ssh_client.connect(hostname='rxdatadirector.catamaranrx.com',username='LDIsFTPUser',password='Xuf5dabU')
sftp = ssh_client.open_sftp()

#<Parameterize:>
sftpDirectory = 'OUT/Extracts'
fileNamingConvention = 'LDIEXTRACT_'

sftpChannel = sftp.get_channel()
sftpTimeout = sftpChannel.gettimeout()

print("Timeout:" + str(sftpTimeout))


#SFTP Configuration (END)

#AWS S3 Configuration (START)

#one s3 service resource for both the client (low level) and resource (high level)
#Each service resource can do different things
s3r = boto3.resource('s3')
s3c = boto3.client('s3')

#BucketName: Set the name of the bucket where files need to be uploaded
#BucketFolder: Set the name of the "folder" - in the bucket - where the file needs to reside
#Bucket: Set the actual bucket that will be used for file upload
BucketName = 'ldi.datafile.to-process'
Bucket = s3r.Bucket(BucketName)

#AWS S3 Configuration (END)


#Local Download Path Configuration (START)

#Folder is the location where SFTP file will be downloaded
downloadFolder = 'D:/ETL/'

#use this if you want to rename the file to something other than the file that is on the SFTP
#downloadFileName = ''

#Local Download Path Configuration (END)


print("User:" + os.getlogin())

#Today in datetime format; used to compare dates for files
today = datetime.date.today()
print("Today is: " + str(today))

#List attributes of all files in the SFTP directory
fileList = sftp.listdir_attr(path=sftpDirectory)

for file in fileList:
    #Only files that have a specific naming convention
    if file.filename.startswith(fileNamingConvention):
        if datetime.date.fromtimestamp(file.st_mtime) >= today:
            #if st_mtime
            #if st_mtime in time.strftime("%d/%m/%Y"):
            print(file.filename)
            #This will get the file and load to the D: (WORKS!)
            #sftp.get(sftpDirectory + '/' + file.filename, downloadFolder + file.filename)

            #Upload the file (utilizing the filepath created above) to S3
            #Bucket.upload_file('<FilePath\FileName>', '<Target "Key"/FileName in S3>')
            #print('begin upload @ ' + str(datetime.datetime.today()))
            #Bucket.upload_file(downloadFolder + file.filename, file.filename)
            #print("Successfully uploaded file " + file.filename)
            #print('End upload @ ' + str(datetime.datetime.today()))

            #Set encryption for the file
            #Boto3 (AWS SDK for python) does not have a way to encrypt it while uploading file (using the upload_file method
            #Using the put_object allows encryption but does not hold the actual file within the zip folder)
            #s3c.copy_object(CopySource={'Bucket':BucketName, 'Key': file.filename},Bucket=BucketName, Key=file.filename, ServerSideEncryption='AES256')
            #print("Successfully encrypted file " + file.filename)


            print('begin upload @ ' + str(datetime.datetime.today()))

            data = sftp.open(sftpDirectory + '/' + file.filename,'r',1)
            Bucket.put_object(Body=data.read(), Bucket=BucketName, Key=file.filename, ServerSideEncryption='AES256')

            print('End upload @ ' + str(datetime.datetime.today()))
