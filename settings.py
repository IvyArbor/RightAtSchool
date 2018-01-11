from os.path import join, dirname
from dotenv import load_dotenv
from os import environ as env

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

conf = {
    "s3": {
        "to-process": {
            "bucket": env.get('S3_PROCESS_BUCKET'),
        },
        "archive": {
            "enabled": True,
            "bucket": env.get('S3_ARCHIVE_BUCKET'),
        },
    },
    "sftp": {
        "hostname": env.get('SFTP_HOSTNAME'),
        "username": env.get('SFTP_USERNAME'),
        "password": env.get('SFTP_PASSWORD'),
        "port": env.get('SFTP_PORT')
    },
    "sftp_ats": {
        "hostname": env.get('SFTP_ATS_HOSTNAME'),
        "username": env.get('SFTP_ATS_USERNAME'),
        "password": env.get('SFTP_ATS_PASSWORD'),
        "port": env.get('SFTP_ATS_PORT')
    },
    'mysql': {
        "AUDIT": {
            "host": env.get('AUDIT_HOST'),
            "user": env.get('AUDIT_USER'),
            "password": env.get('AUDIT_PASSWORD'),
            "database": env.get('AUDIT_DATABASE'),
            "ssl": None if env.get('AUDIT_USE_SSL') == 'No' else {
                'ca': env.get('AUDIT_SSL_CA_BUNDLE'),
            },
        },
        "rightatschool_testdb": {
            "host": env.get('STG_HOST'),
            "user": env.get('STG_USER'),
            "password": env.get('STG_PASSWORD'),
            "database": env.get('STG_DATABASE'),
            "ssl": None if env.get('STG_USE_SSL') == 'No' else {
                'ca': env.get('STG_SSL_CA_BUNDLE'),
            },
        },
        "rightatschool_productiondb": {
            "host": env.get('DW_HOST_PROD'),
            "user": env.get('DW_USER_PROD'),
            "password": env.get('DW_PASSWORD_PROD'),
            "database": env.get('DW_DATABASE_PROD'),
            "ssl": None if env.get('DW_USE_SSL_PROD') == 'No' else {
                'ca': env.get('DW_SSL_CA_BUNDLE'),
            },
        },
        "DW": {
            "host": env.get('DW_HOST'),
            "user": env.get('DW_USER'),
            "password": env.get('DW_PASSWORD'),
            "database": env.get('DW_DATABASE'),
            "ssl": None if env.get('DW_USE_SSL') == 'No' else {
                'ca': env.get('DW_SSL_CA_BUNDLE'),
            },
        },
    },
    "paths": {
      "local": env.get('LOCAL_FILE_PATH'),
    },
}

if __name__ == "__main__":
    print(conf)
