from __future__ import print_function
import httplib2
import os
import base64
from apiclient import errors
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage


try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/gmail-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/gmail.modify'
CLIENT_SECRET_FILE = 'client_secret_Gmail.json'
APPLICATION_NAME = 'Gmail API Python Quickstart'

'''
#removed directory with data
import shutil
shutil.rmtree('laborReports/', ignore_errors=True)

#create directory is it does not exist
if not os.path.exists("laborReports/"):
    os.makedirs("laborReports/")

#removed directory with data
import shutil
shutil.rmtree('payPeriodReports/', ignore_errors=True)

#create directory is it does not exist
if not os.path.exists("payPeriodReports/"):
    os.makedirs("payPeriodReports/")
'''

def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'gmail-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


messages = []
def ListMessagesMatchingQuery(service, user_id, query=''):
  """List all Messages of the user's mailbox matching the query.

  Args:
    service: Authorized Gmail API service instance.
    user_id: User's email address. The special value "me"
    can be used to indicate the authenticated user.
    query: String used to filter messages returned.
    Eg.- 'from:user@some_domain.com' for Messages from a particular sender.

  Returns:
    List of Messages that match the criteria of the query. Note that the
    returned list contains Message IDs, you must use get with the
    appropriate ID to get the details of a Message.
  """
  try:
    response = service.users().messages().list(userId=user_id,
                                               q=query).execute()

    if 'messages' in response:
      messages.extend(response['messages'])


    while 'nextPageToken' in response:
      page_token = response['nextPageToken']
      response = service.users().messages().list(userId=user_id, q=query,pageToken=page_token).execute()

      messages.extend(response['messages'])

    return messages
  except errors.HttpError as error:
    print ('An error occurred: %s' % error)

credentials = get_credentials()
http = credentials.authorize(httplib2.Http())
service = discovery.build('gmail', 'v1', http=http)

def GetAttachments(service, user_id, msg_id, store_dir):
  """Get and store attachment from Message with given id.
  Args:
  service: Authorized Gmail API service instance.
  user_id: User's email address. The special value "me"
  can be used to indicate the authenticated user.
  msg_id: ID of Message containing attachment.
  prefix: prefix which is added to the attachment filename on saving
  """

  try:
      message = service.users().messages().get(userId=user_id, id=msg_id).execute()
      if "parts" in message['payload']:
          print (message['payload']['parts'])
          for part in message['payload']['parts']:
              newvar = part['body']
              if 'attachmentId' in newvar:
                  att_id = newvar['attachmentId']
                  att = service.users().messages().attachments().get(userId=user_id, messageId=msg_id, id=att_id).execute()
                  data = att['data']
                  print("PART: ", part)
                  file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))
                  print(part['filename'])
                  path = ''.join([store_dir, part['filename']])
                  f = open(path, 'wb')
                  f.write(file_data)
                  f.close()
  except errors.HttpError as error:
    print('An error occurred: %s' % error)


ListMessagesMatchingQuery(discovery.build('gmail', 'v1', http=http), 'me', 'NOVA4000Alerts ASC15315: LABOR REPORT-JHSU after:2017/12/20 is:unread')
i=0
for x in messages:
    print (x["id"])
    GetAttachments(service, 'me', x["id"], store_dir="laborReports/")
    markAsRead = service.users().messages().modify(userId='me', id=messages[i]["id"], body={ 'removeLabelIds': ['UNREAD']}).execute()
    i+=1

ListMessagesMatchingQuery(discovery.build('gmail', 'v1', http=http), 'me').clear()
ListMessagesMatchingQuery(discovery.build('gmail', 'v1', http=http), 'me', 'NOVA4000Alerts ASC15315: LABOR REPORT_PAST PAY PERIOD-JHSU after:2017/12/20 is:unread')
ListMessagesMatchingQuery(discovery.build('gmail', 'v1', http=http), 'me', 'NOVA4000Alerts ASC15315: LABOR REPORT_PAST PAY PERIOD2-JHSU after:2017/12/20 is:unread')
i=0
for y in messages:
    print (y["id"])
    GetAttachments(service, 'me', y["id"], store_dir="payPeriodReports/")
    markAsRead = service.users().messages().modify(userId='me', id=messages[i]["id"], body={ 'removeLabelIds': ['UNREAD']}).execute()
    i+=1



# print (messages[0]["id"])
#
# # #this will get all UNREAD email attachments from NovaTime email
# GetAttachments(service, 'me', messages[0]["id"], store_dir="payPeriodReports/")
#
# # #this will mark the message as read
# markAsRead = service.users().messages().modify(userId='me', id=messages[0]["id"], body={ 'removeLabelIds': ['UNREAD']}).execute()
