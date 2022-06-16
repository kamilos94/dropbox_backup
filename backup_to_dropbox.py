#!/opt/bin/python
from configparser import ConfigParser
from datetime import datetime
import json
import logging
import os
import sys
import time

import dropbox
from dropbox.files import WriteMode
from dropbox.exceptions import ApiError, AuthError

config = ConfigParser()
config.read('/absolute/path/to/config')

APP_KEY = config['params']['APP_KEY']
APP_SECRET = config['params']['APP_SECRET']
TOKEN_URL = config['params']['TOKEN_URL']
REFRESH_TOKEN = config['params']['REFRESH_TOKEN']
SOURCE_DIR = config['params']['SOURCE_DIR']
DROPBOX_DIR = config['params']['DROPBOX_DIR']

data = {
    'grant_type': 'refresh_token',
    'refresh_token': REFRESH_TOKEN,
}
r = requests.post(TOKEN_URL, data=data, auth=(APP_KEY, APP_SECRET))
output = json.loads(r.text)
TOKEN = output['access_token']

log_path = '/path/to/log'
log_file = f'{log_path}{os.path.basename(__file__).replace(".py", ".log")}'
if not os.path.exists(log_path):
    os.makedirs(log_path)
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s|%(levelname)s|%(message)s')


def upload(dbx, file, overwrite=False):
    filename = DROPBOX_DIR + file
    with open(file, 'rb') as f:
        data = f.read()
    mode = (dropbox.files.WriteMode.overwrite
            if overwrite
            else dropbox.files.WriteMode.add)
    mtime = os.path.getmtime(file)
    client_modified = datetime(*time.gmtime(mtime)[:6])

    try:
        logging.info(f'Uploading {filename} to Dropbox')
        dbx.files_upload(data, filename, mode, client_modified=client_modified, mute=True)
    except ApiError as err:
        if (err.error.is_path() and err.error.get_path().reason.is_insufficient_space()):
            logging.error('Cannot back up; insufficient space.')
            sys.exit()
        elif err.user_message_text:
            logging.error(err.user_message_text)
            sys.exit()
        else:
            logging.error(err)
            sys.exit()


def dbx_get_md(dbx, subfolder):
    dbx_path = f"/{DROPBOX_DIR}/{subfolder.replace(os.path.sep, '/')}"
    try:
        folder = dbx.files_list_folder(dbx_path)
    except dropbox.exceptions.ApiError as err:
        logging.info(f'Folder list failed for {dbx_path}, -- assumed empty: err')
        return {}
    else:
        return {entry.name: entry for entry in folder.entries}


if __name__ == '__main__':
    dbx = dropbox.Dropbox(TOKEN)
    try:
        dbx.users_get_current_account()
    except AuthError:
        logging.error('Invalid access token. Cannot authenticate.')
        sys.exit()

    os.chdir(SOURCE_DIR)
    for root, _, files in os.walk(SOURCE_DIR):
        if files:
            for file in files:
                if file.startswith('.'): continue
                root = root.replace(SOURCE_DIR, '')
                relative_path_filename = root + '/' + file
                mtime = os.path.getmtime(relative_path_filename)
                mtime_dt = datetime(*time.gmtime(mtime)[:6])
                dbx_files_metadata = dbx_get_md(dbx, root)
                if file in dbx_files_metadata:
                    md = dbx_files_metadata[file]
                    size = os.path.getsize(relative_path_filename)
                    if (isinstance(md, dropbox.files.FileMetadata) and mtime_dt == md.client_modified and size == md.size):
                        logging.info(f'{file} is already synced [stats match]')
                    else:
                        upload(dbx, relative_path_filename, overwrite=True)
                else:
                    upload(dbx, relative_path_filename)
    logging.info('Backup done!')
