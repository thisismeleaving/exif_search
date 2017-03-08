"""
Author: Berty Pribilovics

Queue up a chained set of tasks
 - get an image
   - get exif data for that image
   - store results in elasticsearch

"""
import xml.etree.ElementTree as ET

from operator import itemgetter

import requests
from pymongo import MongoClient
from PIL import Image, ExifTags

from celery_app import app

_URL = 'http://s3.amazonaws.com/waldo-recruiting'
_NAMESPACE = {'photos': 'http://s3.amazonaws.com/doc/2006-03-01/'}
_IMG_CACHE = 'img_cache/'



@app.task
def get_image(_id):
    """
    For the given _id, HTTP GET to cache the image file

    """
    print('recieved Key: {}'.format(_id))
    resource_uri = '{}/{}'.format(_URL, _id)
    file_to_write = '{}/{}'.format(_IMG_CACHE, _id)
    response = requests.get(resource_uri, timeout=300)
    print(response.status_code)
    with open(file_to_write, 'wb') as f:
        f.write(response.content)
    return file_to_write


@app.task
def get_exif_data(file):
    """
    Return a dict of the exif tag key/value pairs for the given image

    """
    # this should be expanded on to use @contextmanager
    mongo_client = MongoClient()
    mongo_dbc = mongo_client.test

    with open(file) as f:
        img = Image.open(file)
        exif = {
            ExifTags.TAGS[k]: v
            for k, v in img._getexif().items()
            if k in ExifTags.TAGS
        }

    mongo_dbc.images.insert_one({
      'record': file,
      'exif': exif
    })
    # print(exif)


@app.task
def get_image_records():
    """
    Get the xml image data and convert it into a list of dicts

    """
    res = requests.get(_URL)
    tree = ET.fromstring(res.text)

    records = [
        {
            field.tag.replace(_NAMESPACE.get('photos'), '').strip('{}'): field.text
            for field in record
        }
        for record in tree.findall('photos:Contents', _NAMESPACE)
    ]

    records.sort(key=itemgetter('Size'))

    return records
