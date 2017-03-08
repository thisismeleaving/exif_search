"""
Author: Berty Pribilovics

Get the list of image record data from S3 and queue up the celery tasks
to process them.  The task to extract exif data is chained to each task that
fetches the individual image.

This demo expects two celery workers to be running.
e.g.
celery -A celery_app -Q http -n image_fetcher
celery -A celery_app -Q data_processing -n exif_extractor

"""
from operator import itemgetter
from celery import chain

import exif

if __name__ == '__main__':
    records = exif.get_image_records()
    for r in records:
        chain(
            exif.get_image.s(r.get('Key')),
            exif.get_exif_data.s()
        ).apply_async()
