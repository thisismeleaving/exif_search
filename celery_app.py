from celery import Celery

redis = 'redis://localhost:6379/0'
app = Celery('ExifSearch', broker=redis, backend=redis, include=['exif'])

app.conf.update(**{
    'CELERY_ROUTES' : {
        'exif.get_image' : {
            'queue' : 'http'
        },
        'exif.get_exif_data' : {
            'queue' : 'data_processing'
        }
    }
})

if __name__ == '__main__':
    app.start()