import boto
import boto.s3

import os.path
import sys

import secrets

from datetime import datetime, timedelta

# AWS KEYS
AWS_ACCESS_KEY_ID = secrets.AWS_ACCESS_KEY_ID
AWS_ACCESS_KEY_SECRET = secrets.AWS_ACCESS_KEY_SECRET
# Fill in info on data to upload
# destination bucket name
bucket_name = secrets.bucket_name
# source directory
sourceDir = secrets.sourceDir
# destination directory name (on s3)
destDir = secrets.destDir

#max size in bytes before uploading in parts. between 1 and 5 GB recommended
MAX_SIZE = 20 * 1000 * 1000
#size of parts when uploading in parts
PART_SIZE = 6 * 1000 * 1000

process_start_time = datetime.now()

conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY_SECRET)

# bucket = conn.create_bucket(bucket_name,
#         location=boto.s3.connection.Location.DEFAULT)

bucket = conn.get_bucket(bucket_name)

# get last update time from file
upload_time_f = open("upload_time.txt", "r+")
# 2021-07-01 00:00:00.000000
last_upload_time = datetime.strptime(upload_time_f.read(),
                                     '%Y-%m-%d %H:%M:%S.%f')

print('Last upload time: ' + str(last_upload_time))
sys.stdout.write('Searching for new files in... ')
sys.stdout.flush()

print(sourceDir)
uploadFileNames = []
for (sourceDir, dirname, filename) in os.walk(sourceDir):
    for file in filename:
        try:
            last_modified = datetime.fromtimestamp(
                os.path.getmtime(sourceDir + file))

            if (last_modified > last_upload_time):
                print('last_modified', file, last_modified)
                uploadFileNames.append(file)
                continue
        except:
            # print('Error: last_modified - ' + file)
            pass

        try:
            last_created = datetime.fromtimestamp(
                os.path.getctime(sourceDir + file))
            if (last_created > last_upload_time):
                print('last_created', file, last_modified)
                uploadFileNames.append(file)
                continue
        except:
            # print('Error: last_created - ' + file)
            pass

        try:
            last_accessed = datetime.fromtimestamp(
                os.path.getctime(sourceDir + file))

            if (last_accessed > last_upload_time):
                print('last_accessed', file, last_modified)
                uploadFileNames.append(file)
                continue
        except:
            # print('Error: last_accessed - ' + file)
            pass

    # uploadFileNames.extend(filename)
    break


def percent_cb(complete, total):
    sys.stdout.write('.')
    sys.stdout.flush()


for filename in uploadFileNames:
    sourcepath = os.path.join(sourceDir + filename)
    destpath = os.path.join(destDir, filename)
    print('Uploading %s to Amazon S3 bucket %s' % \
           (sourcepath, bucket_name))

    filesize = os.path.getsize(sourcepath)
    if filesize > MAX_SIZE:
        print("multipart upload")
        mp = bucket.initiate_multipart_upload(destpath)
        fp = open(sourcepath, 'rb')
        fp_num = 0
        while (fp.tell() < filesize):
            fp_num += 1
            print("uploading part %i" % fp_num)
            mp.upload_part_from_file(fp,
                                     fp_num,
                                     cb=percent_cb,
                                     num_cb=10,
                                     size=PART_SIZE)

        mp.complete_upload()

    else:
        print("singlepart upload")
        k = boto.s3.key.Key(bucket)
        k.key = destpath
        k.set_contents_from_filename(sourcepath, cb=percent_cb, num_cb=10)

# Complete, update last update time
upload_time_f.seek(0)
upload_time_f.write(str(process_start_time))
upload_time_f.truncate()

print("Upload completed. \nStarted: " + str(process_start_time) +
      "; Completed: " + str(datetime.now()) + ";")
