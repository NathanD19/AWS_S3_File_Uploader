
# AWS FILE UPLOADER

File uploader that uploads data to AWS S3 bucket.

The uploader uses a text file (`upload_time.txt`) to save last upload time.
The uploader will only upload files that have been modified after the last upload time specified in the text file.

Useful for back up processes or just to get some files into an S3 bucket quickly.

#### upload_time.txt
Date time format: `%Y-%m-%d %H:%M:%S.%f`
example: 2021-07-02 15:54:42.360237 

##### Credits
SavvyGuard for base code 
https://gist.github.com/SavvyGuard/6115006
