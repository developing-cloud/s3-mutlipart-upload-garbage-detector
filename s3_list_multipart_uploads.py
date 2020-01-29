from datetime import datetime, timezone

import boto3

s3 = boto3.client('s3')

max_days_to_wait_for_upload_completion = 2
size_count = 0

for bucket in [response['Name'] for response in s3.list_buckets()['Buckets']]:
    # print (f'I am now in the bucket {bucket}')
    try:
        multipart_uploads_response = s3.list_multipart_uploads(Bucket=bucket)
        uploads_key = 'Uploads'
        if uploads_key in multipart_uploads_response:
            for upload in multipart_uploads_response[uploads_key]:
                initiated = upload['Initiated']
                now = datetime.now(timezone.utc)
                days_elapsed = (now - initiated).days
                if days_elapsed > max_days_to_wait_for_upload_completion:
                    print(
                        f'Found in-progress multipart upload in the bucket {bucket} older than {max_days_to_wait_for_upload_completion} days [actual days elapsed: {days_elapsed}]')
                    parts_response = s3.list_parts(Bucket=bucket, Key=upload['Key'], UploadId=upload['UploadId'])
                    key = parts_response['Key']
                    print(f'  Key: {key}')
                    parts_key = 'Parts'
                    if parts_key in parts_response:
                        for part in parts_response[parts_key]:
                            print(f'    Part: {part}')
                            size_count = size_count + int(part['Size'])
                    else:
                        print(f'    No parts included for {key}')
    except Exception as e:
        print(str(e))
size_count_mb = int(size_count / 1024 / 1024)
print(f'Storage to save: {size_count_mb} MB')
