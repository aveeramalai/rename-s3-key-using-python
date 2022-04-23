import json
import boto3
from botocore.exceptions import ClientError

s3_resource = boto3.resource('s3')


def get_all_s3_keys(sourceBucketName, key_prefix, dry_run_flag):
    """
    Get all S3 keys for the given key prefix

    Args:
        sourceBucketName (string): Name of the Source bucket.
        key_prefix (string): Get all the keys for the given prefix.
        dry_run_flag (bool): Flag to determine to update the keys or not.
    Returns:
        int: Total number of records processed.
    """
    try:
        counter = 0
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator("list_objects_v2")
        kwargs = {'Bucket': sourceBucketName, 'Prefix': key_prefix}
        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
            except KeyError:
                break

            for content in contents:
                s3transferfiles(sourceBucketName, content["Key"], dry_run_flag)
                counter += 1
        return counter
    except ClientError as e:
        print(e.response['Error']['Message'])


def s3transferfiles(sourceBucketName, keyName, dry_run_flag):
    """
    This function will update key prefix and insert the label for all the partitions. 
    
    Args:
        sourceBucketName (string): Name of the Source bucket.
        keyName (string): S3 key prefix to update.
        dry_run_flag (bool): Flag to determine to update the keys or not.
    """
    try:
        if keyName is not None:
            file = keyName
            try:
                # Get the file name with extension
                fileName = file[file.rindex('/')+1:len(file)]
                year=2022
                month=04
                day=13
                newKey = f"dev/Label1=PREFIX1/Label2=PREFIX2/Label3=PREFIX3/year={year}/month={month}/day={day}/{fileName}"

                print(f"old key = > {file}")
                print(f"new key = > {newKey}")

                if dry_run_flag is False:
                    input_source = {'Bucket': sourceBucketName, 'Key': file}
                    s3_resource.Object(sourceBucketName, newKey).copy_from(
                        CopySource=input_source)

            except ClientError as e:
                print(e.response['Error']['Message'])
            else:
                print('Success')
        else:
            print('No matching records')
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print('Operation completed')


def main():
    result = get_all_s3_keys(
        sourceBucketName="<BUCKET NAME>", key_prefix="<KEY PREFIX>", dry_run_flag=True)
    print(f"Total Number of records processed is {result}")


if __name__ == "__main__":
    main()
