import json
import boto3
import pandas as pd

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']

    try:
        # Read the JSON file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        data = response['Body'].read().decode('utf-8')
        records = json.loads(data)

        # Create a DataFrame and filter records
        df = pd.DataFrame(records)
        filtered_df = df[df['status'] == 'delivered']

        # Write the filtered DataFrame to a new JSON file
        output_key = object_key.replace('raw_input', 'processed')
        filtered_data = filtered_df.to_json(orient='records')
        s3_client.put_object(Bucket='doordash-processed-data', Key=output_key, Body=filtered_data)

        # Publish a success message to SNS
        sns_client.publish(
            TopicArn='arn:aws:sns:us-east-1:905418111531:doordash-notifications',
            Message=f'Successfully processed {object_key}',
            Subject='Processing Complete'
        )

        return {
            'statusCode': 200,
            'body': json.dumps('Processing complete')
        }
    except Exception as e:
        # Publish a failure message to SNS
        sns_client.publish(
            TopicArn='arn:aws:sns:region:account-id:doordash-notifications',
            Message=f'Failed to process {object_key}: {str(e)}',
            Subject='Processing Failed'
        )
        return {
            'statusCode': 500,
            'body': json.dumps('Processing failed')
        }
