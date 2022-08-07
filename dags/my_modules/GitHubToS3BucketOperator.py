from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
from boto3 import client

class GitHubToS3Operator(BaseOperator):

    def __init__(self, *args, **kwargs):
        super(GitHubToS3Operator, self).__init__(*args, **kwargs)

    def get_data(self,url):
        df = pd.read_csv(url)
        return df.to_csv(index=False)
    
    def send_to_bucket(self,content,bucket,file_name):
        s3_client = client("s3")
        #s3_client = client("s3",aws_access_key_id="",
        #aws_secret_access_key="")
        key = "data/"+file_name
        response = s3_client.put_object(Body=content,Bucket=bucket,Key=key)
        return response
    
    def execute(self,context):
        bucket_name = "mafer-bucket-deb-220296"
        files = [
                {"url":"https://raw.githubusercontent.com/maferchavez/Data-Bootcamp-Project/main/raw_data/movie_review.zip",
                 "name":"movie_review.csv"
                },
                {"url":"https://raw.githubusercontent.com/maferchavez/Data-Bootcamp-Project/main/raw_data/log_reviews.zip",
                "name":"log_reviews.csv"
                }
                ]
        for file in files:
            df = self.get_data(file["url"])
            self.send_to_bucket(df,bucket_name,file.get("name"))
        