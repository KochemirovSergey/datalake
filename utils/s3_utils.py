import boto3
from botocore.client import Config


def get_s3_client(endpoint_url: str, access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
    )


def upload_html(
    client,
    bucket: str,
    key: str,
    local_path: str,
    public_base_url: str,
) -> str:
    """
    Загружает HTML-файл в S3, возвращает публичный URL.
    ContentType выставляется text/html; charset=utf-8.
    """
    with open(local_path, "rb") as f:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=f,
            ContentType="text/html; charset=utf-8",
        )
    return f"{public_base_url.rstrip('/')}/{bucket}/{key}"
