from dagster import ConfigurableResource, EnvVar


class S3Config(ConfigurableResource):
    endpoint_url: str = EnvVar("S3_ENDPOINT_URL")
    access_key: str = EnvVar("S3_ACCESS_KEY")
    secret_key: str = EnvVar("S3_SECRET_KEY")
    bucket: str = EnvVar("S3_DASHBOARD_BUCKET")
    public_base_url: str = EnvVar("S3_PUBLIC_BASE_URL")
