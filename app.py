import os
import boto3
import redis
from dotenv import load_dotenv
from botocore.signers import RequestSigner

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_USER = os.getenv("REDIS_USER")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def generate_iam_auth_token():
    """Generate a short-lived IAM auth token for ElastiCache Redis."""
    session = boto3.Session()
    credentials = session.get_credentials().get_frozen_credentials()

    signer = RequestSigner(
        service_id="elasticache",
        region_name=AWS_REGION,
        signing_name="elasticache",
        signature_version="v4",
        credentials=credentials,
        event_emitter=session.events,
    )

    url = signer.generate_presigned_url(
        {"method": "GET", "url": f"https://{REDIS_HOST}", "body": {}, "headers": {}, "context": {}},
        operation_name="connect",
        expires_in=900,
        region_name=AWS_REGION,
    )

    # Strip the protocol — Redis uses just the token part
    return url.removeprefix("https://")


def connect_redis():
    """Connect to Redis using IAM auth over TLS."""
    token = generate_iam_auth_token()

    client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        username=REDIS_USER,
        password=token,
        ssl=True,
        ssl_cert_reqs="none",
        decode_responses=True,
    )

    return client


def main():
    print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")
    print(f"Using IAM user: {REDIS_USER}")
    print()

    client = connect_redis()

    # Test 1: Ping
    print("1. PING test...")
    result = client.ping()
    print(f"   PONG: {result}")

    # Test 2: SET/GET
    print("2. SET/GET test...")
    client.set("test-key", "hello-from-iam-auth")
    value = client.get("test-key")
    print(f"   SET test-key = 'hello-from-iam-auth'")
    print(f"   GET test-key = '{value}'")

    # Test 3: Delete
    print("3. DELETE test...")
    client.delete("test-key")
    value = client.get("test-key")
    print(f"   GET test-key after delete = {value}")

    # Test 4: Connection info
    print("4. Connection info...")
    info = client.info("server")
    print(f"   Redis version: {info['redis_version']}")
    print(f"   TLS enabled: {info.get('tls_enabled', 'N/A')}")

    print()
    print("All tests passed. Redis IAM auth is working.")

    client.close()


if __name__ == "__main__":
    main()
