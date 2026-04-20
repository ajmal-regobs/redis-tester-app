import os
import boto3
import redis
from flask import Flask, jsonify, request
from botocore.signers import RequestSigner
from botocore.model import ServiceId

app = Flask(__name__)

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_USER = os.getenv("REDIS_USER")
REDIS_CLUSTER_NAME = os.getenv("REDIS_CLUSTER_NAME")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def generate_iam_auth_token():
    """Generate a short-lived IAM auth token for ElastiCache Redis."""
    session = boto3.Session()
    credentials = session.get_credentials()

    signer = RequestSigner(
        service_id=ServiceId("elasticache"),
        region_name=AWS_REGION,
        signing_name="elasticache",
        signature_version="v4",
        credentials=credentials,
        event_emitter=session.events,
    )

    url = signer.generate_presigned_url(
        {"method": "GET", "url": f"https://{REDIS_CLUSTER_NAME}/?Action=connect&User={REDIS_USER}", "body": {}, "headers": {}, "context": {}},
        operation_name="connect",
        expires_in=900,
        region_name=AWS_REGION,
    )

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


@app.route("/health")
def health():
    try:
        client = connect_redis()
        client.ping()
        client.close()
        return jsonify({"status": "healthy", "redis": "connected"}), 200
    except Exception as e:
        print(f"Health check failed: {e}", flush=True)
        return jsonify({"status": "unhealthy", "redis": str(e)}), 503


@app.route("/write", methods=["POST"])
def write():
    try:
        data = request.get_json()
        key = data.get("key")
        value = data.get("value")

        if not key or value is None:
            return jsonify({"error": "key and value are required"}), 400

        client = connect_redis()
        client.set(key, value)
        client.close()

        return jsonify({"key": key, "value": value, "status": "written"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/read/<key>", methods=["GET"])
def read(key):
    try:
        client = connect_redis()
        value = client.get(key)
        client.close()

        if value is None:
            return jsonify({"key": key, "value": None, "status": "not found"}), 404

        return jsonify({"key": key, "value": value, "status": "found"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/read-all", methods=["GET"])
def read_all():
    try:
        client = connect_redis()
        keys = client.keys("*")
        data = {}
        for key in keys:
            data[key] = client.get(key)
        client.close()

        return jsonify({"count": len(data), "data": data}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
