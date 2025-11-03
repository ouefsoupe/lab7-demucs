# rest/rest-server.py
import os, io, sys, base64, hashlib, json, platform
from flask import Flask, request, jsonify, send_file
from redis import StrictRedis
from minio import Minio

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
WORK_QUEUE  = os.getenv("WORK_QUEUE", "toWorker")
LOG_LIST    = "logging"

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS   = os.getenv("S3_ACCESS_KEY", "rootuser")
S3_SECRET   = os.getenv("S3_SECRET_KEY", "rootpass123")
S3_SECURE   = S3_ENDPOINT.startswith("https")
INPUT_BUCKET  = os.getenv("INPUT_BUCKET", "queue")
OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET", "output")

def rclient(): return StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)
def s3():
    endpoint = S3_ENDPOINT.replace("http://","").replace("https://","")
    return Minio(endpoint, access_key=S3_ACCESS, secret_key=S3_SECRET, secure=S3_SECURE)

def log(kind, msg):
    key = f"{platform.node()}.rest.{kind}"
    print(kind.upper()+":", msg, flush=True)
    rclient().lpush(LOG_LIST, f"{key}:{msg}")

app = Flask(__name__)

@app.route("/", methods=["GET"])
def hello():
    return '<h1>Music Separation Server</h1><p>Use /apiv1/* endpoints</p>'

# helper: accept both proper JSON and jsonpickle string body
def parse_body():
    raw = request.get_data(cache=False, as_text=True)
    try:
        return json.loads(raw)
    except Exception:
        return request.get_json(force=True, silent=True) or {}

@app.route("/apiv1/separate", methods=["POST"])
def separate():
    """
    Body: { mp3: <base64 string>, model?: <str>, callback?: {url, data?} }
    Returns: { hash: <songhash>, reason: "Song enqueued for separation" }
    """
    body = parse_body()
    if "mp3" not in body:
        return ("missing 'mp3' base64 field", 400)

    model = body.get("model", "mdx_extra_q")
    callback = body.get("callback")

    try:
        audio = base64.b64decode(body["mp3"])
    except Exception:
        return ("invalid base64 in 'mp3'", 400)

    songhash = hashlib.sha256(audio).hexdigest()[:56]

    # upload input object to MinIO
    mn = s3()
    if not mn.bucket_exists(INPUT_BUCKET):
        mn.make_bucket(INPUT_BUCKET)
    mn.put_object(
        INPUT_BUCKET, f"{songhash}.mp3",
        io.BytesIO(audio), length=len(audio),
        content_type="audio/mpeg"
    )

    # enqueue job for worker
    payload = {"hash": songhash, "model": model, "callback": callback}
    rclient().lpush(WORK_QUEUE, json.dumps(payload))
    log("info", f"queued {songhash} model={model}")

    return jsonify({"hash": songhash, "reason": "Song enqueued for separation"})

@app.route("/apiv1/queue", methods=["GET"])
def queue_dump():
    rc = rclient()
    n = rc.llen(WORK_QUEUE)
    items = [x.decode("utf-8") for x in rc.lrange(WORK_QUEUE, 0, max(n-1, -1))]
    return jsonify({"queue": items})

@app.route("/apiv1/track/<songhash>/<track>", methods=["GET"])
def get_track(songhash, track):
    obj = f"{songhash}-{track}"
    mn = s3()
    if not mn.bucket_exists(OUTPUT_BUCKET):
        return ("output bucket not found", 404)
    try:
        data = mn.get_object(OUTPUT_BUCKET, obj)
        return send_file(
            io.BytesIO(data.read()),
            mimetype="audio/mpeg",
            as_attachment=True,
            download_name=obj
        )
    except Exception:
        return ("track not found", 404)

@app.route("/apiv1/remove/<songhash>/<track>", methods=["GET","DELETE"])
def remove_track(songhash, track):
    obj = f"{songhash}-{track}"
    mn = s3()
    try:
        mn.remove_object(OUTPUT_BUCKET, obj)
        log("info", f"deleted {obj}")
        return jsonify({"deleted": obj})
    except Exception as e:
        return (str(e), 404)

if __name__ == "__main__":
    # Bind 0.0.0.0 for k8s
    app.run(host="0.0.0.0", port=5000)
