# worker/worker.py
import os, json, pathlib, subprocess, sys, time
import redis
from minio import Minio

print("worker start")

# Config
REDIS_HOST   = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT   = int(os.getenv("REDIS_PORT", "6379"))
WORK_QUEUE   = os.getenv("WORK_QUEUE", "toWorker")

MINIO_HOST   = os.getenv("MINIO_HOST", "127.0.0.1:9000")   # <— was 'minio:9000'
MINIO_USER   = os.getenv("MINIO_USER", "rootuser")
MINIO_PASSWD = os.getenv("MINIO_PASSWD", "rootpass123")
INPUT_BUCKET = os.getenv("INPUT_BUCKET", "queue")
OUTPUT_BUCKET= os.getenv("OUTPUT_BUCKET", "output")

# Local writable data dir (NOT /data when running on macOS)
DATA_DIR = pathlib.Path(os.getenv("DATA_DIR", "./local_data")).resolve()
IN_DIR   = (DATA_DIR / "input");  IN_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR  = (DATA_DIR / "output"); OUT_DIR.mkdir(parents=True, exist_ok=True)

# Clients
r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)
m = Minio(
    MINIO_HOST,
    access_key=MINIO_USER,
    secret_key=MINIO_PASSWD,
    secure=False
)

def demucs(in_mp3: pathlib.Path, out_dir: pathlib.Path, model: str = "mdx_extra_q"):
    cmd = [
        "python3","-m","demucs",
        "-d","cpu",
        "-n", model,
        "--out", str(out_dir),
        "--mp3",
        str(in_mp3),
    ]
    print("RUN:", " ".join(cmd), flush=True)
    res = subprocess.run(cmd, text=True, capture_output=True)
    if res.returncode != 0:
        print(res.stdout); print(res.stderr, file=sys.stderr)
        raise RuntimeError("demucs failed")

while True:
    # Blocking pop (returns (queue, bytes))
    _, raw = r.blpop(WORK_QUEUE, timeout=0)
    try:
        job = json.loads(raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw)
    except Exception as e:
        print(f"bad job payload: {raw!r} err={e}")
        continue

    songhash = job.get("hash")
    model    = job.get("model", "mdx_extra_q")
    if not songhash:
        print(f"missing 'hash' in job: {job}")
        continue

    print(f"processing {songhash} model={model}")

    # 1) download <hash>.mp3 from INPUT_BUCKET
    in_mp3 = IN_DIR / f"{songhash}.mp3"
    try:
        m.fget_object(INPUT_BUCKET, f"{songhash}.mp3", str(in_mp3))
    except Exception as e:
        print(f"failed to fetch input {songhash}.mp3: {e}")
        continue

    # 2) run demucs
    try:
        demucs(in_mp3, OUT_DIR, model=model)
    except Exception as e:
        print(f"demucs error for {songhash}: {e}")
        continue

    # 3) upload results: OUT_DIR/<model>/<hash>/{vocals,drums,bass,other}.mp3
    out_base = OUT_DIR / model / songhash
    parts = ["vocals","drums","bass","other"]
    for p in parts:
        src = out_base / f"{p}.mp3"
        if not src.exists():
            print(f"missing {src}, skipping")
            continue
        dest = f"{songhash}-{p}.mp3"
        try:
            m.fput_object(OUTPUT_BUCKET, dest, str(src), content_type="audio/mpeg")
            print(f"uploaded {dest}")
        except Exception as e:
            print(f"upload failed {dest}: {e}")
