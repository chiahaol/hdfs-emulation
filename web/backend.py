import asyncio
import sys

sys.path.append("..")

from flask import Flask
from edfs.edfs_client import EDFSClient

app = Flask(__name__)

@app.route("/files",  methods=["GET"])
async def get_all_files():
    edfs_client = await EDFSClient.create()
    files = await edfs_client.get_all_files()
    return files

@app.route("/download",  methods=["GET"])
def download():
    print("download")
    return {"success": True}

@app.route("/upload",  methods=["GET"])
def upload():
    print("upload")
    return {"success": True}


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)