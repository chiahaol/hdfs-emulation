import asyncio
import sys

sys.path.append("..")

from flask import Flask, request, make_response
from edfs.edfs_client import EDFSClient

app = Flask(__name__)

@app.route('/')
def root():
    return app.send_static_file("index.html")

@app.route("/files",  methods=["GET"])
async def get_all_files():
    edfs_client = await EDFSClient.create()
    files = await edfs_client.get_all_files()
    return files

@app.route("/download/<path:file>",  methods=["GET"])
async def download(file):
    print(file)
    edfs_client = await EDFSClient.create()
    data = await edfs_client.get_file(file)
    if not data.get("success"):
         response = make_response("", 404)
    else:
        content = data.get("file")
        response = make_response(content, 200)
        response.mimetype = "text/plain"
    return response

@app.route("/upload",  methods=["GET"])
def upload():
    print("upload")
    return {"success": True}


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)