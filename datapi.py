from flask import Flask, request, make_response
from pathlib import Path
import os
import json
import pandas as pd

app = Flask(__name__)
BASE_DIR = Path(__file__).resolve().parent


@app.route('/transform', methods=['POST'])
def download_data():
    data_path = os.path.join(BASE_DIR, "transform.jsonl")

    if request.method=='POST':
        byte_data = request.get_data()
        json_data = json.loads(byte_data.decode('utf-8'))
        print(json_data)
        return 'Data received', 200

    else:
        return 'Method Not Allowed', 405


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
