import vk_api
import json
import requests

from flask import request
from pymongo import MongoClient
import pymongo
from flask import Flask
import time
from datetime import datetime, date, timedelta
from datetime import time as timed
import collections
import random
import os
from pydub import AudioSegment
from tasks import add
app = Flask(__name__)


# settings
webhook_return_code = "2adc56cf"
webhook_group_id = "169308824"
webhook_group_type = "club"
webhook_group_name = "Статистика Конфы"

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.errorhandler(500)
def handler(e):
    return 'ok'

@app.route('/vk_webhook', methods=['POST', 'GET'])
def register_webhook():
    content = request.json

    if content["type"] == "confirmation":
        return webhook_return_code
    else:
        print("sending task to celery")
        add.delay(content)
        print("sending ok")
        return "ok"


if __name__ == "__main__": 
    app.run(port = 3000) 