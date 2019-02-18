# voicebot-vk

HTTPS-server is necessary for this bot (for getting webhook updates)

First, launch mongo server: 
```
mongod
```

Then, run web server: 
```
python3 stats.py
```

And then, run Celery: 
```
export vk_api_key="YOUR GROUP API KEY" 
celery -A tasks worker --loglevel=info
```
