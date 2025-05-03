import requests

something = requests.get("https://starbase.nerdpg.live/api/json/roadClosures")
print(something.content)