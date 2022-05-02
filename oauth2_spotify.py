from urllib.parse import urlencode
import json
import requests
from flask import Flask
from flask import redirect
from flask import request
# make sure you install flask pip install flask
app = Flask(__name__)

def load_credentials():
    try:
        with open('credentials.json', 'r') as f:
            data = json.load(f)
            return data
    except Exception as e:
        print("There was an error loading the credentials. Contact MBD Team D Section 2 for support.")

def update_credentials(field, client_id, client_secret):
    try:
        with open('credentials.json', 'w') as f:
            data = {}
            data['client_id'] = client_id
            data['client_secret'] = client_secret
            data['token'] = field
            f.write(json.dumps(data))
            return 1
    except e as Exception:
        print("There was an error loading the credentials. Contact MBD Team D Section 2 for support.")


host = '0.0.0.0'
port = 8088
credentials = load_credentials()
client_id = credentials['client_id']
client_secret = credentials['client_secret']
SHOW_DIALOG_bool = True
SHOW_DIALOG_str = str(SHOW_DIALOG_bool).lower()

@app.route("/modern-data")
def authorize():
    # Redirection to the spotify web portal to authorize
    auth_url = 'https://accounts.spotify.com/authorize'
    data = urlencode({"response_type": "code", "redirect_uri": 'http://{}:{}/redirect/modern-data-team-d'.format(host,port),"scope": 'playlist-modify-public playlist-modify-private user-read-playback-state user-modify-playback-state user-read-currently-playing user-read-playback-state', "client_id": client_id})
    lookup_url = f'{auth_url}?{data}'
    print(lookup_url)
    return redirect(lookup_url)

@app.route("/redirect/modern-data-team-d")
def get_access_token():
    token = request.args['code']
    data = {
        "grant_type": "authorization_code",
        "code":'{}'.format(token),
        "redirect_uri": 'http://{}:{}/redirect/modern-data-team-d'.format(host,port),
        'client_id': client_id,
        'client_secret': client_secret
     }
    token_url = 'https://accounts.spotify.com/api/token'
    r = requests.post(token_url, data=data)
    response_json = json.loads(r.text)
    print(response_json)
    access_token = response_json["access_token"]
    update_credentials(access_token, client_id, client_secret)
    return "Access Token: {}".format(access_token)

if __name__ == '__main__':
    app.run(debug=True, port=port, host="0.0.0.0")
