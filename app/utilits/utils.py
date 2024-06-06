
import json
import os
from datetime import datetime

import requests
from decouple import config


class ODKClient:
    def __init__(self):
        if os.path.exists("./settings.json"):
            with open("./settings.json", 'r') as file:
                self.odk_settings = json.load(file)
            
            self.odk_default_project_id = config("DEFAULT_PROJECT_ID", cast=int) 
            self.odk_base_url = config("ODK_API_URL", cast=str) 
            self.odk_api_version = config("ODK_API_VERSION", cast=str, default="v1") 
            self.odk_username = config("ODK_USERNAME", cast=str) 
            self.odk_password = config("ODK_PASSWORD", cast=str) 
            self.session = requests.Session()
        else:
            raise FileNotFoundError("settings.json file not found")

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, exc_tb):
        if isinstance(exc_value, IndexError):
            print(f"An exception occurred: {exc_type}")
            print(f"Exception: {exc_value}")
            return True
        

    def send_request(self, method, url,  stream=None, headers = None, **kwargs):
        
        session_object = self.odk_authenticate()
        
        if not session_object:
            return requests.Response(status_code=404 ,content="Cannot authenticate to the remote server")

        if headers:
            self.session.headers.update(headers)

        self.session.headers.update({'Authorization': f'Bearer {session_object["token"]}'})

        response = self.session.request(method, url, stream=stream, **kwargs)

        # If unauthorized, recheck cached session key before continuing
        if response.status_code == 401:
            if os.path.exists("session.json"):
                os.remove("session.json")
                
                session_object = self.odk_authenticate()
                if session_object and "token" in session_object:
                    self.session.headers.update({'Authorization': f'Bearer {session_object["token"]}'})
                    response = self.session.request(method, url, **kwargs)
                else:
                    return requests.Response(status_code=404 ,content="Cannot authenticate to the remote server")
        return response

    def odk_authenticate(self, headers=None, session_cache=None, clear_cache = False):
        headers = headers if headers else {
            "Content-Type":'application/json'
        }
        session_cache = session_cache if session_cache else "session.json"
        
        if clear_cache and os.path.exists(session_cache):
            os.remove(session_cache)

        try:
            with open(session_cache, 'r') as file:
                session_data = json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            session_data = None

        if session_data and 'expireAt' in session_data:
            now = datetime.now()
            expire = datetime.strptime(session_data['expireAt'], "%Y-%m-%dT%H:%M:%SZ")

            if now > expire:
                session_data = None

        if not session_data:
            user = json.dumps({
                "email":self.odk_username,
                "password":self.odk_password
            })

            url = f"{self.odk_base_url}/{self.odk_api_version}/sessions"

            response = requests.post(url, data=user, headers=headers)

            if response.status_code == 200 or response.status_code == 201:
                session_data = json.loads(response.text)
                with open(session_cache, 'w') as file:
                    json.dump(session_data, file)
            if response.status_code == 403 or response.status_code == 401:
                response = requests.post(url, data=user, headers=headers)

                if response.status_code == 200 or response.status_code == 201:
                    session_data = json.loads(response.text)
                    with open(session_cache, 'w') as file:
                        json.dump(session_data, file)
        return session_data
    
    def getFormSubmissions(
            self, 
            start_date=None, 
            end_date=None, 
            skip=None, 
            top=None):  
        headers = {
            "Content-Type":'application/json',
            "X-Extended-Metadata": "true"
        }
        
        pagination_string = f"&$skip={skip}" if skip else "" 
        pagination_string += f"&$top={top}" if top else ""

        start_date = start_date if start_date else ""
        end_date = end_date if end_date else ""


        date_filter = ""
        if(len(start_date) > 0 and len(end_date) > 0):
            date_filter += f'&$filter=__system/submissionDate ge {start_date} and __system/submissionDate le {end_date}'

        if len(start_date) > 0 and len(end_date) == 0:
            date_filter = f'&$filter=__system/submissionDate gt {start_date}'
        
        if(len(end_date) > 0) and len(start_date) == 0:
            date_filter = f'&$filter=__system/submissionDate lt {end_date}'
        
        url = f"{self.odk_base_url}/{self.odk_api_version}/projects/{self.odk_default_project_id}/forms/{self.odk_settings['va_tables'][0]['odk_form_id']}.svc/Submissions?$count=true{pagination_string}{date_filter}"
            
        response = self.send_request('get', url, headers=headers)
        if response.status_code == 200 or response.status_code == 201:
            data = json.loads(response.text)
            return data
        else:
            return "Failed to fetch form submissions!"            
        return "Failed to fetch form submissions!"