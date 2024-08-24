import json
import os
from datetime import datetime

import httpx

from app.odk.utils.data_transform import flattenTranslations, xml_to_json
from app.settings.models.settings import OdkConfigModel


class ODKClientAsync:
    def __init__(self, config: OdkConfigModel):
        self.odk_default_project_id = config.project_id 
        self.odk_base_url = config.url
        self.odk_api_version = config.api_version
        self.odk_username = config.username
        self.odk_password = config.password
        self.odk_form_id = config.form_id
        self.is_sort_allowed = config.is_sort_allowed

    async def __aenter__(self):
        self.client = httpx.AsyncClient()
        return self
    
    async def __aexit__(self, exc_type, exc_value, exc_tb):
        await self.client.aclose()
        if isinstance(exc_value, IndexError):
            print(f"An exception occurred: {exc_type}")
            print(f"Exception: {exc_value}")
            return True

    async def send_request(self, method, url, headers=None, **kwargs):
        session_object = await self.odk_authenticate()
        
        
        if not session_object:
            return httpx.Response(status_code=404, content="Cannot authenticate to the remote server")

        if headers:
            self.client.headers.update(headers)

        self.client.headers.update({'Authorization': f'Bearer {session_object["token"]}'})


        response = await self.client.request(method, url, **kwargs)
        # If unauthorized, recheck cached session key before continuing
        if response.status_code == 401:
            if os.path.exists("session.json"):
                os.remove("session.json")
                
                session_object = await self.odk_authenticate()
                if session_object and "token" in session_object:
                    self.client.headers.update({'Authorization': f'Bearer {session_object["token"]}'})
                    response = await self.client.request(method, url, **kwargs)
                else:
                    return httpx.Response(status_code=404, content="Cannot authenticate to the remote server")
        return response

    async def odk_authenticate(self, headers=None, session_cache=None, clear_cache=False):
        headers = headers if headers else {"Content-Type": 'application/json'}
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
            user = json.dumps({"email": self.odk_username, "password": self.odk_password})
            url = f"{self.odk_base_url}/{self.odk_api_version}/sessions"

            async with httpx.AsyncClient() as client:
                response = await client.post(url, data=user, headers=headers)

                if response.status_code in {200, 201}:
                    session_data = response.json()
                    with open(session_cache, 'w') as file:
                        json.dump(session_data, file)
                if response.status_code in {403, 401}:
                    response = await client.post(url, data=user, headers=headers)

                    if response.status_code in {200, 201}:
                        session_data = response.json()
                        with open(session_cache, 'w') as file:
                            json.dump(session_data, file)
        return session_data
    
    async def getFormSubmissions(self, start_date=None, end_date=None, skip: int = None, top: int = None, order_by: str = None, order_direction: str = 'asc'):
        headers = {
            "Content-Type": 'application/json',
            "X-Extended-Metadata": "true"
        }
        
        pagination_string = f"&$skip={skip}" if skip else "" 
        pagination_string += f"&$top={top}" if top else ""

        start_date = start_date if start_date else ""
        end_date = end_date if end_date else ""

        filter = ""
        if len(start_date) > 0 and len(end_date) > 0:
            filter += f'&$filter=__system/submissionDate ge {start_date} and __system/submissionDate le {end_date}'

        if len(start_date) > 0 and len(end_date) == 0:
            filter = f'&$filter=__system/submissionDate ge {start_date}'
        
        if len(end_date) > 0 and len(start_date) == 0:
            filter = f'&$filter=__system/submissionDate le {end_date}'
        
        if order_by and self.is_sort_allowed:
            if order_direction not in ("asc", "desc"):
                order_direction = "asc"
            filter += f'&$orderby={order_by} {order_direction}'
        
        url = f"{self.odk_base_url}/{self.odk_api_version}/projects/{self.odk_default_project_id}/forms/{self.odk_form_id}.svc/Submissions?$count=true{pagination_string}{filter}"
        
        response = await self.send_request('get', url, headers=headers)
        if response.status_code in {200, 201}:
            return response.json()
        else:
            # Raise an exception with the response text as the error message
            raise Exception(f"Failed to fetch form submissions: {response.text}")




    async def getFormQuestions(self):  
        
        headers = {
            "Content-Type":'application/json'
        }
        
        url = f'{self.odk_base_url}/{self.odk_api_version}/projects/{self.odk_default_project_id}/forms/{self.odk_form_id}.xml'
        
        response = await self.send_request('get', url, headers=headers)
        
        if response.status_code in { 200, 201}:
            json_data = xml_to_json(response.text)
            questions = json_data['h:html']['h:head']['model']['itext']['translation']
            fields = questions['text'] if 'text' in questions else flattenTranslations(questions, 'text')
            
            return fields
        else:
            return json.loads(response.text)

    async def getFormFields(self):
        headers = {
            "Content-Type":'application/json'
        }
        
        url = f'{self.odk_base_url}/{self.odk_api_version}/projects/{self.odk_default_project_id}/forms/{self.odk_form_id}/fields?odata=true'
        
        response = await self.send_request('get', url, headers=headers)
        
        return json.loads(response.text)

