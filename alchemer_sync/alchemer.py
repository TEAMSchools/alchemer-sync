import requests

# DEFAULT_RESULTS_PER_PAGE = 50


class AlchemerSession(requests.Session):
    def __init__(self, api_version, api_token, api_token_secret, auth_method="api_key"):
        self.api_version = api_version
        self.base_url = f"https://api.alchemer.com/{self.api_version}"

        if auth_method == "api_key":
            self.base_params = {
                "api_token": api_token,
                "api_token_secret": api_token_secret,
            }
        elif auth_method == "oauth":
            pass

        super(AlchemerSession, self).__init__()

    def request(self, method, url, *args, **kwargs):
        url = f"{self.base_url}/{url}"
        self.params.update(self.base_params)
        return super(AlchemerSession, self).request(method, url, *args, **kwargs)

    def get_object(self, object_name, params={}):
        try:
            r = self.get(object_name, params=params)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError:
            raise requests.exceptions.HTTPError(r.status_code, r.reason)
        else:
            raise xc

    def get_all_data(self, object_name, params={}):
        all_data = []
        while True:
            r = self.get_object(object_name, params)
            
            page = r.get("page", 1)
            total_pages = r.get("total_pages", 1)
            data = r.get("data")
            if type(data) == list:
                all_data.extend(r.get("data"))
            elif type(data) == dict:
                all_data.append(r.get("data"))
            
            if page == total_pages:
                break

        return all_data


# """
import os
from dotenv import load_dotenv

load_dotenv()
ALCHEMER_API_VERSION = os.getenv("ALCHEMER_API_VERSION")
ALCHEMER_API_TOKEN = os.getenv("ALCHEMER_API_TOKEN")
ALCHEMER_API_TOKEN_SECRET = os.getenv("ALCHEMER_API_TOKEN_SECRET")


alchemer = AlchemerSession(
    ALCHEMER_API_VERSION, ALCHEMER_API_TOKEN, ALCHEMER_API_TOKEN_SECRET
)
print()
# """