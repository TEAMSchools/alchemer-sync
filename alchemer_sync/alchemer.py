import requests

# DEFAULT_RESULTS_PER_PAGE = 50


class AlchemerSession(requests.Session):
    def __init__(self, api_version, api_token, api_token_secret, auth_method="api_key"):
        self.api_version = api_version
        self.base_url = f"https://api.alchemer.com/{self.api_version}"

        if api_version != "v5":
            raise NotImplementedError(
                "This library currently only works with v5+"
            )  # TODO: add < v5

        if auth_method == "api_key":
            self.base_params = {
                "api_token": api_token,
                "api_token_secret": api_token_secret,
            }
        elif auth_method == "oauth":
            raise NotImplementedError(
                "This library currently only works with 'api_key' authentication"
            )  # TODO: add oauth

        super(AlchemerSession, self).__init__()

    def request(self, method, url, *args, **kwargs):
        id = kwargs.pop("id")
        url = f"{self.base_url}/{url}/{id}"
        self.params.update(self.base_params)
        return super(AlchemerSession, self).request(
            method=method, url=url, *args, **kwargs
        )

    def get_object(self, object_name, id="", params={}):
        try:
            r = self.get(url=object_name, id=id, params=params)
            r.raise_for_status()
            return r.json()
        except Exception as xc:
            raise xc

    def get_object_data(self, object_name, id="", params={}):
        all_data = []
        while True:
            r = self.get_object(object_name=object_name, id=id, params=params)

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

    def survey(self, id):
        return Survey(self, id)


class Survey(object):
    def __init__(self, session, id):
        self.session = session
        self.id = id

        self.data = self.session.get_object_data(object_name="survey", id=self.id)[0]

    def question(self):
        pass
