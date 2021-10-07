import requests


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
        id = kwargs.pop("id", "")
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

    def router(self, name, id):
        if id:
            if name == "survey":
                return Survey(self, id)
            elif name == "account":
                return Account(self, id)
        else:
            return self.get_object_data(object_name=name)

    def survey(self, id=None):
        return self.router("survey", id)

    def account(self, id=None):
        return self.router("account", id)


class AlchemerObject(object):
    def __init__(self, session, name, id):
        self.__name__ = name
        self._session = session
        self.id = id
        self._session.base_url = f"{self._session.base_url}/{self.__name__}/{self.id}"

        data = next(
            iter(self._session.get_object_data(object_name=self.__name__, id=id)),
            {},
        )
        for k, v in data.items():
            setattr(self, k, v)

    def _subobject(self, name, id):
        if id:
            return AlchemerObject(self._session, name, id)
        else:
            return self._session.get_object_data(object_name=name)


class Survey(AlchemerObject):
    def __init__(self, session, id):
        super().__init__(session, "survey", id)

    def question(self, id=None):
        return self._subobject("surveyquestion", id)

    def campaign(self, id=None):
        return self._subobject("surveycampaign", id)


class SurveyQuestion(AlchemerObject):
    pass


class Account(AlchemerObject):
    def __init__(self, session, id):
        super().__init__(session, "account", id)

    def teams(self, id=None):
        return super()._subobject("accountteams", id)
