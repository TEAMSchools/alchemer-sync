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

    def get_object(self, object_name, id=None, params={}):
        try:
            r = self.get(url=object_name, id=id, params=params)
            r.raise_for_status()
            return r.json()
        except Exception as xc:
            raise xc

    def get_object_data(self, object_name, id=None, params={}):
        id = id or ""
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

    def survey(self, id=None):
        return Survey(session=self, name="survey", id=id)

    def account(self, id=None):
        return Account(session=self, name="account", id=id)

    def account_teams(self, id=None):
        return AccountTeams(session=self, name="accountteams", id=id)

    def account_user(self, id=None):
        return AccountUser(session=self, name="accountuser", id=id)


class AlchemerObject(object):
    def __init__(self, session, name, id):
        self.__name__ = name
        self._session = session
        self.id = id or ""

    def get(self):
        if self.id:
            self.__data = self._session.get_object_data(
                object_name=self.__name__, id=self.id
            )
            for k, v in self.__data[0].items():
                setattr(self, k, v)
        return self

    def list(self):
        return self._session.get_object_data(object_name=self.__name__, id=self.id)


class Survey(AlchemerObject):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def question(self, id=None):
        return SurveyQuestion(session=self._session, name="surveyquestion", id=id)

    def campaign(self, id=None):
        return SurveyCampaign(session=self._session, name="surveycampaign", id=id)


class SurveyQuestion(AlchemerObject):
    """Survey Sub-Object"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def option(self, id=None):
        return SurveyQuestionOption(session=self._session, name="surveyoption", id=id)


class SurveyCampaign(AlchemerObject):
    """Survey Sub-Object"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class SurveyQuestionOption(AlchemerObject):
    """Survey Sub-Sub-Object"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class Account(AlchemerObject):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class AccountTeams(AlchemerObject):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class AccountUser(AlchemerObject):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

"""
TODO:
OBJECTS
Domain Object v5
SSO Object v5
SurveyTheme Object v5
ContactList Object v5
ContactCustomField Object

SUB-OBJECTS:
SurveyPage Sub-Object v5
SurveyQuestion Sub-Object v5
SurveyCampaign Sub-Object v5
SurveyReport Sub-Object v5
SurveyResponse Sub-Object v5
SurveyStatistic Sub-Object v5
Quotas Sub-Object v5
[BETA] Reporting Object
[BETA] Results Object
ContactListContact Sub-Object v5

SUB-SUB-OBJECTS:
SurveyOption Sub-Object v5
SurveyContact Sub-Object v5
EmailMessage Sub-Object v5
[BETA] ReportElement Object
"""
