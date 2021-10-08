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
            self.auth_params = {
                "api_token": api_token,
                "api_token_secret": api_token_secret,
            }
        elif auth_method == "oauth":
            raise NotImplementedError(
                "This library currently only works with 'api_key' authentication"
            )  # TODO: add oauth

        super(AlchemerSession, self).__init__()

    def request(self, method, url, params, *args, **kwargs):
        id = kwargs.pop("id", "")

        url = f"{self.base_url}/{url}/{id}"
        params.update(self.auth_params)
        return super(AlchemerSession, self).request(
            method=method, url=url, params=params, *args, **kwargs
        )

    def _api_call(self, method, object_name, params, id=None):
        try:
            r = self.request(method, url=object_name, id=id, params=params)
            r.raise_for_status()
            return r.json()
        except Exception as xc:
            raise xc

    def _api_get(self, object_name, params, id=None):
        id = id or ""
        all_data = []
        while True:
            r = self._api_call(
                method="GET", object_name=object_name, id=id, params=params
            )

            page = r.get("page", 1)
            total_pages = r.get("total_pages", 1)
            data = r.get("data")

            if type(data) == list:
                all_data.extend(r.get("data"))
            elif type(data) == dict:
                all_data.append(r.get("data"))

            if page == total_pages:
                break

            params.update({"page": page + 1})

        return all_data

    def survey(self, id=None):
        return Survey(session=self, name="survey", id=id)

    def account(self, id=None):
        return Account(session=self, name="account", id=id)

    def account_teams(self, id=None):
        return AccountTeams(session=self, name="accountteams", id=id)

    def account_user(self, id=None):
        return AccountUser(session=self, name="accountuser", id=id)

    def domain(self, id=None):
        return AccountUser(session=self, name="domain", id=id)

    def sso(self, id=None):
        return AccountUser(session=self, name="sso", id=id)

    def survey_theme(self, id=None):
        return AccountUser(session=self, name="surveytheme", id=id)

    def contact_list(self, id=None):
        return AccountUser(session=self, name="contactlist", id=id)

    def contact_custom_field(self, id=None):
        return AccountUser(session=self, name="contactcustomfield", id=id)


class AlchemerObject(object):
    def __init__(self, session, name, id):
        self.__name__ = name
        self._session = session
        self.id = id or ""

    def get(self, params={}):
        if self.id:
            self.__data = self._session._api_get(
                object_name=self.__name__, id=self.id, params=params
            )
            for k, v in self.__data[0].items():
                setattr(self, k, v)
        return self

    def list(self, params={}):
        return self._session._api_get(
            object_name=self.__name__, id=self.id, params=params
        )

    def create(self, params):
        return self._session._api_call(
            method="PUT", object_name=self.__name__, id=self.id, params=params
        )

    def update(self, params):
        return self._session._api_call(
            method="POST", object_name=self.__name__, id=self.id, params=params
        )

    def delete(self):
        return self._session._api_call(
            method="DELETE", object_name=self.__name__, id=self.id, params={}
        )


class Survey(AlchemerObject):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def question(self, id=None):
        return SurveyQuestion(parent=self, name="surveyquestion", id=id)

    # def campaign(self, id=None):
    #     return SurveyCampaign(session=self._session, name="surveycampaign", id=id)


class SurveyQuestion(AlchemerObject):
    """Survey Sub-Object"""

    def __init__(self, **kwargs):
        parent = kwargs.pop("parent")
        parent._session.base_url = (
            f"{parent._session.base_url}/{parent.__name__}/{parent.id}"
        )
        # print(parent._session.base_url)
        super().__init__(session=parent._session, **kwargs)


#     def option(self, id=None):
#         return SurveyQuestionOption(session=self._session, name="surveyoption", id=id)


# class SurveyCampaign(AlchemerObject):
#     """Survey Sub-Object"""

#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)


# class SurveyQuestionOption(AlchemerObject):
#     """Survey Sub-Sub-Object"""

#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)


class Account(AlchemerObject):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class AccountTeams(AlchemerObject):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class AccountUser(AlchemerObject):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class Domain(AlchemerObject):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class SSO(AlchemerObject):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class SurveyTheme(AlchemerObject):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class ContactList(AlchemerObject):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class ContactCustomField(AlchemerObject):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


"""
TODO:
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
