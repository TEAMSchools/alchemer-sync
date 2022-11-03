import json
import os
import pathlib

import alchemer
import tomli

options_filepath = pathlib.Path("alchemer_sync/options.toml").absolute()
with options_filepath.open("rb") as f:
    survey_config = tomli.load(f)

contacts_filepath = pathlib.Path(os.getenv("CONTACTS_EXTRACT_FILEPATH")).absolute()
with contacts_filepath.open("r") as f:
    contacts = json.load(f)

active_contacts = [c for c in contacts if c["Status"] != "TERMINATED"]
terminated_contacts = [c for c in contacts if c["Status"] == "TERMINATED"]

options_active = [
    f"{c['Last Name']}, {c['First Name']} - {c['Division']} [{c['Employee Number']}]".replace(
        "'", ""
    )
    for c in active_contacts
]
options_terminated = [
    (
        f"{c['Last Name']}, "
        f"{c['First Name']} - "
        f"{c['Division']} "
        f"[{c['Employee Number']}]"
    ).replace("'", "")
    for c in terminated_contacts
]

alchemer_client = alchemer.AlchemerSession(
    api_version=os.getenv("ALCHEMER_API_VERSION"),
    api_token=os.getenv("ALCHEMER_API_TOKEN"),
    api_token_secret=os.getenv("ALCHEMER_API_TOKEN_SECRET"),
    time_zone=os.getenv("ALCHEMER_TIMEZONE"),
)

for s in survey_config["surveys"]:
    survey = alchemer_client.survey.get(s["id"])
    print(survey.title)

    for qid in s["question_ids"]:
        question = survey.question.get(qid)
        print(question.shortname)

        options = question.option.list()

        print("\tCREATE new options")
        unmatched_options = []
        for oa in options_active:
            if not [opt for opt in options if opt["value"] == oa]:
                unmatched_options.append(oa)

        for uopt in unmatched_options:
            print(f"\t\t{uopt}")
            try:
                question.option.create(params={"title": uopt, "value": uopt})
            except Exception as xc:
                print(xc)

        print("\tDELETE terminated options")
        terminated_options = []
        for opt in options:
            if [ot for ot in options_terminated if ot == opt["value"]]:
                terminated_options.append(opt)

        for topt in terminated_options:
            print(f"\t\t{topt['value']}")
            try:
                question.option.delete(id=topt["id"])
            except Exception as xc:
                print(xc)

        print("\tDELETE invalid options")
        mismatched_options = []
        for opt in options:
            if not [oa for oa in options_active if oa == opt["value"]]:
                mismatched_options.append(opt)

        for mopt in mismatched_options:
            print(f"\t\t{mopt['value']}")
            try:
                question.option.delete(id=mopt["id"])
            except Exception as xc:
                print(xc)
