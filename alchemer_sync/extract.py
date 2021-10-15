import gzip
import json
import os
import pathlib
import traceback
from datetime import datetime, timedelta

import alchemer
from dateutil import parser, tz
from dotenv import load_dotenv
from google.cloud import storage

from datarobot.utilities import email

load_dotenv()

ALCHEMER_API_VERSION = os.getenv("ALCHEMER_API_VERSION")
ALCHEMER_API_TOKEN = os.getenv("ALCHEMER_API_TOKEN")
ALCHEMER_API_TOKEN_SECRET = os.getenv("ALCHEMER_API_TOKEN_SECRET")
ALCHEMER_TIMEZONE = os.getenv("ALCHEMER_TIMEZONE")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_SCHEMA_NAME = os.getenv("GCS_SCHEMA_NAME")
CURRENT_ACADEMIC_YEAR = int(os.getenv("CURRENT_ACADEMIC_YEAR"))

PROJECT_PATH = pathlib.Path(__file__).absolute().parent
TZINFO = tz.gettz(ALCHEMER_TIMEZONE)


def to_json(data, file_name):
    file_path = PROJECT_PATH / "data" / file_name
    if not file_path.parent.exists():
        file_path.parent.mkdir(parents=True)

    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        json.dump(data, f)

    return file_path


def upload_to_gcs(bucket, schema, file_path):
    parts = file_path.parts
    blob = bucket.blob(f"{schema}/{'/'.join(parts[parts.index('data') + 1:])}")
    blob.upload_from_filename(file_path)
    return blob


def main():
    gcs_storage_client = storage.Client()
    gcs_bucket = gcs_storage_client.bucket(GCS_BUCKET_NAME)

    data_dir = PROJECT_PATH / "data"
    if not data_dir.exists():
        data_dir.mkdir(parents=True)

    state_file_path = data_dir / "state.json"
    if not state_file_path.exists():
        state = {}
        with state_file_path.open("a+") as f:
            json.dump(state, f)
    else:
        with state_file_path.open("r+") as f:
            state = json.load(f)

    print("Authenticating client...")
    alchemer_client = alchemer.AlchemerSession(
        api_version=ALCHEMER_API_VERSION,
        api_token=ALCHEMER_API_TOKEN,
        api_token_secret=ALCHEMER_API_TOKEN_SECRET,
        time_zone=ALCHEMER_TIMEZONE,
    )

    print("Getting list of surveys...\n")
    survey_list = alchemer_client.survey.list()
    for s in survey_list:
        bookmark = state.get(s.get("id")) or "1970-01-01T00:00:00"
        modified_on = parser.parse(s.get("modified_on")).replace(tzinfo=TZINFO)

        start_datetime = parser.parse(bookmark).replace(tzinfo=TZINFO)
        start_str = start_datetime.strftime("%Y-%m-%d %H:%M:%S %Z")
        start_timestamp_str = str(start_datetime.timestamp()).replace(".", "_")

        end_datetime = datetime.now(tz=TZINFO) - timedelta(hours=1)
        end_str = end_datetime.strftime("%Y-%m-%d %H:%M:%S %Z")

        ay_start_datetime = datetime(
            year=CURRENT_ACADEMIC_YEAR, month=7, day=1, tzinfo=TZINFO
        )
        ay_start_str = ay_start_datetime.strftime("%Y-%m-%d %H:%M:%S %Z")

        # skip unmodified archived surveys that haven't been downloaded
        if s.get("status") == "Archived" and start_datetime > modified_on:
            continue

        # get survey object
        survey = alchemer_client.survey.get(s.get("id"))

        if survey.statistics is None:
            survey.statistics = {}
        total_count = sum([int(v) for v in survey.statistics.values()])
        dq_count = survey.statistics.get("Disqualified", 0)

        print(f"{survey.title}\nStart:\t{start_str}\nEnd:\t{end_str}")

        # survey metadata
        endpoint = "survey"
        print(f"\n{survey.id} - {endpoint}...")

        file_name = f"{endpoint}/{survey.id}.json.gz"
        file_path = to_json(survey.data, file_name)
        print(f"\tSaved to {file_path}")

        blob = upload_to_gcs(gcs_bucket, GCS_SCHEMA_NAME, file_path)
        print(f"\tUploaded to {blob.public_url}")

        # survey_question
        endpoint = "survey_question"
        print(f"\n{survey.id} - {endpoint}...")

        sq_list = survey.question.list()
        sq_list = [dict(sq, survey_id=survey.id) for sq in sq_list]
        print(f"\tFound {len(sq_list)} records!")

        file_name = f"{endpoint}/{survey.id}.json.gz"
        file_path = to_json(sq_list, file_name)
        print(f"\tSaved to {file_path}")

        blob = upload_to_gcs(gcs_bucket, GCS_SCHEMA_NAME, file_path)
        print(f"\tUploaded to {blob.public_url}")

        # survey_campaign
        endpoint = "survey_campaign"
        print(f"\n{survey.id} - {endpoint}...")

        sc_list = survey.campaign.list()
        sc_list = [dict(sc, survey_id=survey.id) for sc in sc_list]
        print(f"\tFound {len(sc_list)} records!")

        file_name = f"{endpoint}/{survey.id}.json.gz"
        file_path = to_json(sc_list, file_name)
        print(f"\tSaved to {file_path}")

        blob = upload_to_gcs(gcs_bucket, GCS_SCHEMA_NAME, file_path)
        print(f"\tUploaded to {blob.public_url}")

        # survey_response DQs
        if dq_count > 0:
            dq_list = (
                survey.response.filter("status", "=", "Disqualified")
                .filter("date_submitted", ">=", ay_start_str)
                .filter("date_submitted", "<", end_str)
                .list()
            )

            if dq_list:
                endpoint = "survey_response_disqualified"
                print(f"\n{survey.id} - {endpoint}...")

                dq_list = [dict(dq, survey_id=survey.id) for dq in dq_list]
                print(f"\tFound {len(dq_list)} records!")

                file_name = f"{endpoint}/{survey.id}_{ay_start_str}.json.gz"
                file_path = to_json(dq_list, file_name)
                print(f"\tSaved to {file_path}")

                blob = upload_to_gcs(gcs_bucket, GCS_SCHEMA_NAME, file_path)
                print(f"\tUploaded to {blob.public_url}")

        # survey_response
        if total_count > 0:
            sr_list = (
                survey.response.filter("date_submitted", ">=", start_str)
                .filter("date_submitted", "<", end_str)
                .list()
            )

            if sr_list:
                endpoint = "survey_response"
                print(f"\n{survey.id} - {endpoint}...")

                sr_list = [dict(sr, survey_id=survey.id) for sr in sr_list]

                # `survey_data` needs to be transformed into list of dicts
                sr_list = [
                    dict(
                        sr,
                        survey_data_list=[v for k, v in sr.get("survey_data").items()],
                    )
                    for sr in sr_list
                ]

                # `options` needs to be transformed into list of dicts
                for sr in sr_list:
                    for q in sr["survey_data_list"]:
                        options = q.get("options")
                        if options:
                            q["options_list"] = [v for k, v in options.items()]

                print(f"\tFound {len(sr_list)} records!")

                file_name = f"{endpoint}/{survey.id}_{start_timestamp_str}.json.gz"
                file_path = to_json(sr_list, file_name)
                print(f"\tSaved to {file_path}")

                blob = upload_to_gcs(gcs_bucket, GCS_SCHEMA_NAME, file_path)
                print(f"\tUploaded to {blob.public_url}")

        # update bookmark
        state[survey.id] = end_datetime.isoformat()
        print()

    # save state
    with state_file_path.open("r+") as f:
        f.seek(0)
        json.dump(state, f)
        f.truncate()


if __name__ == "__main__":
    try:
        main()
    except Exception as xc:
        print(xc)
        print(traceback.format_exc())
        email_subject = "Alchemer Extract Error"
        email_body = f"{xc}\n\n{traceback.format_exc()}"
        email.send_email(subject=email_subject, body=email_body)
