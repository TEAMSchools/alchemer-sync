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

load_dotenv()

ALCHEMER_API_VERSION = os.getenv("ALCHEMER_API_VERSION")
ALCHEMER_API_TOKEN = os.getenv("ALCHEMER_API_TOKEN")
ALCHEMER_API_TOKEN_SECRET = os.getenv("ALCHEMER_API_TOKEN_SECRET")
ALCHEMER_TIMEZONE = os.getenv("ALCHEMER_TIMEZONE")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_SCHEMA_NAME = os.getenv("GCS_SCHEMA_NAME")
RESULTS_PER_PAGE = os.getenv("RESULTS_PER_PAGE")

PROJECT_PATH = pathlib.Path(__file__).absolute().parent


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
    # blob.upload_from_filename(file_path)
    return blob


def main():
    gcs_storage_client = storage.Client()
    gcs_bucket = gcs_storage_client.bucket(GCS_BUCKET_NAME)

    data_dir = PROJECT_PATH / "data"
    if not data_dir.exists():
        data_dir.mkdir(parents=True)

    state_file_path = data_dir / "state.json"
    if not state_file_path.exists():
        state = {"bookmarks": {}}
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
        # load last run time from state and save current run time
        bookmark = state["bookmarks"].get(s.get("id")) or "1970-01-01T00:00:00"

        alchemer_tz = tz.gettz(ALCHEMER_TIMEZONE)
        start_datetime = parser.parse(bookmark).replace(tzinfo=alchemer_tz)
        start_datetime_str = start_datetime.strftime("%Y-%m-%d %H:%M:%S %Z")
        end_datetime = datetime.now(tz=alchemer_tz) - timedelta(hours=1)
        end_datetime_str = end_datetime.strftime("%Y-%m-%d %H:%M:%S %Z")
        end_timestamp_str = str(end_datetime.timestamp()).replace(".", "_")

        # skip unmodified archived surveys that haven't been downloaded
        if s.get("status") == "Archived" and start_datetime > s.get("modified_on"):
            continue

        # get survey object
        survey = alchemer_client.survey.get(s.get("id"))

        if survey.statistics is None:
            survey.statistics = {}
        total_count = sum([int(v) for v in survey.statistics.values()])
        dq_count = survey.statistics.get("Disqualified", 0)

        print(f"{survey.title}\nStart:\t{start_datetime_str}\nEnd:\t{end_datetime_str}")

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

        file_name = f"{endpoint}/{survey.id}.json.gz"
        file_path = to_json(sc_list, file_name)
        print(f"\tSaved to {file_path}")

        blob = upload_to_gcs(gcs_bucket, GCS_SCHEMA_NAME, file_path)
        print(f"\tUploaded to {blob.public_url}")

        # survey_response DQs
        endpoint = "survey_response_disqualified"
        print(f"\n{survey.id} - {endpoint}...")

        if dq_count > 0:
            sr_dq_list = survey.response.filter("status", "=", "Disqualified").list()
            sr_dq_list = [dict(dq, survey_id=survey.id) for dq in sr_dq_list]

            file_name = f"{endpoint}/{survey.id}.json.gz"
            file_path = to_json(sr_dq_list, file_name)
            print(f"\tSaved to {file_path}")

            blob = upload_to_gcs(gcs_bucket, GCS_SCHEMA_NAME, file_path)
            print(f"\tUploaded to {blob.public_url}")

        # survey_response
        endpoint = "survey_response"
        print(f"\n{survey.id} - {endpoint}...")

        if total_count > 0:
            sr_list = (
                survey.response.filter("date_submitted", ">=", start_datetime_str)
                .filter("date_submitted", "<", end_datetime_str)
                .list({"page": 1, "resultsperpage": 10})  # TODO: delete dev params
            )

            if sr_list:
                sr_list = [dict(sr, survey_id=survey.id) for sr in sr_list]

                # `survey_data` needs to be transformed into list of dicts
                sr_list = [
                    dict(
                        sr,
                        survey_data_list=[v for k, v in sr.get("survey_data").items()],
                    )
                    for sr in sr_list
                ]

                file_name = f"{endpoint}/{survey.id}_{end_timestamp_str}.json.gz"
                file_path = to_json(sr_list, file_name)
                print(f"\tSaved to {file_path}")

                blob = upload_to_gcs(gcs_bucket, GCS_SCHEMA_NAME, file_path)
                print(f"\tUploaded to {blob.public_url}")

        # update bookmark
        state["bookmarks"][survey.id] = end_datetime.isoformat()
        print()

    # # save state
    # with state_file_path.open("r+") as f:
    #     f.seek(0)
    #     json.dump(state, f)
    #     f.truncate()


if __name__ == "__main__":
    try:
        main()
    except Exception as xc:
        print(xc)
        print(traceback.format_exc())
