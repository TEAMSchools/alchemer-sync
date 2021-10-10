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
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_SCHEMA_NAME = os.getenv("GCS_SCHEMA_NAME")
LOCAL_TIMEZONE = os.getenv("LOCAL_TIMEZONE")

PROJECT_PATH = pathlib.Path(__file__).absolute().parent


def save_data_file(endpoint, file_name, data):
    file_dir = PROJECT_PATH / "data" / endpoint
    file_path = file_dir / file_name

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
    # gcs_storage_client = storage.Client()
    # gcs_bucket = gcs_storage_client.bucket(GCS_BUCKET_NAME)

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
    alchemer = AlchemerSession(
        ALCHEMER_API_VERSION, ALCHEMER_API_TOKEN, ALCHEMER_API_TOKEN_SECRET
    )

    print("Getting list of surveys...\n")
    survey_list = alchemer.get_object_data("survey")
    for s in survey_list:
        # load last run time from state and save current run time
        bookmark = state["bookmarks"].get(s.get("id"))
        last_run_str = bookmark or "1900-01-01 00:00:00 EST"
        last_run = parser.parse(last_run_str[:19]).replace(
            tzinfo=tz.gettz(LOCAL_TIMEZONE)
        )
        end_datetime = datetime.now(tz=tz.gettz(LOCAL_TIMEZONE)) - timedelta(hours=1)
        end_datetime_str = end_datetime.strftime("%Y-%m-%d %H:%M:%S %Z")
        modified_on = parser.parse(s.get("modified_on")).replace(
            tzinfo=tz.gettz(LOCAL_TIMEZONE)
        )

        # skip unmodified archived surveys that haven't been downloaded
        if s.get("status") == "Archived" and last_run > modified_on:
            continue

        print(f"{s['title']}\nStart: {last_run_str}\nEnd: {end_datetime_str}\n")

        # survey metadata
        endpoint = "survey"
        print(f"\t{endpoint}...")
        
        file_name = f"{s['id']}.json"
        file_path = save_data_file(endpoint, file_name, s)
        print(f"\t\tSaved to {file_path}!")
        
        # blob = upload_to_gcs(gcs_bucket, GCS_SCHEMA_NAME, file_path)
        # print(f"\t\tUploaded to {blob.public_url}!")

        # survey_question
        endpoint = "survey_question"
        print(f"\t{endpoint}...")
        # try:
        #     survey_question = alchemer.get_object_data("surveyquestion", id=s["id"])
        #     survey_question_data = survey_question["data"]
        #     survey_question_df = pd.DataFrame(survey_question_data)
        #     survey_question_df["survey_id"] = s["id"]
        #     save_df(survey_question_df, endpoint, survey_filename)
        # except Exception as xc:
        #     print(xc)
        #     print(traceback.format_exc())

        # # update state if successful
        # if result:
        #     state["bookmarks"][s["id"]] = end_datetime_str

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
