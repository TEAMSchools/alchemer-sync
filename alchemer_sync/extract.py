import json
import math
import os
import pathlib
import traceback
from datetime import datetime, timedelta

import requests
import pandas as pd
import pytz
from dotenv import load_dotenv
from google.cloud import storage
from surveygizmo import SurveyGizmo

load_dotenv()

STATE_FILE = os.getenv("STATE_FILE")
SURVEYGIZMO_TOKEN = os.getenv("SURVEYGIZMO_TOKEN")
SURVEYGIZMO_SECRET = os.getenv("SURVEYGIZMO_SECRET")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

PROJECT_PATH = pathlib.Path(__file__).absolute().parent

DEFAULT_RESULTS_PER_PAGE = 50

gcs_storage_client = storage.Client()
gcs_bucket = gcs_storage_client.bucket(GCS_BUCKET_NAME)


def survey_sort(survey_list_data):
    statistics = survey_list_data.get("statistics")
    if statistics:
        return statistics.get("Complete") or 0
    else:
        return 0


def save_df(df, endpoint, file_name):
    file_dir = PROJECT_PATH / "data" / endpoint
    if not file_dir.exists():
        file_dir.mkdir(parents=True)
        print(f"\tCreated {file_dir}!")

    file_path = file_dir / file_name
    print(f"\tSaving {file_path}...")
    df.to_json(file_path, orient="records")

    # upload to GCS
    destination_blob_name = "surveygizmo/" + "/".join(file_path.parts[-2:])
    blob = gcs_bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)
    print(f"\tUploaded to {destination_blob_name}!")


def get_survey_response_data(
    survey_id, date_submitted_ge, date_submitted_lt, page=0, results_per_page=50
):
    survey_response = (
        client.api.surveyresponse.filter("date_submitted", ">=", date_submitted_ge)
        .filter("date_submitted", "<", date_submitted_lt)
        .list(survey_id, page=page, resultsperpage=results_per_page)
    )
    return survey_response


def get_survey_data(s, last_run_datetime, now_datetime_iso):
    survey_response_metadata = {}

    endpoint = "survey"
    print(f"\n\tGetting {endpoint} data...")
    survey_df = pd.DataFrame([s])
    survey_filename = f"{s['id']}.json"
    save_df(survey_df, endpoint, survey_filename)

    # survey_question
    endpoint = "survey_question"
    print(f"\n\tGetting {endpoint} data...")
    try:
        survey_question = client.api.surveyquestion.list(s["id"])
        survey_question_data = survey_question["data"]
        survey_question_df = pd.DataFrame(survey_question_data)
        survey_question_df["survey_id"] = s["id"]
        save_df(survey_question_df, endpoint, survey_filename)
    except Exception:
        # email.send_email(
        #     f"SurveyGizmo sync error - {s['title']} - {endpoint}",
        #     traceback.format_exc(),
        # )
        pass

    # survey_campaign
    endpoint = "survey_campaign"
    print(f"\n\tGetting {endpoint} data...")
    try:
        survey_campaign = client.api.surveycampaign.list(s["id"])
        survey_campaign_data = survey_campaign["data"]
        survey_campaign_df = pd.DataFrame(survey_campaign_data)
        survey_campaign_df["survey_id"] = s["id"]
        save_df(survey_campaign_df, endpoint, survey_filename)

        # filter for currently open campaigns
        survey_campaign_df["link_open_date_clean"] = pd.to_datetime(
            survey_campaign_df.link_open_date,
            infer_datetime_format=True,
            errors="coerce",
        )
        survey_campaign_df["link_close_date_clean"] = pd.to_datetime(
            survey_campaign_df.link_close_date,
            infer_datetime_format=True,
            errors="coerce",
        )
    except Exception:
        # email.send_email(
        #     f"SurveyGizmo sync error - {s['title']} - {endpoint}",
        #     traceback.format_exc(),
        # )
        pass

    # survey_response DQs
    endpoint = "survey_response_disqualified"
    print(f"\n\tGetting {endpoint} data...")
    try:
        # check number of pages
        survey_response_dq_metadata = client.api.surveyresponse.filter(
            "status", "=", "Disqualified"
        ).list(s["id"], resultsperpage=0)
        total_count = survey_response_dq_metadata.get("total_count") or 0

        # if data exists, loop through pages
        if total_count > 0:
            range_stop = math.ceil(total_count / DEFAULT_RESULTS_PER_PAGE) + 1
            survey_response_dq_data = []
            for p in range(1, range_stop):
                print(f"\t\tPage {p}...")
                survey_response_dq_list = client.api.surveyresponse.filter(
                    "status", "=", "Disqualified"
                ).list(s["id"], page=p)
                survey_response_dq_data.extend(survey_response_dq_list["data"])

            survey_response_dq_df = pd.DataFrame(survey_response_dq_data)
            survey_response_dq_df["survey_id"] = s["id"]
            save_df(survey_response_dq_df, endpoint, survey_filename)
    except Exception:
        # email.send_email(
        #     f"SurveyGizmo sync error - {s['title']} - {endpoint}",
        #     traceback.format_exc(),
        # )
        pass

    # survey_response
    endpoint = "survey_response"
    print(f"\n\tGetting {endpoint} count...")
    try:
        # check number of pages
        survey_response_metadata = get_survey_response_data(
            s["id"], last_run_datetime, now_datetime_iso, results_per_page=0
        )
        print(
            f"\t\t{survey_response_metadata['total_count']} records,",
            f"{survey_response_metadata['total_pages']} pages",
        )
    except Exception:
        # email.send_email(
        #     f"SurveyGizmo sync error - {s['title']} - {endpoint} count",
        #     traceback.format_exc(),
        # )
        return False

    total_count = survey_response_metadata.get("total_count") or 0
    if total_count > 0:
        print(f"\n\tGetting {endpoint} data...")
        try:
            range_stop = math.ceil(total_count / DEFAULT_RESULTS_PER_PAGE) + 1
            survey_response_data = []
            for p in range(1, range_stop):
                print(f"\t\tPage {p}...")
                survey_response_list = get_survey_response_data(
                    s["id"], last_run_datetime, now_datetime_iso, page=p
                )
                survey_response_data.extend(survey_response_list["data"])
        except Exception as e:
            # if 500 error, reduce page size and try again
            print(e)
            print("\t\tReducing page size and retrying...")
            new_page_size = int(int(survey_response_metadata["results_per_page"]) / 2)
            print(
                f"\t\t{survey_response_metadata['total_count']} records,",
                f"{survey_response_metadata['total_pages']} pages",
            )

            # start over with response data
            try:
                range_stop = math.ceil(total_count / new_page_size) + 1
                survey_response_data = []
                for p in range(1, range_stop):
                    print(f"\t\tPage {p}...")
                    survey_response_list = get_survey_response_data(
                        s["id"],
                        last_run_datetime,
                        now_datetime_iso,
                        page=p,
                        results_per_page=new_page_size,
                    )
                    survey_response_data.extend(survey_response_list["data"])
            except Exception:
                # email.send_email(
                #     f"SurveyGizmo sync error - {s['title']} - {endpoint}",
                #     traceback.format_exc(),
                # )
                return False

        if survey_response_data:
            survey_response_df = pd.DataFrame(survey_response_data)
            survey_response_df["survey_id"] = s["id"]

            # `survey_data` needs to be transformed into list of dicts
            survey_response_df["survey_data"] = survey_response_df.survey_data.apply(
                lambda x: [v for k, v in x.items()]
            )

            last_run_str = "".join(c for c in last_run_datetime if c.isdigit())
            response_filename = f"{s['id']}_{last_run_str}.json"
            save_df(survey_response_df, endpoint, response_filename)

    return True


# if state file does not extist, create it, else load
if not os.path.exists(STATE_FILE):
    state = {"bookmarks": {}}
    with open(STATE_FILE, "a+") as f:
        json.dump(state, f)
else:
    with open(STATE_FILE, "r+") as f:
        state = json.load(f)

print("Authenticating client...")
client = SurveyGizmo(
    api_version="v5", api_token=SURVEYGIZMO_TOKEN, api_token_secret=SURVEYGIZMO_SECRET
)

print("Getting list of surveys...")
survey_list = client.api.survey.list()
survey_list_data = survey_list["data"]

survey_list_data = sorted(survey_list_data, key=lambda x: survey_sort(x))

for s in survey_list_data:
    # load last run time from state and save current run time
    last_run_datetime = state["bookmarks"].get(s["id"]) or "1900-01-01 00:00:00 EST"
    now_datetime = datetime.now(pytz.timezone("US/Eastern")) - timedelta(hours=1)
    now_datetime_iso = now_datetime.strftime("%Y-%m-%d %H:%M:%S %Z")

    # pull all data
    print(f"\n{s['title']}\nStart: {last_run_datetime}" f"\nEnd: {now_datetime_iso}")
    result = get_survey_data(s, last_run_datetime, now_datetime_iso)

    # update state if successful
    if result:
        state["bookmarks"][s["id"]] = now_datetime_iso

# save state
with open(STATE_FILE, "r+") as f:
    f.seek(0)
    json.dump(state, f)
    f.truncate()
