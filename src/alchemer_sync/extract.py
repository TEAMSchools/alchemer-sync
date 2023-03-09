import gzip
import json
import os
import pathlib
import traceback

import alchemer
import pendulum
from google.cloud import storage


def save_json(data, file_dir, file_name):
    file_dir.mkdir(parents=True, exist_ok=True)
    file_path = file_dir / file_name

    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        json.dump(data, f)

    return file_path


def upload_to_gcs(bucket, schema, file_path):
    parts = file_path.parts
    blob = bucket.blob(f"{schema}/{'/'.join(parts[parts.index('data') + 1:])}")
    blob.upload_from_filename(file_path)
    return blob


def main():
    alchemer_timezone = os.getenv("ALCHEMER_TIMEZONE")

    gcs_storage_client = storage.Client()
    gcs_bucket = gcs_storage_client.bucket(os.getenv("GCS_BUCKET_NAME"))

    data_dir = pathlib.Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)

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
        api_version=os.getenv("ALCHEMER_API_VERSION"),
        api_token=os.getenv("ALCHEMER_API_TOKEN"),
        api_token_secret=os.getenv("ALCHEMER_API_TOKEN_SECRET"),
        time_zone=alchemer_timezone,
    )

    print("Getting list of surveys...\n")
    survey_list = alchemer_client.survey.list()
    for s in survey_list:
        modified_on = pendulum.parse(text=s.get("modified_on"), tz=alchemer_timezone)

        bookmark = state.get(s.get("id"))
        if bookmark is not None:
            start_datetime = pendulum.parse(bookmark, tz=alchemer_timezone)
        else:
            start_datetime = pendulum.from_timestamp(0, tz=alchemer_timezone)

        start_str = start_datetime.format("YYYY-MM-DD HH:mm:ss Z")

        end_datetime = pendulum.now(tz=alchemer_timezone).subtract(hours=1)
        end_str = end_datetime.format("YYYY-MM-DD HH:mm:ss Z")

        ay_start_datetime = pendulum.datetime(
            year=int(os.getenv("CURRENT_ACADEMIC_YEAR")),
            month=7,
            day=1,
            tz=alchemer_timezone,
        )
        ay_start_str = ay_start_datetime.format("YYYY-MM-DD HH:mm:ss Z")

        # skip unmodified archived surveys that haven't been downloaded
        if s.get("status") == "Archived" and start_datetime > modified_on:
            continue

        # get survey object
        survey = alchemer_client.survey.get(s.get("id"))

        if survey.statistics is None:
            survey.statistics = {}
        total_count = sum([int(v) for v in survey.statistics.values()])
        dq_count = survey.statistics.get("Disqualified", 0)

        print(f"{survey.title}\nStart:\t{start_datetime}\nEnd:\t{end_datetime}")

        # survey metadata
        endpoint = "survey"
        print(f"\n{survey.id} - {endpoint}...")

        file_path = save_json(
            data=survey.data,
            file_dir=data_dir / endpoint,
            file_name=f"{survey.id}.json.gz",
        )
        print(f"\tSaved to {file_path}")

        blob = upload_to_gcs(gcs_bucket, "surveygizmo", file_path)
        print(f"\tUploaded to {blob.public_url}")

        # survey_question
        endpoint = "survey_question"
        print(f"\n{survey.id} - {endpoint}...")

        sq_list = survey.question.list()
        print(f"\tFound {len(sq_list)} records!")

        # add survey_id
        sq_list = [{**sq, "survey_id": survey.id} for sq in sq_list]

        file_path = save_json(
            data=sq_list,
            file_dir=data_dir / endpoint,
            file_name=f"{survey.id}.json.gz",
        )
        print(f"\tSaved to {file_path}")

        blob = upload_to_gcs(gcs_bucket, "surveygizmo", file_path)
        print(f"\tUploaded to {blob.public_url}")

        # survey_campaign
        endpoint = "survey_campaign"
        print(f"\n{survey.id} - {endpoint}...")

        sc_list = survey.campaign.list()
        print(f"\tFound {len(sc_list)} records!")

        # add survey_id
        sc_list = [{**sc, "survey_id": survey.id} for sc in sc_list]

        file_path = save_json(
            data=sc_list,
            file_dir=data_dir / endpoint,
            file_name=f"{survey.id}.json.gz",
        )
        print(f"\tSaved to {file_path}")

        blob = upload_to_gcs(gcs_bucket, "surveygizmo", file_path)
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
                print(f"\tFound {len(dq_list)} records!")

                # add survey_id
                dq_list = [{**dq, "survey_id": survey.id} for dq in dq_list]

                file_path = save_json(
                    data=dq_list,
                    file_dir=data_dir / endpoint,
                    file_name=f"{survey.id}_{ay_start_str}.json.gz",
                )
                print(f"\tSaved to {file_path}")

                blob = upload_to_gcs(gcs_bucket, "surveygizmo", file_path)
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
                print(f"\tFound {len(sr_list)} records!")

                # add survey_id
                sr_list = [{**sr, "survey_id": survey.id} for sr in sr_list]

                # `survey_data` needs to be transformed into list of dicts
                for sr in sr_list:
                    survey_response_data = sr.get("survey_data")
                    if isinstance(survey_response_data, dict):
                        survey_response_data_list = []

                        for k, v in survey_response_data.items():
                            survey_response_data_list.append(v)

                        sr["survey_data_list"] = survey_response_data_list
                    else:
                        sr["survey_data_list"] = survey_response_data

                # `options` needs to be transformed into list of dicts
                for sr in sr_list:
                    for q in sr["survey_data_list"]:
                        options = q.get("options")
                        if options:
                            q["options_list"] = [v for k, v in options.items()]

                file_path = save_json(
                    data=sr_list,
                    file_dir=data_dir / endpoint,
                    file_name=f"{survey.id}_{start_datetime.int_timestamp}.json.gz",
                )
                print(f"\tSaved to {file_path}")

                blob = upload_to_gcs(gcs_bucket, "surveygizmo", file_path)
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
