#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Script for downloading GitHub Actions artifacts from 'Build python wheels' workflow."""
import argparse
import itertools
import os
import pprint
import shutil
import sys
import tempfile
import time
import zipfile

import dateutil.parser
import requests

GH_API_URL_WORKLOW_FMT = (
    "https://api.github.com/repos/{repo_url}/actions/workflows/build_wheels.yml"
)
GH_API_URL_WORKFLOW_RUNS_FMT = (
    "https://api.github.com/repos/{repo_url}/actions/workflows/{workflow_id}/runs"
)
GH_WEB_URL_WORKLOW_RUN_FMT = "https://github.com/{repo_url}/actions/runs/{run_id}"


def parse_arguments():
  """
  Gets all neccessary data from the user by parsing arguments or asking for input.
  Return: github_token, user_github_id, repo_url, release_branch, release_commit, artifacts_dir
  """
  parser = argparse.ArgumentParser(
      description=
      "Script for downloading GitHub Actions artifacts from 'Build python wheels' workflow."
  )
  parser.add_argument("--github-user", required=True)
  parser.add_argument("--repo-url", required=True)
  parser.add_argument("--release-branch", required=True)
  parser.add_argument("--release-commit", required=True)
  parser.add_argument("--artifacts_dir", required=True)

  args = parser.parse_args()

  github_token = ask_for_github_token()
  print("You passed following arguments:")
  pprint.pprint({**vars(args), **{"github_token": github_token}})
  if not get_yes_or_no_answer("Do you want to continue?"):
    print("You said NO. Quitting ...")
    sys.exit(1)

  user_github_id = args.github_user
  repo_url = args.repo_url
  release_branch = args.release_branch
  release_commit = args.release_commit
  artifacts_dir = args.artifacts_dir

  return github_token, user_github_id, repo_url, release_branch, release_commit, artifacts_dir


def ask_for_github_token():
  """Ask for github token and print basic information about it"""
  url = "https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token"
  message = (
      f"You need to have a github access token with public_repo scope. "
      f"More info about creating access tokens can be found here {url}")
  print(message)
  github_token = input("Enter github token: ")
  if not github_token:
    return ask_for_github_token()
  return github_token


def request_url(url, github_token, return_json=True, *args, **kwargs):
  """Helper function form making requests authorized by GitHub token"""
  r = requests.get(url, *args, auth=("token", github_token), **kwargs)
  if return_json:
    r.raise_for_status()
    return r.json()
  return r


def safe_get(data, key, url=None):
  """Gets data by the key with informative Exception in case of non existent key."""
  if key not in data:
    message = f'There is missing key: "{key}" in response data: {data}.'
    if url:
      message += f" Requested url: {url}"
    raise ValueError(message)
  return data.get(key)


def get_yes_or_no_answer(question):
  """Asks yes or no question"""
  reply = str(input(question + " 'y' or 'n'): ")).lower().strip()
  if reply == "y":
    return True
  if reply == "n":
    return False
  else:
    return get_yes_or_no_answer("Uhhhh... please enter")


def get_build_wheels_workflow_id(repo_url, github_token):
  """Gets workflow id"""
  url = GH_API_URL_WORKLOW_FMT.format(repo_url=repo_url)
  data = request_url(url, github_token)
  return safe_get(data, "id", url)


def get_last_run(
    workflow_id, repo_url, release_branch, release_commit, github_token):
  """
  Gets data of last run for given repo, branch and commit.
  Raises exception when no run found.
  """
  url = GH_API_URL_WORKFLOW_RUNS_FMT.format(
      repo_url=repo_url, workflow_id=workflow_id)
  data = request_url(
      url,
      github_token,
      params={
          # FIXME change to push
          "event": "pull_request",
          "branch": release_branch
          # "event": "push", "branch": release_branch
      },
  )
  runs = safe_get(data, "workflow_runs", url)

  filtered_commit_runs = [
      r for r in runs if r.get("head_sha", "") == release_commit
  ]
  if not filtered_commit_runs:
    workflow_web_url = GH_API_URL_WORKFLOW_RUNS_FMT.format(
        repo_url=repo_url, workflow_id=workflow_id)
    raise Exception(
        f"No runs for workflow (branch {release_branch}, commit {release_commit}). Verify at {workflow_web_url}"
    )

  sorted_runs = sorted(
      filtered_commit_runs,
      key=lambda w: dateutil.parser.parse(w["created_at"]),
      reverse=True,
  )
  last_run = sorted_runs[0]
  print(
      f"Found last run. SHA: {release_commit}, created_at: '{last_run['created_at']}', id: {last_run['id']}"
  )
  workflow_web_url = GH_WEB_URL_WORKLOW_RUN_FMT.format(
      repo_url=repo_url, run_id=last_run["id"])
  print(f"Verify at {workflow_web_url}")
  print(
      f"Upload to GCS available at:\n"
      f"\tgs://beam-wheels-staging/{release_branch}/{release_commit}-{workflow_id}/"
  )
  return last_run


def validate_run(run_data, repo_url, github_token):
  """Validates workflow run. Verifies status and waits if run is not finished."""
  status = safe_get(run_data, "status")
  conclusion = safe_get(run_data, "conclusion")
  if status == "completed" and conclusion == "success":
    return run_data

  url = safe_get(run_data, "url")
  workflow_web_url = GH_WEB_URL_WORKLOW_RUN_FMT.format(
      repo_url=repo_url, run_id=run_data["id"])
  print(
      f"Started waiting for Workflow run {run_data['id']} to finish. Check on {workflow_web_url}"
  )
  start_time = time.time()
  last_request = start_time
  spinner = itertools.cycle(["|", "/", "-", "\\"])
  request_interval = 10

  while True:
    now = time.time()
    elapsed_time = time.strftime("%H:%M:%S", time.gmtime(now - start_time))
    print(
        f"\r {next(spinner)} Waiting to finish. Elapsed time: {elapsed_time}. "
        f"Current state: status: `{status}`, conclusion: `{conclusion}`.",
        end="",
    )

    time.sleep(0.3)
    if (now - last_request) > request_interval:
      last_request = now
      run_data = request_url(url, github_token)
      status = safe_get(run_data, "status")
      conclusion = safe_get(run_data, "conclusion")
      if status in ["queued", "in_progress"]:
        continue
      elif status == "completed" and conclusion == "success":
        print(
            f"\rFinished in: {elapsed_time}. "
            f"Last state: status: `{status}`, conclusion: `{conclusion}`.",
        )
        return run_data
      else:
        print("\r")
        raise Exception(
            f"Run unsuccessful. Conclusion: {conclusion}. Payload: {run_data}")


def reset_directory(artifacts_dir):
  """Clears directory asking for confirmation before."""
  question = (
      f"Creating Artifacts directory. Any existing content in {artifacts_dir} will be erased. Proceed?\n"
      f"Your answer")
  if get_yes_or_no_answer(question):
    print(f"Clearing directory: {artifacts_dir}")
    shutil.rmtree(artifacts_dir, ignore_errors=True)
    os.makedirs(artifacts_dir)
  else:
    print("You said NO for clearing artifacts directory. Quitting ...")
    sys.exit(1)


def download_artifacts(artifacts_url, artifacts_dir, github_token):
  """Downloads github artifacts from given url."""
  print("Starting downloading artifacts ... (it may take a while)")
  data_artifacts = request_url(artifacts_url, github_token)
  artifacts = safe_get(data_artifacts, "artifacts", artifacts_url)
  filtered_artifacts = [
      a for a in artifacts if (
          a["name"].startswith("source_gztar_zip") or
          a["name"].startswith("wheelhouse"))
  ]
  for artifact in filtered_artifacts:
    url = safe_get(artifact, "archive_download_url")
    name = safe_get(artifact, "name")
    artifacts_size_mb = round(artifact["size_in_bytes"] / (1024 * 1024), 2)
    print(
        f"\tDownloading {name}.zip artifact (size: {artifacts_size_mb} megabytes)"
    )

    with tempfile.NamedTemporaryFile(
        "wb", prefix=name, suffix=".zip"
    ) as f, request_url(
        url, github_token, return_json=False, allow_redirects=True, stream=True
    ) as r:
      with open(f.name, "wb") as f:
        shutil.copyfileobj(r.raw, f)
      with zipfile.ZipFile(f.name, "r") as zip_ref:
        print(f"\tUnzipping {len(zip_ref.filelist)} files")
        zip_ref.extractall(artifacts_dir)


if __name__ == "__main__":
  print(
      "Starting script for download GitHub Actions artifacts for Build Wheels workflow"
  )
  (
      github_token,
      user_github_id,
      repo_url,
      release_branch,
      release_commit,
      artifacts_dir,
  ) = parse_arguments()

  try:
    workflow_id = get_build_wheels_workflow_id(repo_url, github_token)
    run = get_last_run(
        workflow_id, repo_url, release_branch, release_commit, github_token)
    run = validate_run(run, repo_url, github_token)
    artifacts_url = run["artifacts_url"]
    reset_directory(artifacts_dir)
    download_artifacts(artifacts_url, artifacts_dir, github_token)
    print("Script finished successfully!")
    print(f"Artifacts available in directory: {artifacts_dir}")
  except KeyboardInterrupt as e:
    print("\nScript cancelled. Quitting ...")
