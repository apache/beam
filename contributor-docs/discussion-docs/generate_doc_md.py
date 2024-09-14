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
"""This script creates a MD file with a list of discussion documents
from dev@beam.apache.org.

Usage:

1. Download email archives: The script requires local copies of
the dev@beam.apache.org mbox files for the desired year.
You can download these manually or modify the script to
automate the download process.

2. Run the script:
   ```bash
   python generate_doc_md.py <year>
   ```
3. Output: The script will create a Markdown file named <year>.md containing
a table of discussion documents with their authors,
subjects, and submission dates.

Note:
The script currently extracts links to Google Docs and
Apache short links (s.apache.org). Ensure you have the necessary libraries
installed (e.g., requests, bs4, mailbox).

"""

import os
import re
import requests
import mailbox
import datetime
import sys

from bs4 import BeautifulSoup
from dataclasses import dataclass

LIST_NAME = "dev"
DOMAIN = "beam.apache.org"
OUTPUT_DIR = "generated"


def download_mbox(list_name, domain, year, month):
  """Downloads an mbox file from the Apache mailing list archive."""

  # Construct the URL
  url = f"https://lists.apache.org/api/mbox.lua?list={list_name}&domain={domain}&d={year}-{month:02d}"

  try:
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Raise an exception for bad status codes

    # Create the directory for the archive if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Generate the output filename
    output_filename = f"{OUTPUT_DIR}/{list_name}@{domain}_{year}-{month:02d}.mbox"

    with open(output_filename, "wb") as f:
      for chunk in response.iter_content(chunk_size=8192):
        f.write(chunk)

    print(f"Downloaded {output_filename}")

  except requests.exceptions.RequestException as e:
    print(f"Error downloading archive: {e}")


def download_mbox_for_one_year(year):
  """Downloads mbox files for each month in a given year."""
  for month in range(1, 13):
    download_mbox(LIST_NAME, DOMAIN, year, month)


def get_google_doc_title(link):
  """Fetches the title of a Google Doc from its link."""
  try:
    response = requests.get(link)
    response.raise_for_status()  # Raise an exception for bad status codes

    soup = BeautifulSoup(response.content, "html.parser")
    title = soup.title.string.strip()
    return title
  except requests.exceptions.RequestException as e:
    print(f"Error fetching URL: {e}  {link}")
    return None
  except Exception as e:
    print(f"Error extracting title: {e} {link}")
    return None


def extract_name_re(email_string):
  """Extracts the name from an email string using regular expressions."""
  email_string = email_string.replace('"', "")
  match = re.match(r"^(.+?) via .+ <.+@.+>$", email_string)
  if match:
    return match.group(1)
  else:
    match = re.match(r"^(.+?) <.+@.+>$", email_string)
    if match:
      return match.group(1)
  return email_string


def convert_to_timestamp(date_string):
  """Converts a date string to a timestamp object."""

  date_format = "%a, %d %b %Y %H:%M:%S %z"
  datetime_obj = datetime.datetime.strptime(date_string, date_format)
  timestamp = datetime_obj.timestamp()
  return timestamp


@dataclass
class EmailMessage:
  """A data class representing an email message."""

  sender: str
  doc_title: str
  doc_url: str
  body: str
  timestamp: datetime.datetime = None


def extract_google_doc_sheet_link(text):
  """Extracts Google Docs or Sheets link from text."""
  pattern = r"https?:\/\/docs\.google\.com\/(document|spreadsheets)\/d\/([a-zA-Z0-9-_]+)\/.*"
  match = re.search(pattern, text)
  if match:
    return match.group(0)
  else:
    return None


def extract_s_link(text):
  """Extracts Apache short link from text."""
  pattern = r"https?://s\.apache\.org/.*"
  match = re.search(pattern, text)
  if match:
    return match.group(0)
  else:
    return None


def add_message(messages: list[EmailMessage], new_message: EmailMessage):
  """Adds a new message to the list, ensuring unique subjects and keeping the oldest message."""

  url = new_message.doc_url
  for i, message in enumerate(messages):
    if message.doc_url == url:
      if new_message.timestamp < message.timestamp:
        messages[i] = new_message
      return
  messages.append(new_message)


def remove_invalid_characters(string):
  """Removes invalid characters from a string."""

  while string.endswith(".") or string.endswith("(") or string.endswith(")"):
    string = string[:-1]

  return string


def find_google_docs_links(mbox_file, doc_messages, doc_urls):
  """Filters email messages from an mbox file that contain Google Docs links."""

  if not os.path.isfile(mbox_file):
    print(f"Cannot find the file {mbox_file}")

  mbox = mailbox.mbox(mbox_file)

  for message in mbox:
    c = message.get_payload()
    # for multipart messages, only use the first part
    while isinstance(c, list):
      c = c[0].get_payload()

    # assume the message only contain one doc url
    doc_url = None
    gdoc_url = extract_google_doc_sheet_link(c)
    if gdoc_url:
      doc_url = gdoc_url.split()[0].split(">")[0]
    else:
      s_url = extract_s_link(c)
      if s_url:
        doc_url = s_url.split()[0].split(">")[0]
    if doc_url and not doc_url in doc_urls:
      doc_url = remove_invalid_characters(doc_url)
      doc_urls.append(doc_url)
      title = get_google_doc_title(doc_url)
      try:
        sender = extract_name_re(str(message["From"]))
      except:
        print("Something is wrong: ", message["From"])
        sender = None
      if not sender:
        print("test-------")
        print(message["From"])
      doc_time = convert_to_timestamp(message["Date"])
      if title:
        title = title.replace("- Google Docs", "").strip()
        new_msg = EmailMessage(
            doc_title=title,
            doc_url=doc_url,
            body=c,
            sender=sender,
            timestamp=doc_time,
        )
        add_message(doc_messages, new_msg)

  return doc_messages


def sort_emails_by_timestamp(emails: list[EmailMessage]) -> list[EmailMessage]:
  """Sorts a list of EmailMessage objects by timestamp from oldest to newest."""

  return sorted(emails, key=lambda email: email.timestamp)


def extract_docs_for_one_year(year):
  """Extracts Google Docs links from emails in a given year."""

  doc_messages = []
  doc_urls = []
  for month in range(1, 13):
    # Generate the output filename
    output_filename = f"{OUTPUT_DIR}/{LIST_NAME}@{DOMAIN}_{year}-{month:02d}.mbox"
    find_google_docs_links(output_filename, doc_messages, doc_urls)
  return sort_emails_by_timestamp(doc_messages)


def convert_to_md_table(email_messages: list[EmailMessage], year: int):
  """Converts a list of EmailMessage objects to a Markdown file with a table."""

  output_file = f"{year}.md"
  with open(output_file, "w") as f:
    f.write("""<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->\n\n""")
    f.write(
        f"# List Of Documents Submitted To dev@beam.apache.org In {year}\n")
    f.write("| No. | Author | Subject | Date (UTC) |\n")
    f.write("|---|---|---|---|")
    for eid, email in enumerate(email_messages):
      datetime_obj = datetime.datetime.fromtimestamp(email.timestamp)
      formatted_date = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
      doc_title = email.doc_title.replace("|", ":")
      row_no = f'{eid+1}'
      f.write(
          f"\n| {row_no} | {email.sender} | [{doc_title}]({email.doc_url}) | {formatted_date} |"
      )


if __name__ == '__main__':
  if len(sys.argv) > 1:
    year = sys.argv[1]
    #download_mbox_for_one_year(year)
    docs = extract_docs_for_one_year(year)
    convert_to_md_table(docs, year)
  else:
    print("Please provide a year as an argument.")
