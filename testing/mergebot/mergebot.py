"""Mergebot is a script which talks to GitHub and submits all ready pull requests.

Mergebot talks to a specified GitHub project and watches for @mentions for its account.
Acceptable commands are:
  @<mergebot-name> merge
"""
from subprocess import call
import requests
import sys
import time

AUTHORIZED_USERS = ["davor"]
BOT_NAME = 'beam-testing'
GITHUB_ORG = 'apache'
REPOSITORY = 'incubator-beam'
SECRET_FILE = '../../github_auth/apache-beam.secret'
SOURCE_REMOTE = 'github'
TARGET_BRANCH = 'master'
TARGET_REMOTE = 'apache'

GITHUB_API_ROOT = 'https://api.github.com'
GITHUB_REPO_FMT_URL = GITHUB_API_ROOT + '/repos/{0}/{1}'
GITHUB_REPO_URL = GITHUB_REPO_FMT_URL.format(GITHUB_ORG, REPOSITORY)
CMDS = ['merge']
ISSUES_URL = GITHUB_REPO_URL + '/issues'
COMMENT_FMT_URL = ISSUES_URL + '/{pr_num}/comments'
PULLS_URL = GITHUB_REPO_URL + '/pulls'

bot_key = ''


def main():
  print('Starting up.')
  # Load github key from filesystem
  key_file = open(SECRET_FILE, 'r')
  bot_key = key_file.read().strip()
  print('Loaded key file.')
  # Loop: Forever, once per minute.
  while True:
    poll_github()
    time.sleep(60)

def poll_github():
  print('Loading pull requests from Github at {}.'.format(PULLS_URL))
  # Load list of pull requests from Github
  r = requests.get(PULLS_URL, auth=(BOT_NAME, bot_key))
  if r.status_code != 200:
    print('Oops, that didn\'t work. Error below, waiting then trying again.')
    print(r.text)
    return

  print('Loaded.')
  pr_json = r.json()
  # Loop: Each pull request
  for pr in pr_json:
    search_pr(pr)


def search_pr(pr):
  pr_num = pr['number']
  print('Looking at PR #{}.'.format(pr_num))
  # Load comments for each pull request
  cmt_url = COMMENT_FMT_URL.format(pr_num=pr_num)
  print('Loading comments.')
  r = requests.get(cmt_url, auth=(BOT_NAME, bot_key))
  if r.status_code != 200:
    print('Oops, that didn\'t work. Error below, moving on.')
    print(r.text)
    return

  cmt_json = r.json()
  if len(cmt_json) < 1:
    print('No comments on PR #{}. Moving on.'.format(pr_num))
    return
  # FUTURE: Loop over comments to make sure PR has been LGTMed
  cmt = cmt_json[-1]
  cmt_body = cmt['body'].encode('ascii', 'ignore')
  # Look for @apache-beam request comments
  # FUTURE: Look for @apache-beam reply comments
  if not cmt_body.startswith('@apache-beam'):
    print('Last comment: {}, not a command. Moving on.'.format(cmt_body))
    return
  cmd_str = cmt_body.split('@apache-beam ', 1)[1]
  cmd = cmd_str.split(' ')[0]
  if cmd not in CMDS:
    # Post back to PR
    post_error('Command was {}, not a valid command.'.format(cmd), pr_num)
    print('Command was {}, not a valid command.'.format(cmd))
    return

  if cmd == 'merge':
    if cmt['user']['login'] not in AUTHORIZED_USERS:
      post_error('Unauthorized users cannot merge: {}'.format(cmt['user']['login']))
      print('Unauthorized user {} attempted to merge PR {}.'.format(cmt['user']['login'], pr_num))
      return
    # Kick off merge workflow
    print('Command was merge, merging.')
    if merge(pr_num):
      post_info('Merge of PR#{} succeeded.', pr)
      if not clean_up():
        print("cleanup failed; dying.")
        sys.exit(1)


def merge(pr):
  if not set_up():
    post_error('Error setting up - please try again.', pr)
    return False
  # Make temp directory and cd into.
  # Clone repository and configure.
  print("Starting merge process for #{}.".format(pr))
  clone_success = call(['git', 'clone', '-b', TARGET_BRANCH, 'https://github.com/{}/{}.git'.format(GITHUB_ORG, REPOSITORY), '/usr/local/google/home/jasonkuster/tmp/'], cwd='/usr/local/google/home/jasonkuster/tmp/')
  if not clone_success == 0:
    post_error('Couldn\'t clone from github/{}/{}. Please try again.'.format(GITHUB_ORG, REPOSITORY), pr)
    return False
  call(['git', 'remote', 'add', TARGET_REMOTE, 'https://git-wip-us.apache.org/repos/asf/{}.git'.format(REPOSITORY)], cwd='/usr/local/google/home/jasonkuster/tmp/')
  call(['git', 'remote', 'rename', 'origin', SOURCE_REMOTE], cwd='/usr/local/google/home/jasonkuster/tmp/')
  call('git config --local --add remote.' + SOURCE_REMOTE  + '.fetch "+refs/pull/*/head:refs/remotes/{}/pr/*"'.format(SOURCE_REMOTE), shell=True, cwd='/usr/local/google/home/jasonkuster/tmp/')
  call(['git', 'fetch', '--all'], cwd='/usr/local/google/home/jasonkuster/tmp/')
  print("Initial work complete.")
  # Clean up fetch
  initial_checkout = call(['git', 'checkout', '-b', 'finish-pr-{}'.format(pr), 'github/pr/{}'.format(pr)], cwd='/usr/local/google/home/jasonkuster/tmp/')
  if not initial_checkout == 0:
    post_error("Couldn't checkout code. Please try again.", pr)
    return False
  print("Checked out.")
  # Rebase PR onto main.
  rebase_success = call(['git', 'rebase', '{}/{}'.format(TARGET_REMOTE, TARGET_BRANCH)], cwd='/usr/local/google/home/jasonkuster/tmp/')
  if not rebase_success == 0:
    print(rebase_success)
    post_error('Rebase was not successful. Please rebase against main and try again.', pr)
    return False
  print("Rebased")

  # Check out target branch to here
  checkout_success = call(['git', 'checkout', '{}/{}'.format(TARGET_REMOTE, TARGET_BRANCH)], cwd='/usr/local/google/home/jasonkuster/tmp/')
  if not checkout_success == 0:
    post_error('Error checking out target branch: master. Please try again.', pr)
    return False
  print("Checked out Apache master.")

  # Merge
  merge_success = call(['git', 'merge', '--no-ff', '-m', 'This closes #{}'.format(pr), 'finish-pr-{}'.format(pr)], cwd='/usr/local/google/home/jasonkuster/tmp/')
  if not merge_success == 0:
    post_error('Merge was not successful against target branch: master. Please try again.', pr)
    return False
  print("Merged successfully.")

  print("Running mvn clean verify.")
  # mvn clean verify
  mvn_success = call(['mvn', 'clean', 'verify'], cwd='/usr/local/google/home/jasonkuster/tmp/')
  if not mvn_success == 0:
    post_error('mvn clean verify against HEAD + PR#{} failed. Not merging.'.format(pr), pr)
    return False

  # git push (COMMENTED UNTIL MERGEBOT HAS PERMISSIONS)
  #push_success = call(['git', 'push', 'apache', 'HEAD:master'], cwd='/usr/local/google/home/jasonkuster/tmp/')
  #if not push_success == 0:
  #  post_error('Git push failed. Please try again.', pr)
  #  return False
  return True


def post_error(content, pr_num):
  post_pr_comment("Error: {}, #{}.".format(content, pr_num))


def post_info(content, pr_num):
  post_pr_comment("Info: {}, #{}.".format(content, pr_num))


def post_pr_comment(content, pr_num):
  print(content)
  post(content, COMMENT_FMT_URL.format(pr_num=pr_num))


def post(content, endpoint):
  payload = {"body": content}
  requests.post(endpoint, data=payload)


def set_up():
  if not call(['mkdir', '/usr/local/google/home/jasonkuster/tmp']) == 0:
    return False
  return True


def clean_up():
  if not call(['rm', '-rf', '/usr/local/google/home/jasonkuster/tmp']) == 0:
    return False
  return True


if __name__ == "__main__":
  main()
