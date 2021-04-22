import sys
import requests

fixup_words = 'fixup', 'typo', 'lint', 'reviewer', 'spotless', 'mypy'


def main(url):
    url = sys.argv[-1]
    info = requests.get(url).json()
    if is_approved(info):
        print(merge_advice(requests.get(url + '/commits').json()))


def is_approved(info):
    for review in requests.get(info['_links']['self']['href'] +
                               '/reviews').json():
        if review['state'] == 'APPROVED':
            return True
        elif 'LGTM' in review['body']:
            return True
    for comment in requests.get(info['_links']['comments']['href']).json():
        if 'LGTM' in comment['body']:
            return True


def merge_advice(commits):
    if len(commits) == 1:
        return "In it's infinite wisdom, squashbot recommends the merge option."
    fixup_commits = sum(is_fixup_commit(c) for c in commits)
    if fixup_commits:
        return "Looks like there are some commits fixup commits. Squash and merge?"
    elif len(commits) > 5:
        return "That's a lot of commits, is squash and merge the right option?"
    else:
        return "Consider using the merge button on this one."


def is_fixup_commit(commit):
    msg = commit['commit']['message'].lower()
    return any(word in msg for word in fixup_words)


if __name__ == '__main__':
    main(sys.argv[-1])
