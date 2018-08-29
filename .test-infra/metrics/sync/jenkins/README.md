# Running script locally
1. Build container
2. `docker run -it --rm --name my-running-script -v "$PWD":/usr/src/myapp -w /usr/src/myapp beamsyncjenkins:v1 python syncjenkins.py`
