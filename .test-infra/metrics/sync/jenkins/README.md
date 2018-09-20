# Running script locally
1. Build container
2. `docker run -it --rm --name sync -v "$PWD":/usr/src/myapp -w /usr/src/myapp -e "JENSYNC_PORT=5432" -e "JENSYNC_DBNAME=beam_metrics" -e "JENSYNC_DBUSERNAME=admin" -e "JENSYNC_DBPWD=<password>" syncjenkins python syncjenkins.py`
