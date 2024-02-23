# -- Software Stack Version

SPARK_VERSION="2.3.4"
JUPYTERLAB_VERSION="2.1.5"

# -- Building the Images

docker build \
  -f cluster-base.Dockerfile \
  -t cluster-base .


# Zeppelin
docker run \
        -u $(id -u) \
        -p 8080:8080 \
        --rm \
        -v $PWD/logs:/logs \
        -v $PWD/notebook:/notebook \
           -e ZEPPELIN_LOG_DIR='/logs' \
           -e ZEPPELIN_NOTEBOOK_DIR='/notebook' \
           --name zeppelin apache/zeppelin:0.10.0


docker run \
  -u $(id -u) \
  -p 8080:8080 \
  --rm -v /mnt/disk1/notebook:/notebook \
  -v /usr/lib/spark-current:/opt/spark \
  -v /mnt/disk1/flink-1.12.2:/opt/flink \
  -e FLINK_HOME=/opt/flink  \
  -e SPARK_HOME=/opt/spark  \
  -e ZEPPELIN_NOTEBOOK_DIR='/notebook' \
  --name zeppelin apache/zeppelin:0.10.0


# Jupyter lab
docker build \
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg jupyterlab_version="${JUPYTERLAB_VERSION}" \
  -f jupyterlab.Dockerfile \
  -t jupyterlab .

# Use the '--no-cache' option to force a full rebuilt
