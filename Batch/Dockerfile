FROM amazonlinux:latest
RUN yum -y install which unzip aws-cli
ADD fetch_and_run.sh /usr/local/bin/fetch_and_run.sh
RUN yum install -y python3-pip gcc-c++ python3-devel unixODBC-devel
RUN python3 -m pip install wheel pymssql boto3
WORKDIR /tmp
USER nobody
ENTRYPOINT ["/usr/local/bin/fetch_and_run.sh"]
