FROM amazonlinux:latest
RUN yum -y install which unzip aws-cli
RUN yum install -y gcc-c++ make
RUN curl -sL https://rpm.nodesource.com/setup_14.x | bash -
RUN yum install -y nodejs
ADD package.json /tmp/package.json
RUN cd /tmp
CMD npm install
ADD fetch_and_run.sh /usr/local/bin/fetch_and_run.sh
WORKDIR /tmp
USER nobody
ENTRYPOINT ["/usr/local/bin/fetch_and_run.sh"]