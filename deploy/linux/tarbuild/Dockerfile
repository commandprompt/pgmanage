# FROM pyappbuild
FROM ubuntu:18.04

USER root
ENV HOME /root
WORKDIR /root
SHELL ["/bin/bash", "-c"]

ARG REPO="https://github.com/commandprompt/pgmanage"
ARG BRANCH="dev"
ARG VERSION=""

ENV REPO=$REPO
ENV BRANCH=$BRANCH
ENV VERSION=$VERSION

RUN apt-get update && apt-get install -y git build-essential software-properties-common  zlib1g-dev libpq-dev libsasl2-dev  libldap2-dev libssl-dev curl
RUN add-apt-repository --yes ppa:deadsnakes/ppa
RUN apt-get update && apt-get install -y python3.8 python3-pip python-dev python3.8-dev
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 1
RUN python3 -m pip install --upgrade pip setuptools wheel
RUN pip3 install pyinstaller setuptools-rust
COPY entrypoint.sh $HOME

ENTRYPOINT ["/root/entrypoint.sh"]
