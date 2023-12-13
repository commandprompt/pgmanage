# FROM pyappbuild
FROM ubuntu:20.04

ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

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
RUN apt-get update && apt-get install -y software-properties-common ca-certificates curl gnupg
RUN mkdir -p /etc/apt/keyrings && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_18.x nodistro main" > /etc/apt/sources.list.d/nodesource.list
RUN add-apt-repository --yes ppa:deadsnakes/ppa
RUN apt-get update && apt-get install -y git build-essential \
    nodejs zlib1g-dev libpq-dev libsasl2-dev  libldap2-dev libssl-dev \
    fuse patchelf python3.9 python3-pip python3.9-dev python3.9-distutils
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1
RUN python3 -m pip install --upgrade pip setuptools
RUN pip3 install setuptools-rust staticx
COPY entrypoint.sh $HOME

ENTRYPOINT ["/root/entrypoint.sh"]
