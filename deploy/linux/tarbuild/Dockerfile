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
ARG PYVER="3.9.13"

ENV REPO=$REPO
ENV BRANCH=$BRANCH
ENV VERSION=$VERSION
RUN apt-get update && apt-get install -y software-properties-common \
    ca-certificates curl gnupg wget gcc make zlib1g-dev libssl-dev \
    libffi-dev libsqlite3-dev liblzma-dev libpq-dev git patchelf
RUN mkdir -p /etc/apt/keyrings && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_18.x nodistro main" > /etc/apt/sources.list.d/nodesource.list

RUN wget https://www.python.org/ftp/python/$PYVER/Python-$PYVER.tgz && \
    tar -zxvf Python-$PYVER.tgz && \
    cd Python-$PYVER && \
    ./configure --enable-shared && \
    make && \
    make install

RUN rm Python-$PYVER.tgz
RUN rm -r Python-$PYVER/

RUN strip --strip-unneeded /usr/local/lib/libpython3.9.so.1.0

RUN apt-get update && apt-get install -y build-essential nodejs

RUN python3 -m pip install --upgrade pip setuptools
RUN pip3 install setuptools-rust staticx
COPY entrypoint.sh $HOME

ENTRYPOINT ["/root/entrypoint.sh"]