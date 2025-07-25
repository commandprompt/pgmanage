FROM ubuntu:20.04 AS build-base

ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

USER root
ENV HOME=/root
WORKDIR /root
SHELL ["/bin/bash", "-c"]

ARG PYVER="3.9.13"

RUN apt-get update && apt-get install -y wget gcc make zlib1g-dev libssl-dev \
    libffi-dev libsqlite3-dev liblzma-dev

RUN wget https://www.python.org/ftp/python/$PYVER/Python-$PYVER.tgz && \
    tar -zxvf Python-$PYVER.tgz && \
    cd Python-$PYVER && \
    ./configure --enable-shared && \
    make && \
    make install

RUN rm Python-$PYVER.tgz
RUN rm -r Python-$PYVER/

RUN strip --strip-unneeded /usr/local/lib/libpython3.9.so.1.0

RUN apt-get update && apt-get install -y build-essential

RUN pip3 install setuptools-rust staticx

FROM ubuntu:20.04

ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

USER root

ENV REPO="https://github.com/commandprompt/pgmanage"
ENV BRANCH="dev"
ENV VERSION=""
ENV HOME=/root
ENV LD_LIBRARY_PATH=/usr/local/lib

WORKDIR /root
SHELL ["/bin/bash", "-c"]

COPY --from=build-base /usr/local /usr/local
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates curl npm gnupg patchelf libpq-dev file binutils

RUN mkdir -p /etc/apt/keyrings && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
RUN echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_18.x nodistro main" > /etc/apt/sources.list.d/nodesource.list

RUN apt-get update && apt-get install -y --no-install-recommends nodejs

RUN apt autoremove -y

COPY entrypoint.sh $HOME

ENTRYPOINT ["/root/entrypoint.sh"]