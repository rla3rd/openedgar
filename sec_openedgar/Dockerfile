FROM continuumio/miniconda3 AS build

ENV PYTHONDONTWRITEBYTECODE=true

COPY .condarc /root/
RUN chmod 644 /root/.condarc
RUN apt-get update \
    && apt-get install -y gcc

# Install the package as normal:
COPY poetry.lock .
COPY pyproject.toml .
RUN mkdir /packages
COPY packages/lexnlp-4.20-py3-none-any.whl /packages/

RUN conda install -c conda-forge conda-pack poetry --yes --freeze-installed \
    && poetry install \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && find /opt/conda/ -follow -type f -name '*.pyc' -delete \
    && find /opt/conda/ -follow -type f -name '*.js.map' -delete \
    && conda-pack -n openedgar -o /tmp/env.tar \
    && mkdir /venv \
    && cd /venv  \
    && tar xf /tmp/env.tar \
    && rm /tmp/env.tar \
    && conda clean -afy

RUN /venv/bin/conda-unpack
FROM debian:12-slim

ENV TZ=America/New_York

COPY --from=build /venv /venv

RUN apt-get update \
    && apt-get install -y vim.tiny \
    && apt-get remove unattended-upgrades \
    && apt-get -qq -y autoremove \
    && apt-get autoclean \
    && rm -rf /var/lib/apt/lists/* /var/log/dpkg.log


COPY . .
CMD ["/venv/bin/activate", "&&", "python", "manage.py", "runserver", "0.0.0.0:8000"]