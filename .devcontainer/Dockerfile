FROM continuumio/miniconda3

ENV PATH /root/.local/bin:$PATH

RUN conda install -c conda-forge -y pipx

RUN pipx install poetry

COPY environment.yml .

RUN conda env create -n pyspark -f environment.yml && rm environment.yml

RUN conda clean --all -y

ENV PYSPARK_PYTHON=/opt/conda/envs/pyspark/bin/python

ENV IN_DOCKER=true
