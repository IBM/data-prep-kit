# Base image for KFP.

FROM python:3.10-slim-bullseye

RUN apt-get update && apt-get install make

# install libraries
# Adding pyarrow and scipy install, required by dedup
RUN pip install --no-cache-dir pyarrow==14.0.2 scipy==1.9.0
# FIXME: a fix to kfp-tekton mentioned in https://github.com/yaml/pyyaml/issues/601#issuecomment-1660456820
RUN pip install --no-cache-dir "cython<3.0.0" wheel && pip install --no-cache-dir pyyaml==5.4.1 --no-build-isolation
# Adding kfp_tekton install, required by sub workflow
RUN pip install --no-cache-dir -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple fm-data-processing-kfp==0.0.3.dev11

RUN pip install --no-cache-dir "ray[default]"==2.9.1
RUN pip install pydantic==2.5.3
COPY ./src /pipelines/component/src
