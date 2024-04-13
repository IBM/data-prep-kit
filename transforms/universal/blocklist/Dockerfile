FROM docker.io/rayproject/ray:2.9.3-py310

ARG DATA_PREP_LAB
ARG BUILD_DATE
ARG GIT_COMMIT

LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT
LABEL data-prep-lab=${DATA_PREP_LAB}

RUN pip install --no-cache-dir -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple data-prep-lab==${DATA_PREP_LAB}
# Update to fix security
RUN pip install pillow==10.2.0
RUN pip install fastapi==0.109.1
RUN pip install aiohttp==3.9.2
#cryptography==42.0.4
### For blocklist
RUN pip install pygtrie>=2.5.0

COPY ./src/blocklist_transform.py . 
ENV PYTHONPATH /home/ray
