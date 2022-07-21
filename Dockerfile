FROM public.ecr.aws/lambda/python:3.8

RUN yum install -y git gcc

RUN pip3 install https://imos-artifacts.s3.ap-southeast-2.amazonaws.com/promoted/python-aodncore/production/aodncore-1.3.11-py3-none-any.whl && \
    pip3 install https://imos-artifacts.s3.ap-southeast-2.amazonaws.com/promoted/python-aodntools/production/aodntools-1.5.3-py3-none-any.whl && \
    pip3 install https://imos-artifacts.s3.ap-southeast-2.amazonaws.com/promoted/cc-plugin-imos/production/cc_plugin_imos-1.4.8-py2.py3-none-any.whl && \
    pip3 install https://imos-artifacts.s3.ap-southeast-2.amazonaws.com/promoted/python-aodndata/production/aodndata-1.3.79-py3-none-any.whl

COPY requirements.txt  .
RUN  pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY app.py app_copy.py ${LAMBDA_TASK_ROOT}/

# Smoke test!
RUN python -c "from aodndata.soop.soop_xbt_nrt import parse_bufr_file"

CMD [ "app.handler" ]
