FROM python:3.8-slim

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY ./src/run.py ./run.py
COPY ./wait-for-it.sh ./wait-for-it.sh

COPY ./twsapi_macunix.981.01/IBJts/source/pythonclient .
RUN python setup.py bdist_wheel
RUN python -m pip install --user --upgrade dist/ibapi-9.81.1-py3-none-any.whl

CMD [ "./wait-for-it.sh", "database:5432", "--", "python", "./run.py" ]
