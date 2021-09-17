# image: tapis/word_stats.pipelines
from python:3.9
RUN pip install --upgrade pip && pip install ipython nltk jsonschema

# copy the core code in directly; todo -- this should be removed once the core lib is properly published.
COPY core /core

# create the tapis staging directories --
RUN mkdir /TapisInput
RUN mkdir /TapisOutput

# copy the actual application code
COPY word_stats.py /word_stats.py

WORKDIR /
CMD ["python", "/word_stats.py"]