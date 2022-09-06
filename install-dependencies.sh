#!/bin/bash
sudo apt-get update -y \
&& sudo apt-get install python3.9 \
&& sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.7 1 \
&& sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 2 \
&& python --version \
&& apt-get install python3-pip \
&& sudo apt install python3.9-distutils \
&& python3 -c "from distutils import sysconfig" \
&& python -m pip install --upgrade pip \
&& pip install tinkoff-investments \
&& pip install pandas \
&& pip install --upgrade google-api-python-client \
&& pip install --upgrade oauth2client

