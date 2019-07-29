FROM remotepixel/amazonlinux-gdal:2.5.0dev

RUN yum install jq -y

COPY requirements.txt .
COPY create_cog.sh .

RUN pip install -r requirements.txt

WORKDIR app