/usr/bin/curl -o datos-"$(date '+%d-%m-%Y')".csv https://www.datos.gov.co/resource/gt2j-8ykr.csv
aws s3 cp datos-"$(date '+%d-%m-%Y')".csv s3://proyecto-bigdata/raw/
