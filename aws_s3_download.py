import boto3
import pandas as pd

s3 = boto3.client('s3')
# s3.download_file('proyecto-bigdata', 'raw/datos-30-11-2020.csv', 'datos.csv')
s3.download_file('proyecto-bigdata', 'refined/12-07-2020/paises/part-00000-41daa53d-c41a-43de-a19a-b5601a606564-c000.csv', 'part-00000-41daa53d-c41a-43de-a19a-b5601a606564-c000.csv')
s3.download_file('proyecto-bigdata', 'refined/12-07-2020/recuperados_muertos_medellin/part-00000-7afeac70-fe73-4728-b40e-5495dfb2f02c-c000.csv', 'part-00000-7afeac70-fe73-4728-b40e-5495dfb2f02c-c000.csv')
s3.download_file('proyecto-bigdata', 'refined/12-07-2020/recuperados_y_fallecidos_departamento/part-00000-e73a5149-3345-479e-af2d-cd86643cdd2b-c000.csv', 'part-00000-e73a5149-3345-479e-af2d-cd86643cdd2b-c000.csv')
s3.download_file('proyecto-bigdata', 'refined/12-07-2020/recuperados_y_fallecidos_etapa/part-00000-da608de5-58e3-4641-a628-a95302203fa5-c000.csv', 'part-00000-da608de5-58e3-4641-a628-a95302203fa5-c000.csv')
s3.download_file('proyecto-bigdata', 'refined/12-07-2020/sexo/part-00000-d3687c0d-1451-4dfe-81f8-8e338abf1f89-c000.csv', 'part-00000-d3687c0d-1451-4dfe-81f8-8e338abf1f89-c000.csv')

print(pd.read_csv('part-00000-41daa53d-c41a-43de-a19a-b5601a606564-c000.csv'))
print(pd.read_csv('part-00000-7afeac70-fe73-4728-b40e-5495dfb2f02c-c000.csv'))
print(pd.read_csv('part-00000-e73a5149-3345-479e-af2d-cd86643cdd2b-c000.csv'))
print(pd.read_csv('part-00000-da608de5-58e3-4641-a628-a95302203fa5-c000.csv'))
print(pd.read_csv('part-00000-d3687c0d-1451-4dfe-81f8-8e338abf1f89-c000.csv'))