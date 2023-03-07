from google.cloud import bigquery
from flask import Flask,Response,request
import pandas as pd

def middlelayer_fn1(cloudevent):
    project_id = "calcium-sunbeam-341614"
    qry = request.json.get('query')
    bq_client = bigquery.Client(project=project_id)
    df_qry_result = bq_client.query(qry)
    result_list = pd.DataFrame([dict(row) for row in df_qry_result])
    result = result_list.to_json(orient='records')
    print("Middle-Layer-Fn1 Run With A Success")
    return Response(result,200,mimetype='json')