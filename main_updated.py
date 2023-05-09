from google.cloud import bigquery
from flask import Flask,Response,request,jsonify
import pandas as pd
import json

def middlelayer_fn1(cloudevent):
    project_id = "calcium-sunbeam-341614"
    qry = request.json.get('query')
    bq_client = bigquery.Client(project=project_id)
    df_qry_result = bq_client.query(qry)
    result_list = pd.DataFrame([dict(row) for row in df_qry_result])
    result = result_list.to_json(orient='records')

    json_list = json.loads(result)

    nested_dict = {'data': []}
    for json_dict in json_list:
        for key, value in json_dict.items():
            nested_dict['data'].append({"label":key,"value":value})

    nested_json_str = json.dumps(nested_dict)
    print("Middle-Layer-Fn1 Run With A Success")
    #return Response(result,200,mimetype='json')
    return Response(nested_json_str,200,mimetype='json')
