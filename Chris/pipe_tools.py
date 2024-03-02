"""
AWS CI/CD Pipeline Tools

This Python module provides a set of tools for creating and managing CI/CD pipelines on AWS. It includes functions for interacting with various AWS services such as CodePipeline, CodeBuild, and CodeDeploy to automate the build, test, and deployment processes.

Author: Christopher Watson
Date: 2/29/24

Usage:
- Use the functions in this module to create, update, or delete CI/CD pipelines on AWS.
- Ensure you have the necessary AWS credentials and permissions set up in your environment.
"""
from sagemaker import image_uris, model_uris, script_uris
import awswrangler as wr
import time
from  time import strftime, gmtime
from sagemaker.feature_store.feature_group import FeatureGroup
import pandas as pd

def setup_uris(model_id, model_version, scope, instance_type):
    # Retrieve the docker image
    temp_image = image_uris.retrieve(
        region=None,
        framework=None,
        model_id=model_id,
        model_version=model_version,
        image_scope=scope,
        instance_type=instance_type,
    )

    # Retrieve the training script
    temp_source_uri = script_uris.retrieve(
        model_id=model_id, model_version=model_version, script_scope=scope
    )
    # Retrieve the pre-trained model tarball to further fine-tune
    temp_model_uri = model_uris.retrieve(
        model_id=model_id, model_version=model_version, model_scope=scope
    )
    return {'image':temp_image, 'source':temp_source_uri, 'model':temp_model_uri}

def get_pqdata(bucket, train_filter, val_filter, batch_filter):
    path = "s3://{}/meterdataset.parquet/".format(bucket)
    df_parquet_train = wr.s3.read_parquet(
        path, 
        #columns=["artists", "track_name", "popularity"], 
        partition_filter=train_filter, 
        dataset=True
    )

    path = "s3://{}/meterdataset.parquet/".format(bucket)
    df_parquet_val = wr.s3.read_parquet(
        path, 
        #columns=["artists", "track_name", "popularity"], 
        partition_filter=val_filter, 
        dataset=True
    )    
    
    path = "s3://{}/meterdataset.parquet/".format(bucket)
    df_parquet_val = wr.s3.read_parquet(
        path, 
        #columns=["artists", "track_name", "popularity"], 
        partition_filter=val_filter, 
        dataset=True
    )
    
    path = "s3://{}/meterdataset.parquet/".format(bucket)
    df_parquet_batch = wr.s3.read_parquet(
        path, 
        #columns=["artists", "track_name", "popularity"], 
        partition_filter=batch_filter, 
        dataset=True
    )

    df_parquet_train = df_parquet_train.drop(columns=['year','month'])
    df_parquet_val = df_parquet_val.drop(columns=['year','month'])
    df_parquet_batch = df_parquet_batch.drop(columns=['year','month'])
    df_parquet_train['weekday'] = df_parquet_train['weekday'].astype(float)
    df_parquet_val['weekday'] = df_parquet_val['weekday'].astype(float)
    df_parquet_batch['weekday'] = df_parquet_batch['weekday'].astype(float)
    
    df_parquet_batch = df_parquet_batch.drop(columns=['kWh'])
    
    return {'train':df_parquet_train, 'val':df_parquet_val, 'batch':df_parquet_batch}


def cast_object_to_string(data_frame):
    for label in data_frame.columns:
        if data_frame.dtypes[label] == "object" or  data_frame.dtypes[label] == "datetime64[ns]":
            data_frame[label] = data_frame[label].astype("str").astype("string")
            
def setup_feature_groups(pq_data, feature_store_session, event_time_feature_name):
    stamp_mark = strftime("%d-%H-%M-%S", gmtime())
    current_time_sec = int(round(time.time()))


    f_groups = []

    for key in pq_data.keys():
        pq_data[key] = pq_data[key].reset_index()
        feature_group_name = str(key) + "-feature-group-" + stamp_mark
        feature_group = FeatureGroup(
            name=feature_group_name, sagemaker_session=feature_store_session
        )
        cast_object_to_string(pq_data[key])
        # append EventTime feature
        pq_data[key][event_time_feature_name] = pd.Series(
            [current_time_sec] * len(pq_data[key]), dtype="float64"
        )
        feature_group.load_feature_definitions(data_frame=pq_data[key])
        f_groups.append(feature_group)
        
    return f_groups

def wait_for_feature_group_creation_complete(feature_group):
    status = feature_group.describe().get("FeatureGroupStatus")
    while status == "Creating":
        print("Waiting for Feature Group Creation")
        time.sleep(5)
        status = feature_group.describe().get("FeatureGroupStatus")
    if status != "Created":
        raise RuntimeError(f"Failed to create feature group {feature_group.name}")
    print(f"FeatureGroup {feature_group.name} successfully created.")