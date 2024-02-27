import json

def preprocess_handler(inference_record):
   
    flattened_data = {}

    # Process inference data
    if hasattr(inference_record, 'endpoint_output'):
        output_data_json = json.loads(inference_record.endpoint_output.data.rstrip("\n"))
        for i, pred in enumerate(output_data_json["prediction"]):
            flattened_data[f"endpointOutput_prediction{i}"] = pred

    # Process ground truth data
    if hasattr(inference_record, 'ground_truth'):
        # ground truth data is in array format
        ground_truth_data_json = json.loads(inference_record.ground_truth.data.rstrip("\n"))
        for i, gt in enumerate(ground_truth_data_json):
            flattened_data[f"groundTruthData_{i}"] = gt
    #return flattened_data
    return flattened_data



