import json

def preprocess_handler(inference_record):
    #input_data = inference_record.endpoint_input.data
    #output_data_json = json.loads(inference_record.endpoint_output.data.rstrip("\n"))
    #print("testing inference record preprocessing")
    # Flatten the 'prediction' list into a dictionary with keys 'prediction0', 'prediction1', etc.
    #flattened_data = {f"endpointOutput_prediction{i}": pred for i, pred in enumerate(output_data_json["prediction"])}

    #return flattened_data
    flattened_data = {}

    # Process inference data
    if hasattr(inference_record, 'endpoint_output'):
        output_data_json = json.loads(inference_record.endpoint_output.data.rstrip("\n"))
        for i, pred in enumerate(output_data_json["prediction"]):
            flattened_data[f"endpointOutput_prediction{i}"] = pred

    # Process ground truth data
    if hasattr(inference_record, 'ground_truth'):
        # Assuming the ground truth data is in a JSON array format
        ground_truth_data_json = json.loads(inference_record.ground_truth.data.rstrip("\n"))
        for i, gt in enumerate(ground_truth_data_json):
            flattened_data[f"groundTruthData_{i}"] = gt

    return flattened_data



