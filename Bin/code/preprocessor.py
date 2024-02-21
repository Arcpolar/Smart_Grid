import json

def preprocess_handler(inference_record):
    input_data = inference_record.endpoint_input.data
    output_data_json = json.loads(inference_record.endpoint_output.data.rstrip("\n"))
    #print("testing inference record preprocessing")
    # Flatten the 'prediction' list into a dictionary with keys 'prediction0', 'prediction1', etc.
    flattened_data = {f"endpointOutput_prediction{i}": pred for i, pred in enumerate(output_data_json["prediction"])}

    # Optionally, merge with input data (assuming input data is in a compatible format)
    # You might need to parse and flatten the input data similarly if it's also JSON

    return flattened_data

