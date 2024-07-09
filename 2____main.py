
import os
import pandas as pd
import numpy as np
import statsmodels.api as sm
from sklearn.preprocessing import MinMaxScaler
import boto3
import io
import logging
from datetime import datetime
import json
import warnings
warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=Warning)
s3_client=boto3.client('s3')
s3_resource = boto3.resource('s3')

print('Loading environment variables')
RESULTS_BUCKET = os.environ['RESULTS_BUCKET']
DATA_FOLDER =os.environ['DATA_FOLDER']
DATA_FILE_PATH = os.environ['DATA_FILE_PATH']
RESULTS_FOLDER = os.environ['RESULTS_FOLDER']
RESULT_FILE_PATH = os.environ['RESULT_FILE_PATH']
print(RESULT_FILE_PATH)
TARGET_VARIABLE = os.environ['TARGET_VARIABLE']
print(TARGET_VARIABLE)

# Ensure TARGET_VARIABLE is properly parsed
try:
    target_variable = json.loads(TARGET_VARIABLE)
    print(target_variable)
except json.JSONDecodeError as e:
    print(f"Error decoding TARGET_VARIABLE: {e}")

# Function to find the best (p, q) using AIC
def select_varmax_order(endog, exog=None, maxlags=15):
    best_aic = np.inf
    best_order = None
    best_model = None

    for p in range(1,maxlags):
        for q in range(1,maxlags):
            try:
                print(p,q)
                model = sm.tsa.VARMAX(endog, order=(p, q))
                results = model.fit()
                # print('model training complete')
                if results.aic < best_aic:
                    best_aic = results.aic
                    best_order = (p, q)
                    best_model = results
            except Exception as e:
                continue
                print(e)
    print(best_aic)
    return best_order

def fit_model_and_compute_aic(endog):
    try:
        # Fit a model and compute AIC
        # model = sm.tsa.ExponentialSmoothing(df[target_variable], trend='add', seasonal='add', seasonal_periods=12)
        # results = model.fit()
        # aic = results.aic
        # logger.info(f"Successfully computed AIC: {aic}")
        # return aic

        exog = None
        # Find the best order (p, q)
        best_order= select_varmax_order(endog, exog=exog, maxlags=6)

        # Print the best order
        # print(f"The best order is: p={best_order[0]}, q={best_order[1]}")
        # print(best_model.summary())
        # logger.info("Successfully computed p,q")
        return best_order
     
    except Exception as e:
        print(e)

if __name__ == "__main__":
    # logger = get_logger(__name__, logging.INFO)
    try:
        
        response = s3_client.get_object(Bucket=RESULTS_BUCKET, Key=DATA_FILE_PATH)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(csv_content))
        # logger.info("Loaded CSV data successfully.")
        
        endog = df[target_variable]
        # print(endog)
        best_order= fit_model_and_compute_aic(endog)
        
        # Convert the value to string 
        data_to_upload = str(best_order)
        
        result_key = RESULT_FILE_PATH
        # Upload data to S3
        response = s3_client.put_object(
            Bucket=RESULTS_BUCKET,
            Key=result_key,
            Body=data_to_upload.encode('utf-8')  # Convert string to bytes for upload
        )
        # logger.info(f"Computed AIC value: {result_value} has been uploaded to s3://{RESULTS_BUCKET}/{result_key}")

        # print(f"Computed AIC value: {aic_value} has been uploaded to s3://{results_bucket}/{result_key}")
    except Exception as e:
        # logger.error(f"An error occurred: {str(e)}")
        print(e)
