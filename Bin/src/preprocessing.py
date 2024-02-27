import os
import pandas as pd
from sklearn import preprocessing

def load_and_process_weather_data(datapath):
    weather_hourly_darksky = pd.read_csv(os.path.join(datapath, 'weather_hourly_darksky.csv'), index_col='time', parse_dates=True)
    temperaturedata = weather_hourly_darksky['temperature']
    return temperaturedata

def load_and_process_holiday_data(datapath):
    dfholiday = pd.read_csv(os.path.join(datapath, 'uk_bank_holidays.csv'))
    dfholiday['Bank holidays'] = pd.to_datetime(dfholiday['Bank holidays'])
    dfholiday = dfholiday.rename(columns={'Bank holidays': 'date', 'Type': 'Holiday'})
    calendarencoder = preprocessing.LabelEncoder()
    dfholiday['Holiday'] = calendarencoder.fit_transform(dfholiday['Holiday'])
    return dfholiday

def process_energy_data(df):
    houseAVG = df.mean(axis=1).to_frame()
    houseAVG.columns = ['kWh']
    return houseAVG

def merge_data(houseAVG, temperaturedata, dfholiday):
    meterdataset = houseAVG.copy()
    meterdataset['timestamp'] = meterdataset.index
    meterdataset['date'] = pd.to_datetime(meterdataset.index.date)
    meterdataset['weekday'] = meterdataset.index.weekday
    meterdataset['hour'] = meterdataset.index.hour + meterdataset.index.minute / 60
    meterdataset = meterdataset.merge(temperaturedata, left_index=True, right_index=True, how='left')
    meterdataset = meterdataset.merge(dfholiday, on='date', how='left')
    meterdataset = meterdataset.drop('date', axis=1)
    meterdataset['Holiday'] = meterdataset['Holiday'].fillna(-1)
    meterdataset['year'] = pd.DatetimeIndex(meterdataset['timestamp']).year
    meterdataset['month'] = pd.DatetimeIndex(meterdataset['timestamp']).month
    meterdataset.set_index('timestamp', inplace=True)
    meterdataset = meterdataset.dropna()
    return meterdataset

def split_data(meterdataset):
    # Split the data into training, validation, and testing sets
    traindata = meterdataset.loc['2013-01':'2013-12'].copy()
    validationdata = meterdataset.loc['2014-01':].copy()
    testdata = meterdataset.loc['2014-02':].copy()

    return traindata, validationdata, testdata

if __name__ == "__main__":
    base_dir = "/opt/ml/processing"

    # Load data
    temperaturedata = load_and_process_weather_data(os.path.join(base_dir, "weather"))
    dfholiday = load_and_process_holiday_data(os.path.join(base_dir, "holidays"))

    # Load energy data
    df = pd.read_csv(os.path.join(base_dir, 'dataT', 'dataT.csv'), index_col='time', parse_dates=True)

    houseAVG = process_energy_data(df)
    meterdataset = merge_data(houseAVG, temperaturedata, dfholiday)

    # Split data
    traindata, validationdata, testdata = split_data(meterdataset)

    # Save processed data
    output_dir = os.path.join(base_dir, 'output')
    os.makedirs(output_dir, exist_ok=True)
    traindata.to_csv(os.path.join(output_dir, 'train_data.csv'))
    validationdata.to_csv(os.path.join(output_dir, 'validation_data.csv'))
    testdata.to_csv(os.path.join(output_dir, 'test_data.csv'))
