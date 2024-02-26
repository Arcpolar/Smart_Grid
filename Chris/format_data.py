import os
import pandas as pd
import tqdm
import logging
from sklearn import preprocessing


class GridInfoProcessor:
    def __init__(self, data_folder, output_folder):
        self.data_folder = data_folder
        self.archive_folder = data_folder + '/archive/'
        self.output_folder = output_folder
        self.logger = self.setup_logger()
        self.meterdataset = None
        self.weather_hourly_darksky = None
        self.meterconsumptiondata = None
        self.dfholiday = None


    @staticmethod
    def setup_logger():
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        # Create a file handler and set the level to INFO
        file_handler = logging.FileHandler('grid_info_processor.log')
        file_handler.setLevel(logging.INFO)

        # Create a formatter and add it to the handler
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(formatter)

        # Add the handler to the logger
        logger.addHandler(file_handler)

        return logger

    def process_directory(self):
        # Add your directory processing logic here
        # For example, you can list files in the directory

        try:
            files = os.listdir(self.data_folder)

            # Print or do something with the files
            for file in files:
                self.logger.info('Found in given data folder: ' + str(file))

            # load CSV file to data frame meterconsuptiondata
            self.meterconsumptiondata = pd.read_csv(os.path.join(self.data_folder, 'housekWh.csv'),
                                                    index_col='tstp', parse_dates=True)

            # load weather infomation
            self.weather_hourly_darksky = pd.read_csv(os.path.join(self.archive_folder, 'weather_hourly_darksky.csv'),
                                                      index_col='time', parse_dates=True)

            # load uk_bank_holidays.csv
            self.dfholiday = pd.read_csv(os.path.join(self.archive_folder, 'uk_bank_holidays.csv'))


        except OSError as e:
            self.logger.error(f"Critical File Error: {e}")
        except Exception as e:
            self.logger.error(f"Critical Error: {e}")

    def process_S3(self):
        
        housekey = 's3://{}/{}'.format(self.data_folder, 'housekWh.csv')
    
        try:
            # load CSV file to data frame meterconsuptiondata
            self.meterconsumptiondata = pd.read_csv(housekey,
                                                    index_col='tstp', parse_dates=True)
    
            # load weather infomation
            #self.weather_hourly_darksky = pd.read_csv(os.path.join(self.archive_folder, 'weather_hourly_darksky.csv'),
                                                     # index_col='time', parse_dates=True)
    
            # load uk_bank_holidays.csv
            #self.dfholiday = pd.read_csv(os.path.join(self.archive_folder, 'uk_bank_holidays.csv'))
    
            
            return self.meterconsumptiondata
        except OSError as e:
            self.logger.error(f"Critical File Error: {e}")
        except Exception as e:
            self.logger.error(f"Critical Error: {e}")
            
    
    def process_data(self):

        self.logger.info('Starting Data Processing: ')
        self.logger.info('Processing: housekWh')
        
        # Take average of all meters to hourly interval
        houseAVG = self.meterconsumptiondata.mean(axis=1).to_frame()
        houseAVG.columns = ['kWh']
        
        self.logger.info('Processing: weather_hourly_darksky')

        # only take temperature data from weather data
        temperaturedata = self.weather_hourly_darksky['temperature']

        self.logger.info('Processing: uk_bank_holidays')

        # set the time
        self.dfholiday['Bank holidays'] = pd.to_datetime(self.dfholiday['Bank holidays'])
        # change columns name
        self.dfholiday = self.dfholiday.rename(columns={'Bank holidays': 'date', 'Type': 'Holiday'})
        
        self.logger.info('Label encoding: uk_bank_holidays')
        
        # using LabelEncoder() to change to digit
        calendarencoder = preprocessing.LabelEncoder()
        self.dfholiday['Holiday'] = calendarencoder.fit_transform(self.dfholiday['Holiday'])

        self.logger.info('Combining Datasets')
        
        # Use previouse processed average data
        meterdataset = houseAVG.copy()
        # Add new time columns date, weekday, hour
        meterdataset['timestamp'] = meterdataset.index
        meterdataset['date'] = pd.to_datetime(meterdataset.index.date)
        meterdataset['weekday'] = meterdataset.index.weekday
        meterdataset['hour'] = meterdataset.index.hour + meterdataset.index.minute / 60
        # add temperature information
        meterdataset = meterdataset.merge(temperaturedata, left_index=True, right_index=True)
        # add holiday information
        meterdataset = meterdataset.merge(self.dfholiday, on='date', how='left')

        self.logger.info('Setting Parquet Partitions')

        # for parquet use
        meterdataset['year'] = pd.DatetimeIndex(meterdataset['date']).year
        meterdataset['month'] = pd.DatetimeIndex(meterdataset['date']).month

        self.logger.info('Dropping Empty Rows and non-useful columns')

        # drop date column
        meterdataset = meterdataset.drop('date', axis=1)
        # add Holiday column name
        meterdataset['Holiday'] = meterdataset['Holiday'].fillna(-1)
        # set index
        meterdataset.set_index('timestamp', inplace=True)
        # delete NA
        meterdataset = meterdataset.dropna()

        self.meterdataset = meterdataset

    def get_meter_data(self):
        return self.meterdataset

    def send_to_parquet(self):
        self.logger.info('Sending to Parquet Folder: ' + str(self.output_folder))
        self.meterdataset.to_parquet(os.path.join(self.output_folder, 'meterdataset.parquet'),
                                     compression='None', partition_cols=["year", "month"])


# Example usage:
if __name__ == "__main__":
    # Replace 'your_directory_path' with the actual directory path you want to process
    directory_path = './data/'
    output_path = './pq_files/'

    # Create an instance of GridInfoProcessor
    processor = GridInfoProcessor(directory_path, output_path)

    # Process the directory
    processor.process_directory()
    
    processor.process_data()

    processor.send_to_parquet()


