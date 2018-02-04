"""Data processing functions."""

import pandas as pd
import numpy as np
import os
import json
import datetime


WEEK = 20
DATA_DIR = 'data/'
timesteps = 100


with open('symbols.json', 'r') as f:
    SYMBOLS = json.load(f)


def to_dataset():
    """Concatenates partials & exports to a Multiindex CSV file."""
    frames = []
    for symbol in SYMBOLS:
        # The most recent value of every API call is an aggregated value up to that point,
        # so it is omitted.
        tick_partials = [
            pd.read_csv(DATA_DIR + 'partials_' + str(i) + '/' + symbol)
            .sort_values('timestamp').iloc[:-1] for i in range(1, WEEK + 1)
        ]
        tick_concatenated = pd.concat(tick_partials, axis=0, ignore_index=True)\
            .drop_duplicates(subset='timestamp').sort_values('timestamp')
        tick_concatenated.timestamp = pd.to_datetime(tick_concatenated.timestamp)
        tick_concatenated = tick_concatenated.set_index('timestamp')

        # A few stocks are traded in the evening,
        # so these exchanges are omitted from the dataset.
        min_time = datetime.datetime.strptime('09:30:00', '%H:%M:%S').time()
        max_time = datetime.datetime.strptime('16:00:00', '%H:%M:%S').time()
        frames.append(tick_concatenated.between_time(min_time, max_time))

    dataset = pd.concat(frames, axis=1, keys=SYMBOLS)

    print('nan:', dataset.isnull().sum())
    dataset.to_csv(DATA_DIR + 'dataset.csv')


def to_numpy(normalize=True):
    """Data preparation for truncated backpropagation.

    Data are normalized with Min-Max scaling.
    All price values use a common min and max value.
    """
    usecols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    datacols = ['open', 'high', 'low', 'close', 'volume']
    labelcols = ['close']
    price_cols = ['open', 'high', 'low', 'close']
    volume_cols = ['volume']

    dataset = pd.read_csv(DATA_DIR + 'dataset.csv', index_col=0, header=[0, 1]).sort_index(axis=1)
    dataset = dataset.loc[:, (slice(None), usecols)].fillna(method='ffill').fillna(method='bfill')

    if normalize or not os.path.isfile('scaler.json'):
        prices = dataset.xs(price_cols[0], level=1, axis=1)
        volumes = dataset.xs(volume_cols[0], level=1, axis=1)
        scaler = {
            'price_max': prices.max().max(),
            'price_min': prices.min().min(),
            'volume_max': volumes.max().max(),
            'volume_min': volumes.min().min(),
        }
        with open('scaler.json', 'w') as f:
            json.dump(scaler, f)
    else:
        with open('scaler.json', 'r') as f:
            scaler = json.load(f)

    def normalize_values(values, min_value, max_value):
        return (values - min_value) / (max_value - min_value)

    for symbol in SYMBOLS:
        dataset.loc[:, (symbol, price_cols)] = normalize_values(
            dataset.loc[:, (symbol, price_cols)],
            scaler['price_min'],
            scaler['price_max']
        )
        dataset.loc[:, (symbol, volume_cols)] = normalize_values(
            dataset.loc[:, (symbol, volume_cols)],
            scaler['volume_min'],
            scaler['volume_max']
        )

    start = dataset.shape[0] - (int((dataset.shape[0]-1)/timesteps) * timesteps) - 1
    data = dataset.iloc[start:-1].loc[:, (slice(None), datacols)].as_matrix().astype(np.float32)
    labels = dataset.iloc[start+1:].loc[:, (slice(None), labelcols)].as_matrix().astype(np.float32)
    data_reshaped = data.reshape((-1, timesteps, data.shape[1]))
    labels_reshaped = labels.reshape((-1, timesteps, labels.shape[1]))
    np.save(DATA_DIR + 'data.npy', data_reshaped)
    np.save(DATA_DIR + 'labels.npy', labels_reshaped)


print('Converting to dataset...')
to_dataset()
print('Converting to array...')
to_numpy(normalize=False)
