from sklearn.ensemble import IsolationForest
import pandas as pd
import tensorflow as tf
from sklearn.preprocessing import StandardScaler
import numpy as np
from sklearn.cluster import DBSCAN


def check_anomaly(df, metric, a=4, n=5):
    # функция check_anomaly предлагает алгоритм проверки значения на аномальность посредством
    # сравнения интересующего значения c границами межквартильного размаха
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a * df['iqr']
    df['low'] = df['q25'] - a * df['iqr']

    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()

    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0

    return is_alert, df


def check_anomaly_sigm(df, metric, sigma=3):
    mean = df[metric].mean()
    std = df[metric].std()

    upper_limit = mean + sigma * std
    lower_limit = mean - sigma * std

    if df[metric].iloc[-1] > upper_limit or df[metric].iloc[-1] < lower_limit:
        is_alert = 1
    else:
        is_alert = 0

    return is_alert, df


def check_anomaly_isolation_forest(df, metric, outliers_fraction=0.01):
    scaler = StandardScaler()
    np_scaled = scaler.fit_transform(df[[metric]].values.reshape(-1, 1))
    data = pd.DataFrame(np_scaled)

    model = IsolationForest(contamination=outliers_fraction, n_estimators=100)
    model.fit(data)

    df['anomaly'] = model.predict(data)

    return df


# Функция для создания и обучения модели автоэнкодера
def build_autoencoder(input_dim):
    model = tf.keras.models.Sequential([
        tf.keras.layers.Dense(32, activation='relu', input_shape=(input_dim,)),
        tf.keras.layers.Dense(16, activation='relu'),
        tf.keras.layers.Dense(8, activation='relu'),
        tf.keras.layers.Dense(16, activation='relu'),
        tf.keras.layers.Dense(32, activation='relu'),
        tf.keras.layers.Dense(input_dim, activation='sigmoid')
    ])
    model.compile(optimizer='adam', loss='mse')
    return model


# Функция для обнаружения аномалий с помощью автоэнкодера

def check_anomaly_autoencoder(df, metric):
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(df[[metric]].values)

    autoencoder = build_autoencoder(input_dim=scaled_data.shape[1])
    autoencoder.fit(scaled_data, scaled_data, epochs=50, batch_size=64, shuffle=True, validation_split=0.2, verbose=0)

    reconstructed_data = autoencoder.predict(scaled_data)
    mse = np.mean(np.power(scaled_data - reconstructed_data, 2), axis=1)

    df['anomaly'] = mse > np.percentile(mse, 95)

    return df


def check_anomaly_dbscan(df, metric, eps=0.5, min_samples=5):
    X = df[[metric]].values
    dbscan = DBSCAN(eps=eps, min_samples=min_samples)
    df['anomaly'] = dbscan.fit_predict(X)
    return df
