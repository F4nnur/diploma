import base64

import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io
import asyncio
import datetime

from dags.includes.db_connectors.db_connectors import insert_into_clickhouse, Getch
from dags.includes.methods.anomaly_detectors import check_anomaly, check_anomaly_sigm, check_anomaly_isolation_forest, \
    check_anomaly_autoencoder, check_anomaly_dbscan


# from ..db_connectors.db_connectors import Getch, insert_into_clickhouse

# from ..methods.anomaly_detectors import check_anomaly, check_anomaly_sigm, check_anomaly_isolation_forest


async def run_alerts(chat=464674948):
    chat_id = chat or 464674948
    bot = telegram.Bot(token='6552966095:AAGkq7DxCluoE69FtvZgP3n07JGGYPzwsCo')

    data_feed = Getch(''' SELECT
                                  toStartOfFifteenMinutes(time) as ts
                                , toDate(ts) as date
                                , formatDateTime(ts, '%R') as hm
                                , uniqExact(user_id) as users_feed
                                , countIf(user_id, action = 'view') as views
                                , countIf(user_id, action = 'like') as likes
                                , likes / views as CTR
                            FROM diploma.user_feed
                            WHERE ts BETWEEN '2023-05-16' AND '2023-05-17'
                            GROUP BY ts, date, hm
                            ORDER BY ts ''').df

    data = data_feed[['ts', 'date', 'hm', 'users_feed', 'views', 'likes', 'CTR']]

    metric_list = ['users_feed', 'views', 'likes', 'CTR']
    for metric in metric_list:
        print(metric)
        df_copy = data[['ts', 'date', 'hm', metric]].copy()

        is_alert, df_copy = check_anomaly(df_copy, metric)

        if is_alert == 1 or True:
            msg = '''Межквартильный размах.\nВнимание, аномальное значение!\nМетрика {metric}:\nтекущее значение = {current_val:.2f}\nотклонение от предыдущего значения {last_val_diff:.2%}'''.format(
                metric=metric,
                current_val=df_copy[metric].iloc[-1],
                last_val_diff=abs(1 - (df_copy[metric].iloc[-1]) / df_copy[metric].iloc[-2]))

            sns.set(rc={'figure.figsize': (16, 10)})  # задаем размер графика
            plt.tight_layout()

            ax = sns.lineplot(x=df_copy['ts'], y=df_copy[metric], label='metric')
            ax = sns.lineplot(x=df_copy['ts'], y=df_copy['up'], label='up')
            ax = sns.lineplot(x=df_copy['ts'], y=df_copy['low'], label='low')

            for ind, label in enumerate(
                    ax.get_xticklabels()):  # этот цикл нужен чтобы разрядить подписи координат по оси Х,
                if ind % 2 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)

            ax.set(xlabel='time')  # задаем имя оси Х
            ax.set(ylabel=metric)  # задаем имя оси У

            ax.set_title(metric)  # задае заголовок графика
            ax.set(ylim=(0, None))  # задаем лимит для оси У

            # формируем файловый объект
            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()
            img_str = plot_object.getvalue()
            img_base64 = base64.b64encode(img_str).decode('utf-8')

            # отправляем алерт

            await bot.sendMessage(chat_id=chat_id, text=msg)
            await bot.sendPhoto(chat_id=chat_id, photo=plot_object)

            new_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            insert_into_clickhouse({'message': msg, 'image_data': img_base64, 'time': str(new_date)}, 'diploma.charts')

        is_sigma_alert, df_sigma = check_anomaly_sigm(df_copy, metric, sigma=3)

        if is_sigma_alert or True:
            msg_sigma = '''Правило сигм.\nВнимание, аномальное значение!\nМетрика {metric}:\nтекущее значение = {current_val:.2f}'''.format(
                metric=metric,
                current_val=df_sigma[metric].iloc[-1])

            sns.set(rc={'figure.figsize': (16, 10)})  # задаем размер графика
            plt.tight_layout()

            ax = sns.lineplot(x=df_sigma['ts'], y=df_sigma[metric], label='metric')

            ax.set(xlabel='time')
            ax.set(ylabel=metric)

            ax.set_title(metric)
            ax.set(ylim=(0, None))

            plot_object_sigma = io.BytesIO()
            ax.figure.savefig(plot_object_sigma)
            plot_object_sigma.seek(0)
            plot_object_sigma.name = '{0}_sigma.png'.format(metric)
            plt.close()
            img_str_sigma = plot_object_sigma.getvalue()
            img_base64_sigma = base64.b64encode(img_str_sigma).decode('utf-8')

            await bot.sendMessage(chat_id=chat_id, text=msg_sigma)
            await bot.sendPhoto(chat_id=chat_id, photo=plot_object_sigma)

            new_date_sigma = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            insert_into_clickhouse({'message': msg_sigma, 'image_data': img_base64_sigma, 'time': str(new_date_sigma)},
                                   'diploma.sigm')

        is_isolation_forest_alert = check_anomaly_isolation_forest(df_copy, metric)
        if not is_isolation_forest_alert.notna().empty:
            msg_isolation_forest = '''Isolation Forest.\nВнимание, аномальное значение!\nМетрика {metric}:\nтекущее значение = {current_val:.2f}'''.format(
                metric=metric,
                current_val=is_isolation_forest_alert[metric].iloc[-1])

            fig, ax = plt.subplots(figsize=(10, 6))
            normal_data = is_isolation_forest_alert[is_isolation_forest_alert['anomaly'] == 1]
            anomaly_data = is_isolation_forest_alert[is_isolation_forest_alert['anomaly'] == -1]
            ax.plot(normal_data.index, normal_data[metric], color='black', label='Normal')
            ax.scatter(anomaly_data.index, anomaly_data[metric], color='red', label='Anomaly')
            plt.legend()
            plt.show()

            plot_object_iso_forest = io.BytesIO()
            ax.figure.savefig(plot_object_iso_forest)
            plot_object_iso_forest.seek(0)
            plot_object_iso_forest.name = '{0}_iso_forest.png'.format(metric)
            plt.close()
            img_str_iso_forest = plot_object_iso_forest.getvalue()
            img_base64_iso_forest = base64.b64encode(img_str_iso_forest).decode('utf-8')

            await bot.sendMessage(chat_id=chat_id, text=msg_isolation_forest)
            await bot.sendPhoto(chat_id=chat_id, photo=plot_object_iso_forest)

            new_date_iso_forest = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            insert_into_clickhouse({'message': msg_isolation_forest, 'image_data': img_base64_iso_forest,
                                    'time': str(new_date_iso_forest)},
                                   'diploma.iso_forest')

        auoencoder = check_anomaly_autoencoder(df_copy, metric)
        if df_copy[df_copy['anomaly']].shape[0] > 0:
            msg_autoencoder = f'Autoencoder.\nВнимание, аномальное значение!\nМетрика {metric}:\nтекущее значение = {df_copy[metric].iloc[-1]:.2f}'
            fig, ax = plt.subplots(figsize=(10, 6))
            normal_data = df_copy[df_copy['anomaly'] == False]
            anomaly_data = df_copy[df_copy['anomaly'] == True]
            ax.plot(normal_data['ts'], normal_data[metric], color='black', label='Normal')
            ax.scatter(anomaly_data['ts'], anomaly_data[metric], color='red', label='Anomaly')
            ax.legend()
            plt.show()
            plot_object_autoencoder = io.BytesIO()
            ax.figure.savefig(plot_object_autoencoder)
            plot_object_autoencoder.seek(0)
            plot_object_autoencoder.name = f'{metric}_autoencoder.png'
            plt.close()
            img_str_autoencoder = plot_object_autoencoder.getvalue()
            img_base64_autoencoder = base64.b64encode(img_str_autoencoder).decode('utf-8')
            await bot.sendMessage(chat_id=chat_id, text=msg_autoencoder)
            await bot.sendPhoto(chat_id=chat_id, photo=plot_object_autoencoder)
            new_date_autoencoder = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            insert_into_clickhouse({'message': msg_autoencoder, 'image_data': img_base64_autoencoder,
                                    'time': str(new_date_autoencoder)},
                                   'diploma.autoencoder')
        is_dbscan_alert = check_anomaly_dbscan(df_copy, metric)
        if not is_dbscan_alert.empty:
            msg_dbscan = f'DBSCAN.\nВнимание, аномальное значение!\nМетрика {metric}:\nтекущее значение = {df_copy[metric].iloc[-1]:.2f}'
            fig, ax = plt.subplots(figsize=(10, 6))
            normal_data = df_copy[df_copy['anomaly'] == 0]
            anomaly_data = df_copy[df_copy['anomaly'] == -1]
            ax.plot(normal_data['ts'], normal_data[metric], color='black', label='Normal')
            ax.scatter(anomaly_data['ts'], anomaly_data[metric], color='red', label='Anomaly')
            ax.legend()
            plt.show()
            plot_object_dbscan = io.BytesIO()
            ax.figure.savefig(plot_object_dbscan)
            plot_object_dbscan.seek(0)
            plot_object_dbscan.name = f'{metric}_dbscan.png'
            plt.close()
            img_str_dbscan = plot_object_dbscan.getvalue()
            img_base64_dbscan = base64.b64encode(img_str_dbscan).decode('utf-8')
            await bot.sendMessage(chat_id=chat_id, text=msg_dbscan)
            await bot.sendPhoto(chat_id=chat_id, photo=plot_object_dbscan)
            new_date_dbscan = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            insert_into_clickhouse({'message': msg_dbscan, 'image_data': img_base64_dbscan,
                                    'time': str(new_date_dbscan)},
                                   'diploma.dbscan')


asyncio.run(run_alerts())
