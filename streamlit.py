from streamlit_navigation_bar import st_navbar
import base64
from PIL import Image
import io
import requests
import time
import pandahouse as ph
from streamlit_option_menu import option_menu

import streamlit as st


class Getch:
    def __init__(self, query, db='diploma'):
        self.connection = {
            'host': 'http://localhost:8123',
            'database': db,
        }
        self.query = query
        self.df = None  # Initialize df attribute

    def fetch_data(self):
        try:
            self.df = ph.read_clickhouse(self.query, connection=self.connection)
        except Exception as err:
            print("\033[31m {}".format(err))
            exit(0)


st.set_page_config(page_title="LAD", layout="wide")

pages = ["LAD"]
inputs = []

styles = {
    "nav": {
        "background-color": "royalblue",
        "justify-content": "left",
    },
    "img": {
        "padding-right": "14px",
    },
}

st_navbar(
    pages,
    styles=styles,
)

state = st.session_state

if 'inputs' not in state:
    state.inputs = []

if 'input_values' not in state:
    state.input_values = []


def add_input():
    state.inputs.append(f'Метрика {len(state.inputs) + 1}')
    state.input_values.append('')


username = "airflow"
password = "airflow"
auth_token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("utf-8")


def send_patch_request(is_paused: bool):
    url = "http://localhost:8080/api/v1/dags/air_test"
    headers = {
        "Authorization": f"Basic {auth_token}",
        "Content-Type": "application/json",
    }
    data = {"is_paused": is_paused}
    response = requests.patch(url, headers=headers, json=data)
    if response.status_code == 200:
        notification = st.empty()
        notification.success("Запрос отправлен успешно!")
        time.sleep(2)
        notification.empty()
    else:
        notification = st.empty()
        notification.success(f"Ошибка при отправке запроса: {response.status_code} {response.text}")
        time.sleep(2)
        notification.empty()

url = 'https://t.me/vkr_AFF_bot'
st.write("Зайдите и напишите боту /start чтобы получать уведомления: [ссылка](%s)" % url)
st.text_input('Укажите телеграм айди')
st.text_input('Введите ссылку на базу данных:')

for i, input_name in enumerate(state.inputs):
    state.input_values[i] = st.text_input(input_name, value=state.input_values[i])

st.text_input('Введите с каким промежутком проверять данные на аномалии в формате: минута:час:день')

col1, col2, col3 = st.columns(3)

placeholder = st.empty()


def check_anomaly(db_name):
    # send_patch_request(False)
    while True:
        data_feed = Getch(f'''
                                                SELECT image_data, message, time
                                                FROM {db_name}
                                                ORDER BY time DESC
                                                LIMIT 4
                                            ''')
        data_feed.fetch_data()
        with p_1.container():
            for index, row in data_feed.df.iterrows():
                image_data = row['image_data']
                message = row['message']
                img_binary = base64.b64decode(image_data)
                img = Image.open(io.BytesIO(img_binary))
                st.image(img, caption=message, width=700)
        time.sleep(1)


if 'options' not in state:
    state.options = False

with col1:
    if st.button('Найти аномалии'):
        state.options = True

with col2:
    if st.button('Прекратить отслеживать'):
        state.options = False
        send_patch_request(True)
        placeholder.empty()

with col3:
    if st.button('Добавить инпут', on_click=add_input):
        pass

if state.options:
    page = option_menu("", ['Межквартильный размах', 'Правило сигм', 'Isolation Forest', 'Keras AutoEncoder', 'DBSCAN'],
                       orientation="horizontal")
    placeholder = st.container()
    with placeholder:
        p_1 = st.empty()
        p_2 = st.empty()
        p_3 = st.empty()
        p_4 = st.empty()
        p_5 = st.empty()

    if page == 'Межквартильный размах':
        check_anomaly('diploma.charts')
    elif page == 'Правило сигм':
        check_anomaly('diploma.sigm')
    elif page == 'Isolation Forest':
        check_anomaly('diploma.iso_forest')
    elif page == 'Keras AutoEncoder':
        check_anomaly('diploma.autoencoder')
    elif page == 'DBSCAN':
        check_anomaly('diploma.dbscan')
