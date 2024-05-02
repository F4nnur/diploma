import pandahouse as ph
import clickhouse_connect


def insert_into_clickhouse(data, db_name):
    try:
        client = clickhouse_connect.get_client(host='localhost', port=8123)
        keys = tuple(data.keys())
        keys_str = ', '.join(keys)
        vals = f'({keys_str})'
        query = f"""
                INSERT INTO {db_name}
                {vals}
                VALUES
                {tuple(data.values())}
                """
        client.query(query)
    except Exception as e:
        print(e)


class Getch:
    def __init__(self, query, db='diploma'):
        self.connection = {
            'host': 'http://localhost:8123',
            # 'password': 'qazdsa2001',
            # 'user': 'default',
            'database': db,
        }
        self.query = query
        self.getchdf

    @property
    def getchdf(self):
        try:
            self.df = ph.read_clickhouse(self.query, connection=self.connection)

        except Exception as err:
            print("\033[31m {}".format(err))
            exit(0)
