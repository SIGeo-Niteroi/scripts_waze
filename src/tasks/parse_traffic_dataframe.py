from utils.agol_layer_methods import add_features_agol, remove_from_agol, update_features_agol
from utils.index import create_ms_timestamp
from utils.portal_layer_methods import build_new_hist_feature, get_layer_on_portal, create_new_feature, generate_portal_token
from prefect.variables import Variable
from prefect import task, get_run_logger
from prefect.blocks.system import Secret
import pandas as pd


@task(name="Parse traffic data")
def parse_traffic_data(data):

    try:
        df_traffic = pd.DataFrame(data['jams'])
        df_traffic = df_traffic.rename(columns={
             'uuid':'tx_uuid',
             'country': 'tx_pais',
             'city': 'tx_cidade',
             'street': 'tx_rua',
             'roadType': 'tx_tipo_via',
             'length': 'li_comprimento',
             'endNode': 'tx_final',
             'speedKMH': 'db_velocidade_kmh',
             'speed': 'db_velocidade',
             'delay': 'li_atraso',
             'level': 'li_nivel',
             'id': 'li_id',             
        })
        create_ms_timestamp(df_traffic,'dt_data_hora')
        """ df_traffic.drop(columns=['subtype', 'type','roadType', 'location', 'city', 'country'], axis=1, inplace=True) """
        return df_traffic



    except Exception as e:
            error_message = str(e)
            raise ValueError(f"Erro durante a execução parse_traffic_data: {error_message}")


