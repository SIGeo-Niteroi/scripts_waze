import os
from dotenv import load_dotenv
from utils.process_data import process_data
from utils.get_api_data import get_api_data_as_json
from utils.send_email import send_email_error
from utils.registers import clear_old_registers, add_registers
from utils.get_layer_on_arcgis import get_a_layer_index

load_dotenv()

credentials_to_send_error_email = {
    "sender_email_address": os.getenv('SENDER_EMAIL_ADDRESS'),
    "sender_email_password": os.getenv('SENDER_EMAIL_PASSWORD'),
    "recipient_email_address": os.getenv('SENDER_EMAIL_ADDRESS'),
}

credentials_to_get_layer_on_arcgis = {
    "agol_username": os.getenv("AGOL_USERNAME"),
    "agol_password": os.getenv("AGOL_PASSWORD"),
    "layer_id": os.getenv("LAYER_ID"),
}

WAZE_PARTNER_HUB_API_URL = os.getenv("WAZE_PARTNER_HUB_API_URL")


def update_layers_on_arcgis(traffic_layer, df):
    errors = []

    registers_removed = clear_old_registers(traffic_layer)
    if isinstance(registers_removed, str):
        errors.append(registers_removed)

    registers_added = add_registers(df, traffic_layer)
    if isinstance(registers_added, str):
        errors.append(registers_added)

    return errors


def main():
    errors = []

    try:
        json_response = get_api_data_as_json(WAZE_PARTNER_HUB_API_URL)

        df = process_data(json_response)

        traffic_layer = get_a_layer_index(
            credentials_to_get_layer_on_arcgis, 2)

        errors += update_layers_on_arcgis(traffic_layer, df)

    except Exception as e:
        errors.append(str(e))

    finally:
        if errors:
            send_email_error(credentials_to_send_error_email, errors)


if __name__ == "__main__":
    main()
