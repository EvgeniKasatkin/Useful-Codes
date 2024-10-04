from dotenv import load_dotenv, find_dotenv
import pandas as pd
import os
from ga4mp import GtagMP

load_dotenv(find_dotenv())

class MeasurmentProtocolInsert:
    def __init__(self, measurment_id, secret_key, dict_events):
        self.measurment_id = measurment_id
        self.secret_key = secret_key
        self.dict_events = dict_events


    def insert_event(self):
        for index_, cid in enumerate(list(self.dict_events.keys())):
            events = []
            client_id = str(cid)
            ga = GtagMP(measurement_id = self.measurment_id, api_secret = self.secret_key, client_id = client_id)
            event_type = os.getenv('event_type')
            example_event = ga.create_new_event(name = event_type)

            example_event.set_event_param(name = "event_param_name_1", value = list(self.dict_events.values())[index_]['param_name_1'])
            example_event.set_event_param(name = "event_param_name_2", value = list(self.dict_events.values())[index_]['param_name_2'])

            events = [example_event]
            ga.send(events)


if __name__ == "__main__":
    MeasurmentProtocolInsert(os.getenv('measurment_id'), os.getenv('measurment_protocol_key'), os.getenv('dict_events')).insert_event()