import time
from datetime import datetime
from typing import Dict, List

from db.schemas.event_schemas import ContractInfoModel
from eth.event_interfaces import EventContractInterface


class EventUpdaterJobs:

    def __init__(self, job_configs, provider_handler, mongo_handler):
        self.job_configs = job_configs
        self.provider_handler = provider_handler
        self.mongo_handler = mongo_handler

    def job_runner(self, run_indefinitely=True):
        while run_indefinitely:
            try:
                for job in self.job_configs:
                    if job["job_type"] == "betting_event_6h":
                        self.__update_event_job(job_config=job,
                                                provider_handler=self.provider_handler,
                                                mongo_handler=self.mongo_handler)
                    elif job["job_type"] == "betting_event_12h":
                        pass
                    elif job["job_type"] == "betting_event_24h":
                        pass
                    else:
                        raise Exception("Invalid job type")
                print("Job runner sleeping for 2 hrs...")
                time.sleep(86400 / 12)
            except Exception as e:
                print(e)
                run_indefinitely = False

        return

    @classmethod
    def __update_event_job(cls, job_config, provider_handler, mongo_handler):
        for asset in job_config["params"]:
            try:
                params = job_config['params'][asset]
                completed_to_be_updated_event_records = mongo_handler.find(
                    collection=params["collection_name"],
                    query={"is_event_over": False,
                           "event_close": {"$lt": datetime.now().timestamp()},
                           "asset_symbol": asset
                           }
                )
                print(
                    f"Found {len(list(completed_to_be_updated_event_records.clone()))} {asset} {params['collection_name']} "
                    f"completed events to be updated... "

                )
                for event_info in completed_to_be_updated_event_records:
                    event_contract_interface = EventContractInterface(provider=provider_handler,
                                                                      contract_address=event_info["contract_address"],
                                                                      contract_abi=event_info["contract_abi"])

                    current_asset_price = cls._get_current_asset_price(mongo_handler=mongo_handler, asset_symbol=asset)
                    event_contract_interface.event_completion_operations(price_at_close=current_asset_price)

                    event_status = event_contract_interface.check_contract_status()
                    if event_status["is_event_over"]:
                        current_contract_address = event_status["contract_address"]
                        record_updated = cls.__update_event_record(mongo_handler=mongo_handler,
                                                                   collection_name=params["collection_name"],
                                                                   current_contract_address=current_contract_address,
                                                                   current_contract_info=event_status)

                    pass
            except Exception as e:
                raise e

        return

    @classmethod
    def __update_event_record(cls, mongo_handler, collection_name, current_contract_address, current_contract_info):
        update_record = ContractInfoModel(**current_contract_info)
        update_result = mongo_handler.update(collection=collection_name,
                                             query={"contract_address": current_contract_address},
                                             document={"$set": update_record.dict()})

        if update_result.acknowledged:
            print(f"Event {collection_name} {current_contract_info['asset_symbol']} record updated")
        else:
            print(f"Event {collection_name} {current_contract_info['asset_symbol']} record update failed")
            raise Exception("Event record update failed")

        return update_result.acknowledged

    @classmethod
    def _get_current_asset_price(cls, mongo_handler, asset_symbol):
        if asset_symbol == "BTC":
            mongo_response = mongo_handler.find_one(collection='btc_live_price',
                                                    query=[("timestamp", -1)])
            return mongo_response['price']
        elif asset_symbol == "ETH":
            mongo_response = mongo_handler.find_one(collection='eth_live_price',
                                                    query=[("timestamp", -1)])
            return mongo_response['price']
        else:
            raise Exception("Invalid asset symbol")
