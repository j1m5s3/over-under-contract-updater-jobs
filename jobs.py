import time
import traceback

from datetime import datetime
from typing import Dict, List

from db.mongo_interface import MongoInterface
from db.schemas.event_schemas import ContractInfoModel, ContractUpdateModel
from eth.event_interfaces import EventContractInterface
from eth.provider.provider import Provider


class EventUpdaterJobs:

    def __init__(self, job_configs: List,
                 provider_handler: Provider,
                 mongo_handler: MongoInterface):
        self.job_configs = job_configs
        self.provider_handler = provider_handler
        self.mongo_handler = mongo_handler

    def job_runner(self, is_test: bool, run_indefinitely=True):
        while run_indefinitely:
            try:
                if not is_test:
                    for job in self.job_configs:
                        if job["job_type"] == "betting_event_6h":
                            self.__update_event_job(job_config=job,
                                                    provider_handler=self.provider_handler,
                                                    mongo_handler=self.mongo_handler)
                        elif job["job_type"] == "betting_event_12h":
                            pass
                        elif job["job_type"] == "betting_event_24h":
                            pass

                    print("Job runner sleeping for 2 hrs...")
                    time.sleep(86400 / 12)
                else:
                    for job in self.job_configs:
                        if job["job_type"] == "betting_event_test":
                            self.__update_event_job(job_config=job,
                                                    provider_handler=self.provider_handler,
                                                    mongo_handler=self.mongo_handler)

                    print("Job test runner sleeping for 5 min...")
                    time.sleep(300)
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
                        contract_record_updates = event_contract_interface.check_event_stats()
                        contract_record_updates["is_event_over"] = event_status["is_event_over"]

                        contract_address = event_info["contract_address"]

                        record_updated = cls.__update_event_record(mongo_handler=mongo_handler,
                                                                   collection_name=params["collection_name"],
                                                                   current_contract_address=contract_address,
                                                                   current_contract_info=contract_record_updates,
                                                                   asset_symbol=asset)
                        if record_updated:
                            print(f"Event {params['collection_name']} {asset} {contract_address} record updated")

                    ongoing_event_records = mongo_handler.find(
                        collection=params["collection_name"],
                        query={"is_event_over": False,
                               "event_close": {"$gt": datetime.now().timestamp()},
                               "asset_symbol": asset
                               }
                    )

                    if len(list(ongoing_event_records.clone())) == 0:
                        print(f"No ongoing {params['collection_name']} {asset} events found")
                    else:
                        for ongoing_event_info in ongoing_event_records:
                            contract_address = ongoing_event_info["contract_address"]
                            contract_abi = ongoing_event_info["contract_abi"]
                            event_contract_interface = EventContractInterface(provider=provider_handler,
                                                                              contract_address=contract_address,
                                                                              contract_abi=contract_abi)
                            contract_record_updates = event_contract_interface.check_event_stats()

                            record_updated = cls.__update_event_record(mongo_handler=mongo_handler,
                                                                       collection_name=params["collection_name"],
                                                                       current_contract_address=contract_address,
                                                                       current_contract_info=contract_record_updates,
                                                                       asset_symbol=asset)

                            if record_updated:
                                print(f"Event {params['collection_name']} {asset} {contract_address} record updated")

            except Exception as e:
                print(f"Error: {traceback.format_exc()}")
                raise e

        return

    @classmethod
    def __update_event_record(cls,
                              mongo_handler,
                              collection_name,
                              current_contract_address,
                              current_contract_info,
                              asset_symbol):
        update_record = ContractUpdateModel(**current_contract_info)
        update_result = mongo_handler.update(collection=collection_name,
                                             query={"contract_address": current_contract_address},
                                             document={"$set": update_record.dict()})

        if update_result.acknowledged:
            print(f"Event {collection_name} {asset_symbol} record updated")
        else:
            print(f"Event {collection_name} {asset_symbol} record update failed")
            raise Exception("Event record update failed")

        return update_result.acknowledged

    @classmethod
    def _get_current_asset_price(cls, mongo_handler, asset_symbol):
        if asset_symbol == "BTC":
            mongo_response = mongo_handler.find_one_sorted(collection='btc_live_price',
                                                           query=[("timestamp", -1)])
            return mongo_response['price']
        elif asset_symbol == "ETH":
            mongo_response = mongo_handler.find_one_sorted(collection='eth_live_price',
                                                           query=[("timestamp", -1)])
            return mongo_response['price']
        else:
            raise Exception("Invalid asset symbol")


class EventDataJobs:

    def __init__(self, job_configs: List,
                 provider_handler: Provider,
                 mongo_handler: MongoInterface):
        self.job_configs = job_configs
        self.provider_handler = provider_handler
        self.mongo_handler = mongo_handler
