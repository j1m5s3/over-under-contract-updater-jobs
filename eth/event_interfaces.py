from typing import Optional, Dict
from web3 import Web3

from .provider.provider import Provider


class EventContractInterface:
    def __init__(self, provider: Provider, contract_address, contract_abi):
        self.provider = provider
        self.w3_contract_handle = self.provider.w3.eth.contract(address=contract_address, abi=contract_abi)
        if self.w3_contract_handle is None:
            raise Exception("Contract not found")
        else:
            self.contract_name = self.w3_contract_handle.functions.getContractName().call()
            self.contract_address = self.w3_contract_handle.address
            self.contract_abi = self.w3_contract_handle.abi
            self.price_mark = Web3.from_wei(self.w3_contract_handle.functions.getPriceMark().call(), 'ether')
            self.asset_symbol = self.w3_contract_handle.functions.getAssetSymbol().call()
            self.betting_close = self.w3_contract_handle.functions.getBettingClose().call()
            self.event_close = self.w3_contract_handle.functions.getEventClose().call()

            self.contract_balance = Web3.from_wei(self.w3_contract_handle.functions.getContractBalance().call(),
                                                  'ether')
            self.over_betters_balance = Web3.from_wei(self.w3_contract_handle.functions.getOverBettersBalance().call(),
                                                      'ether')
            self.under_betters_balance = Web3.from_wei(
                self.w3_contract_handle.functions.getUnderBettersBalance().call(), 'ether'
            )
            self.over_betting_payout_modifier = self.w3_contract_handle.functions.getOverBettingPayoutModifier().call()
            self.under_betting_payout_modifier = self.w3_contract_handle.functions.getUnderBettingPayoutModifier().call()
            self.over_betters_addresses = self.w3_contract_handle.functions.getOverBettersAddresses().call()
            self.under_betters_addresses = self.w3_contract_handle.functions.getUnderBettersAddresses().call()
            self.is_event_over = self.w3_contract_handle.functions.isEventOver().call()

    def get_event_contract_info(self) -> Optional[Dict]:

        contract_info = {
            "contract_name": self.contract_name,
            "contract_address": self.contract_address,
            "contract_abi": self.contract_abi,
            "price_mark": self.price_mark,
            "asset_symbol": self.asset_symbol,
            "betting_close": self.betting_close,
            "event_close": self.event_close,
            "contract_balance": self.contract_balance,
            "over_betters_balance": self.over_betters_balance,
            "under_betters_balance": self.under_betters_balance,
            "over_betting_payout_modifier": self.over_betting_payout_modifier,
            "under_betting_payout_modifier": self.under_betting_payout_modifier,
            "over_betters_addresses": self.over_betters_addresses,
            "under_betters_addresses": self.under_betters_addresses,
            "is_event_over": self.is_event_over
        }

        return contract_info
