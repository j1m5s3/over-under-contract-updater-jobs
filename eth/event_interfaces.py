from typing import Optional, Dict
from web3 import Web3

from .provider.provider import Provider


class EventContractInterface:
    def __init__(self, provider: Provider, contract_address, contract_abi):
        self.provider = provider
        self.w3_contract_handle = self.provider.w3.eth.contract(address=contract_address, abi=contract_abi)
        if self.w3_contract_handle is None:
            raise Exception("Contract not found")
        self.contract_name = self.__get_contract_name(self.w3_contract_handle)
        self.asset_symbol = self.__get_contract_asset_symbol(self.w3_contract_handle)

    @classmethod
    def __set_price_at_close(cls, price_at_close, w3_contract_handle, provider):
        Web3.from_wei(price_at_close, 'ether')
        tx_hash = w3_contract_handle.functions.setPriceAtClose(price_at_close).transact()
        return provider.w3.eth.waitForTransactionReceipt(tx_hash)

    @classmethod
    def __set_winners(cls, w3_contract_handle, provider):
        tx_hash = w3_contract_handle.functions.setWinners().transact()
        return provider.w3.eth.waitForTransactionReceipt(tx_hash)

    @classmethod
    def __check_is_over(cls, w3_contract_handle):
        return w3_contract_handle.functions.isEventOver().call()

    @classmethod
    def __distribute_remaining_winnings(cls, w3_contract_handle, provider):
        tx_hash = w3_contract_handle.functions.destroyContract().transact()
        return provider.w3.eth.waitForTransactionReceipt(tx_hash)

    @classmethod
    def __get_contract_balance(cls, w3_contract_handle):
        return w3_contract_handle.functions.getContractBalance().call()

    @classmethod
    def __get_winning_betters_addresses(cls, w3_contract_handle):
        return w3_contract_handle.functions.getWinningBettersAddresses().call()

    @classmethod
    def __get_over_betters_balance(cls, w3_contract_handle):
        return w3_contract_handle.functions.getOverBettersBalance().call()

    @classmethod
    def __get_under_betters_balance(cls, w3_contract_handle):
        return w3_contract_handle.functions.getUnderBettersBalance().call()

    @classmethod
    def __get_contract_name(cls, w3_contract_handle):
        return w3_contract_handle.functions.getContractName().call()

    @classmethod
    def __get_contract_asset_symbol(cls, w3_contract_handle):
        return w3_contract_handle.functions.getAssetSymbol().call()

    @classmethod
    def __get_under_betting_payout_modifier(cls, w3_contract_handle):
        return w3_contract_handle.functions.getUnderBettingPayoutModifier().call()

    @classmethod
    def __get_over_betting_payout_modifier(cls, w3_contract_handle):
        return w3_contract_handle.functions.getOverBettingPayoutModifier().call()

    @classmethod
    def __get_betting_fee(cls, w3_contract_handle):
        return w3_contract_handle.functions.getBettingFee().call()

    def event_completion_operations(self, price_at_close):
        try:
            price_at_close_receipt = self.__set_price_at_close(price_at_close,
                                                               self.w3_contract_handle,
                                                               self.provider)
            winners_receipt = self.__set_winners(self.w3_contract_handle, self.provider)

        except Exception as e:
            raise Exception(f"Event close operations failed: {e}")

        return {"price_at_close_receipt: ", price_at_close_receipt, "winners_receipt: ", winners_receipt}

    def distribute_remaining_funds(self):
        try:
            txn_receipt = self.__distribute_remaining_winnings(self.w3_contract_handle, provider=self.provider)
        except Exception as e:
            raise Exception(
                f"Event {self.contract_name} {self.asset_symbol} failed to distribute remaining funds: {e}"
            )

        return {"distribute_remaining_receipt": txn_receipt}

    def check_contract_status(self) -> Optional[Dict]:
        try:
            is_event_over = self.__check_is_over(self.w3_contract_handle)

            if is_event_over is not None:
                print(f"Checked event {self.contract_name} {self.asset_symbol} status")
            else:
                print(f"Unable to check event {self.contract_name} {self.asset_symbol} status")
                raise Exception("Event status check failed")
        except Exception as e:
            raise Exception(f"Event status check failed: {e}")

        return {"is_event_over": is_event_over}

    def check_event_completion_stats(self):
        try:
            winning_betters_addresses = self.__get_winning_betters_addresses(self.w3_contract_handle)
            contract_balance = self.__get_contract_balance(self.w3_contract_handle)
            over_betters_balance = self.__get_over_betters_balance(self.w3_contract_handle)
            under_betters_balance = self.__get_under_betters_balance(self.w3_contract_handle)
            over_betting_payout_modifier = self.__get_over_betting_payout_modifier(self.w3_contract_handle)
            under_betting_payout_modifier = self.__get_under_betting_payout_modifier(self.w3_contract_handle)
            betting_fee = self.__get_betting_fee(self.w3_contract_handle)

        except Exception as e:
            raise Exception(f"Event totals check failed: {e}")

        print(f"Checked event {self.contract_name} {self.asset_symbol} stats")

        return {
            "winning_betters_addresses": winning_betters_addresses,
            "contract_balance": contract_balance,
            "over_betters_balance": over_betters_balance,
            "under_betters_balance": under_betters_balance,
            "over_betting_payout_modifier": over_betting_payout_modifier,
            "under_betting_payout_modifier": under_betting_payout_modifier,
            "betting_fee": betting_fee
        }

