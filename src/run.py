#!/usr/bin/env python

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract, ContractDetails
from ibapi.utils import iswrapper
from ibapi.common import TickerId
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta
from itertools import cycle
from time import time, sleep
import os
from distutils.util import strtobool
from pytz import timezone, utc as UTC

LOCAL_TZ = timezone('Asia/Tokyo')
import telnyx

telnyx.api_key = os.env("TELNYX_API_KEY")
TELNYX_PHONE_NUMBER = os.getenv("TELNYX_PHONE_NUMBER")
DESTINATION_PHONE_NUMBER = os.getenv("DESTINATION_PHONE_NUMBER")
                                     
HEADLINE_KEYWORDS = os.getenv("HEADLINE_KEYWORDS","merge,merging,combine,combining,halt,talks,rumour,to list").split(',')

import logging

handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger_tws = logging.getLogger('ibapi')
logger_tws.addHandler(handler)
logger_tws.setLevel(os.getenv("TWS_LOGGER_LEVEL", "ERROR"))
logger_telnyx = logging.getLogger('telnyx')
logger_telnyx.addHandler(handler)
logger_telnyx.setLevel(os.getenv("TELNYX_LOGGER_LEVEL", "ERROR"))



def make_contract(symbol: str, sec_type: str, currency: str,
                  exchange: str) -> Contract:
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.currency = currency
    contract.exchange = exchange
    return contract


class NewsApp(EClient, EWrapper):
    def __init__(self,
                 providers='BZ+BRFG+DJNL+BRFUPDN',
                 concurrency=10,
                 continous=False,
                 limit=10,
                 streaming=False,
                 sms=False):
        if streaming and continous:
            continous = False
        EClient.__init__(self, wrapper=self)
        EWrapper.__init__(self)
        self.request_id = 0
        self.started = False
        self.next_valid_order_id = None
        self.requests = {}
        if not continous and not streaming:
            self.news = defaultdict(list)
        self.pending = set()
        self.search = []
        self.contract_details = []
        self.providers = providers
        self.concurrency = concurrency
        self.continous = continous
        self.streaming = streaming
        self.last_request = 0.0
        self.limit = limit
        self.messages_day = 20
        self.sms = sms

    def rate_limit(self, limit=None):
        if limit is not None:
            min_interval = 1.0 / float(limit)
        else:
            min_interval = 1.0 / float(self.limit)
        elapsed = time() - self.last_request
        wait_time = min_interval - elapsed
        if wait_time > 0:
            sleep(wait_time)
        self.last_request = time()


    def update_search(self):
        FIX

        for symbol, sec_type, currency, exchange in cursor:
            contract = make_contract(symbol, sec_type, currency, exchange)
            search.append(contract)
        if len(search) > self.concurrency:
            self.concurrency = len(search)
        self.search = iter(search)
        for _ in range(self.concurrency):
            self.rate_limit()
            contract = next(self.search)
            reqId = self.next_request_id(contract)
            self.pending.add(reqId)
            self.reqContractDetails(reqId, contract)

    def next_request_id(self, contract: Contract) -> int:
        self.request_id += 1
        self.requests[self.request_id] = contract
        return self.request_id

    @iswrapper
    def historicalNews(self, requestId: int, time: str, providerCode: str,
                       articleId: str, headline: str) -> None:
        super().historicalNews(requestId, time, providerCode, articleId,
                               headline)
        contract = self.requests[requestId].contract
        with self.get_cursor() as cursor:
            cursor.execute(
                "SELECT last_search_start, last_search_end FROM news WHERE symbol=%s AND sec_type=%s AND exchange=%s AND currency=%s;",
                (contract.symbol, contract.secType, contract.exchange,
                 contract.currency))
            last_search_start, last_search_end = cursor.fetchone()
        # UTC to local
        time = UTC.localize(datetime.strptime(time, '%Y-%m-%d %H:%M:%S.%f'),
                            is_dst=None).astimezone(LOCAL_TZ)
        if LOCAL_TZ.localize(last_search_start) - timedelta(days=1) <= time:
            if self.search_text(headline):
                if not self.continous and not self.streaming:
                    self.news[requestId].append(
                        (time, providerCode, articleId, headline))
                self.send_sms(time, providerCode,
                              self.requests[requestId].longName,
                              self.requests[requestId].contract.symbol,
                              headline)  #marketName

    @staticmethod
    def search_text(text):
        text = text.lower()
        if any([True if keyword in text else False for keyword in HEADLINE_KEYWORDS]):
            return True
        else:
            return False

    def send_sms(self, time: datetime, provider: str, name: str, symbol: str,
                 headline: str):
        if '{' in headline and '}' in headline:
            headline = headline.split('}')[1]
        with self.get_cursor() as cursor:
            try:
                cursor.execute("INSERT INTO sms VALUES(%s,%s,%s,%s)",
                               (time, provider, symbol, headline))
            except UniqueViolation:
                return
            if self.sms:
                telnyx.Message.create(
                    from_=TELNYX_PHONE_NUMBER,
                    to=DESTINATION_PHONE_NUMBER,
                    text=
                    f"{time.strftime('%m-%d %H:%M:%S')} | {provider}\n{name}\n{symbol}\n{headline}"
                )
            else:
                logger.info(
                    f"{time.strftime('%m-%d %H:%M:%S')} | {provider}\n{name}\n{symbol}\n{headline}"
                )
            sent_at = datetime.now()
            cursor.execute(
                "UPDATE sms SET sent_at=%s WHERE time=%s AND provider=%s AND symbol=%s AND headline=%s;",
                (sent_at, time, provider, symbol, headline))
            cursor.execute(
                "SELECT COUNT(1) FROM sms WHERE sent_at BETWEEN %s AND %s;",
                (sent_at - timedelta(hours=24), sent_at))
            messages = cursor.fetchone()[0]
            if messages > self.messages_day:
                logger.warning(
                    f'More messages ({messges}) per day than expected ({self.messages_day}), sleeping one hour'
                )
                sleep(3600)

    @iswrapper
    def historicalNewsEnd(self, requestId: int, hasMore: bool) -> None:
        super().historicalNewsEnd(requestId, hasMore)
        self.pending.remove(requestId)
        if not self.streaming:
            self.requests.pop(requestId)
        self.next_search()
        if not self.continous:
            if not self.streaming:
                if len(self.pending) == 0:
                    logger.info(f"All requests complete.")
                    if len(self.news) == 0:
                        logger.info('No news')
                    else:
                        logger.info(self.news)
                    self.disconnect()

    @iswrapper
    def connectAck(self):
        logger.info("Connected")

    @iswrapper
    def nextValidId(self, order_id: int):
        super().nextValidId(order_id)
        self.next_valid_order_id = order_id
        logger.debug(f"nextValidId: {order_id}")
        self.update_search()

    def start(self):
        if self.started:
            return
        self.started = True
        if self.continous:
            self.contract_details = cycle(self.contract_details)
        else:
            self.contract_details = iter(self.contract_details)
        for _ in range(self.concurrency):
            self.next_search()

    @iswrapper
    def tickNews(self, tickerId: int, timeStamp: int, providerCode: str,
                 articleId: str, headline: str, extraData: str):
        super().tickNews(tickerId, timeStamp, providerCode, articleId,
                         headline, extraData)
        time = LOCAL_TZ.localize(datetime.utcfromtimestamp(timeStamp / 1000.0))
        if LOCAL_TZ.localize(datetime.now()) - timedelta(
                hours=1) <= time and self.search_text(headline):
            self.send_sms(time, providerCode, self.requests[tickerId].longName,
                          self.requests[tickerId].contract.symbol,
                          headline)  # marketName

    def next_search(self):
        self.rate_limit()
        try:
            contractDetails = next(self.contract_details)
            if self.streaming:
                logger.info(
                    f'Start stream for {contractDetails.contract.symbol}')
                reqId = self.next_request_id(contractDetails)
                self.reqMktData(reqId, contractDetails.contract, "mdoff,292",
                                False, False, [])
            reqId = self.next_request_id(contractDetails)
            self.pending.add(reqId)
            with self.get_cursor() as cursor:
                cursor.execute(
                    "SELECT last_search_end FROM news WHERE symbol=%s AND sec_type=%s AND exchange=%s AND currency=%s;",
                    (contractDetails.contract.symbol,
                     contractDetails.contract.secType,
                     contractDetails.contract.exchange,
                     contractDetails.contract.currency))
                last_search_start = cursor.fetchone()[0]
                if last_search_start is None:
                    last_search_start = (
                        datetime.now() -
                        timedelta(days=4)).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    last_search_start = last_search_start.strftime(
                        '%Y-%m-%d %H:%M:%S')
                last_search_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.reqHistoricalNews(reqId, contractDetails.contract.conId,
                                       self.providers, last_search_start, '',
                                       10, [])
                cursor.execute(
                    "UPDATE news SET last_search_start=%s, last_search_end=%s WHERE symbol=%s AND sec_type=%s AND exchange=%s AND currency=%s;",
                    (last_search_start, last_search_end,
                     contractDetails.contract.symbol,
                     contractDetails.contract.secType,
                     contractDetails.contract.exchange,
                     contractDetails.contract.currency))
        except StopIteration:
            if not self.streaming:
                logger.info('Finished')
            pass

    @iswrapper
    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        self.contract_details.append(contractDetails)

    @iswrapper
    def contractDetailsEnd(self, reqId: int):
        super().contractDetailsEnd(reqId)
        self.pending.remove(reqId)
        self.requests.pop(reqId)
        if len(self.pending) < self.concurrency:
            for _ in range(self.concurrency - len(self.pending)):
                try:
                    contract = next(self.search)
                    reqId = self.next_request_id(contract)
                    self.pending.add(reqId)
                    self.reqContractDetails(reqId, contract)
                except StopIteration:
                    if len(self.pending) == 0:
                        self.start()
                        break


def main():

    app = NewsApp(continous=False,
        streaming=True,
        sms=strtobool(os.getenv("SMS","false")))
    app.connect(os.getenv("TWS_HOST","127.0.0.1"), int(os.getenv("TWS_PORT",4001)), clientId=int(os.getenv("TWS_CLIENT_ID",0)))
    app.run()


if __name__ == "__main__":
    main()
