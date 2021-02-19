from collections import defaultdict
import json
import logging
from concurrent.futures import ThreadPoolExecutor

from scipy import stats

from hege.bcscore.bcscore_loader import BCSCORELoader
from hege.utils.utils import str_datetime_to_timestamp

class HegeBuilderHelper:
    def __init__(self, collectors: list, timestamp: int, prefix_mode=False, partition_id=None):
        self.collectors = collectors
        self.timestamp = timestamp
        self.prefix_mode = prefix_mode
        self.partition_id = partition_id
        self.peer_asn_set = set()
        self.total_peer_asn_count = 0

        self.bc_score_list = defaultdict(lambda: defaultdict(list))
        self.hegemony_score = defaultdict(dict)

    def build_hegemony_score(self):
        # Use multiple thread to fetch data concurently
        with ThreadPoolExecutor() as tpool:
            res = tpool.map(self.read_data_for_as_hegemony, self.collectors)
            # Needed to log if an exception is raised
            for r in res:
                pass
        #for collector in self.collectors:
        #    self.read_data_for_as_hegemony(collector)
        self.calculate_hegemony()


    def read_data_for_as_hegemony(self, collector: str):
        loaded_bcscore = self.load_bcscore(collector, self.partition_id)

        logging.debug(f"start analyzing {collector}'s bcscore")
        for scope in loaded_bcscore:
            depended_ases_bcscore = loaded_bcscore[scope]
            for depended_as in depended_ases_bcscore:
                for peer_asn, as_bcscore in depended_ases_bcscore[depended_as]:
                    self.peer_asn_set.add(peer_asn)
                    self.bc_score_list[scope][depended_as].append(as_bcscore)
        logging.debug(f"complete analyzing {collector}'s bcscore")

    def load_bcscore(self, collector: str, partition_id=None):
        logging.debug(f"read {collector}'s bcscore")
        loaded_bcscore = BCSCORELoader(collector, self.timestamp, self.prefix_mode, partition_id).load_data()
        logging.debug(f"successfully read collector {collector}'s bcscore; {len(loaded_bcscore)} scopes data loaded")
        return loaded_bcscore

    def calculate_hegemony(self):
        logging.info("start calculating hegemony score")
        self.total_peer_asn_count = len(self.peer_asn_set)

        logging.info(f"total number of peer asn: {self.total_peer_asn_count}")
        # Some numpy function release the GIL
        with ThreadPoolExecutor() as tpool:
            res = tpool.map(self.calculate_hegemony_helper, self.bc_score_list.items(), chunksize=1000)
            # Needed to log if an exception is raised
            for r in res:
                pass
            
        # for scope in self.bc_score_list:
            # self.calculate_hegemony_helper(scope)
        logging.info("complete calculating hegemony score")

    def calculate_hegemony_helper(self, args):
        # scope_bc_score_list = self.bc_score_list[scope]
        # if self.prefix_mode:
        #     total_asn_count = max([len(scope_bc_score_list[asn]) for asn in scope_bc_score_list])
        # else:
        #    total_asn_count = len(scope_bc_score_list[scope])

        args = scope, scope_bc_score_list

        for asn in scope_bc_score_list:
            peer_asn_count = len(scope_bc_score_list[asn])
            peers_bc_score_list = [0] * (self.total_peer_asn_count - peer_asn_count) + scope_bc_score_list[asn]
            hege_score = float(stats.trim_mean(peers_bc_score_list, 0.1))
            if hege_score != 0:
                self.hegemony_score[scope][asn] = hege_score



if __name__ == "__main__":
    FORMAT = '%(asctime)s %(name)s %(message)s'
    logging.basicConfig(
        format=FORMAT, level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S'
    )

    bcscore_time_string = "2020-08-01T00:00:00"
    bcscore_timestamp = str_datetime_to_timestamp(bcscore_time_string)

    test_collectors = ["rrc00", "rrc10", "route-views.linx", "route-views2"]
    hege_builder = HegeBuilderHelper(test_collectors, bcscore_timestamp)
    hege_builder.build_hegemony_score()

    with open("/app/test-as-hegemony-builder-result.json", "w") as f:
        json.dump(hege_builder.hegemony_score, f, indent=4)
