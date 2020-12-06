from collections import defaultdict
import json

import utils


with open("config.json", "r") as f:
    config = json.load(f)
WITHDRAWN_PATH_ID = -1
PREFIXES_IN_ATOM_BATCH_SIZE = config["bgpatom"]["prefixes_in_atom_batch_size"]
FULL_FLEET_PREFIXES_THRESHOLD = config["bgpatom"]["full_fleet_threshold"]


class BGPAtomPeer:
    def __init__(self, peer_address):
        self.peer_address = peer_address
        self.prefixes_count = 0

        self.path_id_to_aspath = dict()
        self.aspath_to_path_id = dict()

        self.prefix_to_aspath = dict()
        self.prefix_to_origin_asn = dict()

    def get_path_id(self, atom_aspath: tuple):
        if atom_aspath not in self.aspath_to_path_id:
            return self.set_new_aspath(atom_aspath)
        return self.aspath_to_path_id[atom_aspath]

    def set_new_aspath(self, atom_aspath: tuple):
        path_id = len(self.aspath_to_path_id)
        self.aspath_to_path_id[atom_aspath] = path_id
        self.path_id_to_aspath[path_id] = atom_aspath
        return path_id

    def get_aspath_by_path_id(self, path_id: int):
        return self.path_id_to_aspath[path_id]

    def update_prefix_status(self, element: dict):
        prefix = element["fields"]["prefix"]
        message_type = element["type"]

        if prefix == "0.0.0.0/0" or prefix == "::/0":
            return

        if message_type == "A" or message_type == "R":
            aspath = element["fields"]["as-path"]
            self.update_announcement_message(prefix, aspath.split(" "))
        elif message_type == "W":
            self.update_withdrawal_message(prefix)

    def update_announcement_message(self, prefix: str, announced_aspath: list):
        non_prepended_aspath = utils.remove_path_prepending(announced_aspath)
        origin_asn = non_prepended_aspath[-1]
        atom_encoded_path = tuple(non_prepended_aspath[:-1])

        path_id = self.get_path_id(atom_encoded_path)
        self.prefix_to_aspath[prefix] = path_id
        self.prefix_to_origin_asn[prefix] = origin_asn

    def update_withdrawal_message(self, prefix):
        self.prefix_to_aspath[prefix] = WITHDRAWN_PATH_ID

    def is_full_fleet(self):
        return self.prefixes_count > FULL_FLEET_PREFIXES_THRESHOLD

    def dump_bgpatom(self, timestamp: int):
        bgpatom = self.construct_bgpatom()

        for path_id in bgpatom:
            prefixes_batch = list()

            for prefix in bgpatom[path_id]:
                prefixes_batch.append(prefix)
                if len(prefixes_batch) > PREFIXES_IN_ATOM_BATCH_SIZE:
                    yield self.format_dump_data(prefixes_batch, path_id, timestamp)
                    prefixes_batch = list()

            if prefixes_batch:
                yield self.format_dump_data(prefixes_batch, path_id, timestamp)

    def construct_bgpatom(self):
        bgpatom = defaultdict(list)
        self.prefixes_count = 0

        for prefix in self.prefix_to_aspath:
            path_id = self.prefix_to_aspath[prefix]
            if path_id == WITHDRAWN_PATH_ID:
                continue
            self.prefixes_count += 1
            origin_asn = self.prefix_to_origin_asn[prefix]
            bgpatom[path_id].append((prefix, origin_asn))

        if not self.is_full_fleet():
            return dict()
        return bgpatom

    def format_dump_data(self, prefixes_batch: list, path_id: int, timestamp: int):
        aspath = self.path_id_to_aspath[path_id]
        return {
            "prefixes": prefixes_batch,
            "aspath": aspath,
            "peer_address": self.peer_address,
            "timestamp": timestamp
        }
