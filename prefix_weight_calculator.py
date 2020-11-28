from collections import defaultdict
import prefix_encoder


def encode_and_group_prefixes_by_subnet(prefixes_list: list):
    prefixes_by_subnet = defaultdict(list)
    for prefix in prefixes_list:
        encoded_network, subnet = prefix_encoder.encode_prefix(prefix)
        prefixes_by_subnet[int(subnet)].append(encoded_network)
    return prefixes_by_subnet


class PrefixesWeightV4:
    def __init__(self, prefixes_list):
        self.prefixes_weight = dict()
        self.calculate_prefixes_weight(prefixes_list)

    def calculate_prefixes_weight(self, prefixes_list):
        prefixes_by_subnet = encode_and_group_prefixes_by_subnet(prefixes_list)
        available_prefix_subnet = sorted(prefixes_by_subnet.keys())

        for subnet in available_prefix_subnet:
            number_of_addresses_in_subnet = 1 << (32 - subnet)

            for encoded_network in prefixes_by_subnet[subnet]:
                self.prefixes_weight[encoded_network] = number_of_addresses_in_subnet
                self.remove_duplicated_supernet_weight(encoded_network, number_of_addresses_in_subnet)

    def remove_duplicated_supernet_weight(self, encoded_network: int, remove_weight: int):
        encoded_supernet = encoded_network // 2
        while encoded_supernet > 1:
            if encoded_supernet in self.prefixes_weight:
                self.prefixes_weight[encoded_supernet] -= remove_weight
                return
            encoded_supernet //= 2

    def get_prefix_weight(self, prefix: str):
        encoded_network, subnet = prefix_encoder.encode_prefix(prefix)
        return self.prefixes_weight[encoded_network]


if __name__ == "__main__":
    sample_prefixes_list_1 = ["8.8.8.0/24", "8.8.8.0/25"]
    prefix_weight_v4 = PrefixesWeightV4(sample_prefixes_list_1)
    print("test 1: ", sample_prefixes_list_1)
    for prefix in sample_prefixes_list_1:
        print("prefix:", prefix, "weight:", prefix_weight_v4.get_prefix_weight(prefix))

    sample_prefixes_list_2 = ["8.8.8.0/24", "8.8.7.0/25"]
    prefix_weight_v4 = PrefixesWeightV4(sample_prefixes_list_2)
    print("test 2: ", sample_prefixes_list_2)
    for prefix in sample_prefixes_list_2:
        print("prefix:", prefix, "weight:", prefix_weight_v4.get_prefix_weight(prefix))
