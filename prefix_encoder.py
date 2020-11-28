V4_BITS_COUNT = 32
two_power = [1 << nth_bit for nth_bit in range(V4_BITS_COUNT)][::-1]


def encode_prefix(prefix: str):
    network, subnet_str = prefix.split("/")
    network_32bits_int = get_network_32bits_int_representation(network)
    subnet = int(subnet_str)

    encoded_prefix = 1
    for nth_bit in range(subnet):
        nth_bit_is_one = bool(two_power[nth_bit] & network_32bits_int)
        encoded_prefix = 2*encoded_prefix + nth_bit_is_one

    return encoded_prefix, subnet


def get_network_32bits_int_representation(network: str):
    octets = list(map(int, network.split(".")))
    network_32bits_int = 0
    for octet in octets:
        network_32bits_int *= 256
        network_32bits_int += octet
    return network_32bits_int


def decode_prefix(encoded_prefix, subnet):
    nth_from_back = subnet-1
    bits = ["0"] * 32
    while encoded_prefix > 1:
        bits[nth_from_back] = str(encoded_prefix % 2)
        encoded_prefix //= 2
        nth_from_back -= 1

    octets = list()
    for i in range(4):
        start_bit = i*8
        octet_str = "".join(bits[start_bit:start_bit+8])
        octet_value = int(octet_str, 2)
        octets.append(str(octet_value))

    return f"{'.'.join(octets)}/{subnet}"


if __name__ == "__main__":
    sample_prefixes_list = ["8.8.8.0/24", "8.8.8.0/25"]
    for prefix in sample_prefixes_list:
        encoded_network, subnet = encode_prefix(prefix)
        print("encoded:", prefix, encoded_network, subnet)
        decoded_prefix = decode_prefix(encoded_network, subnet)
        print("decoded:", decoded_prefix)
