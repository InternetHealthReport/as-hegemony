class Particle:
    def __init__(self, peer_address, particle_id):
        self.peer_prefix = peer_address
        self.idx = particle_id
        self.prefixes_count = 0

        self.__aspath_to_id = dict()
        self.__number_of_aspath = 0

    def get_id_by_aspath(self, aspath: tuple):
        if aspath in self.__aspath_to_id:
            return self.__aspath_to_id[aspath]

        aspath_id = self.__number_of_aspath
        self.__aspath_to_id[aspath] = aspath_id
        self.__number_of_aspath += 1
        return aspath_id

    def increment_prefixes_count(self):
        self.prefixes_count += 1

    def is_full_fleet_particle(self, threshold: int):
        return self.prefixes_count > threshold
