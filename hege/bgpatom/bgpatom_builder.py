from collections import defaultdict
import json

from hege.bgpatom.particles_handler import ParticlesHandler
import bgpdata
import utils


def atom_aspath_encoding(as_path: str):
    non_prepended_as_path = utils.remove_path_prepending(as_path.split(" "))
    return tuple(non_prepended_as_path[:-1])


class BGPAtomBuilder:
    def __init__(self, collector, timestamp):
        self.collector = collector
        self.atom_timestamp = timestamp
        self.prefix_to_atom_id = defaultdict(list)
        self.particles_handler = ParticlesHandler()

    def resize_prefix_atom_id_length(self, prefix):
        target_size = self.particles_handler.number_of_particles
        current_size = len(self.prefix_to_atom_id[prefix])
        self.prefix_to_atom_id[prefix] += ["*"] * (target_size-current_size)

    def read_ribs_and_add_particles_to_atom(self):
        for element in bgpdata.consume_ribs_message_at(self.collector, self.atom_timestamp):
            peer_address = element["peer_address"]
            as_path = element["fields"]["as-path"]
            prefix = element["fields"]["prefix"]

            if prefix == "0.0.0.0/0":
                continue

            particle = self.particles_handler.get_particle_by_peer_address(peer_address)
            particle.increment_prefixes_count()

            atom_as_path = atom_aspath_encoding(as_path)
            particle_id = particle.idx
            aspath_id = particle.get_id_by_aspath(atom_as_path)

            self.resize_prefix_atom_id_length(prefix)
            self.prefix_to_atom_id[prefix][particle_id] = str(aspath_id)

    def remove_none_full_fleet_particles(self, threshold: int):
        none_full_fleet = self.particles_handler.get_none_full_fleet_particles(threshold)

        for prefix in self.prefix_to_atom_id:
            self.resize_prefix_atom_id_length(prefix)
            for none_full_fleet_particle_id in none_full_fleet:
                self.prefix_to_atom_id[prefix][none_full_fleet_particle_id] = "-"

    def dump_bgpatom(self):
        bgpatom_id_to_prefixes = defaultdict(list)
        for prefix in self.prefix_to_atom_id:
            atom_id = "|".join(self.prefix_to_atom_id[prefix])
            bgpatom_id_to_prefixes[atom_id].append(prefix)
        return bgpatom_id_to_prefixes


if __name__ == "__main__":
    with open("/app/config.json", "r") as f:
        config = json.load(f)

    test_collector = "rrc10"
    BGPATOM_FULL_FLEET_THRESHOLD = config["bgpatom"]["full_fleet_threshold"]

    atom_time_string = "2020-08-01T00:00:00"
    atom_datetime = utils.str2dt(atom_time_string, utils.DATETIME_STRING_FORMAT)
    atom_timestamp = utils.dt2ts(atom_datetime)

    bgpatom_builder = BGPAtomBuilder(test_collector, atom_timestamp)
    bgpatom_builder.read_ribs_and_add_particles_to_atom()
    bgpatom_builder.remove_none_full_fleet_particles(BGPATOM_FULL_FLEET_THRESHOLD)

    bgpatom = bgpatom_builder.dump_bgpatom()
    print("number of atom: ", len(bgpatom))
