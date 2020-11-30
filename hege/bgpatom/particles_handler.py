from hege.bgpatom.particle import Particle


class ParticlesHandler:
    def __init__(self):
        self.particles = dict()
        self.number_of_particles = 0

    def get_particle_by_peer_address(self, peer_address: str):
        if peer_address in self.particles:
            return self.particles[peer_address]

        new_particle_id = self.number_of_particles
        particle = Particle(peer_address, new_particle_id)
        self.particles[peer_address] = particle

        self.number_of_particles += 1
        return particle

    def get_none_full_fleet_particles(self, threshold: int):
        none_full_fleet = list()
        for peer_address in self.particles:
            particle = self.particles[peer_address]
            if not particle.is_full_fleet_particle(threshold):
                particle_id = particle.idx
                none_full_fleet.append(particle_id)
        return none_full_fleet
