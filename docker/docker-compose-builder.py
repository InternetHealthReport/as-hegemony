def build_template(name: str, command: str, depended: list):
    return f"  {name}:\n" \
           f"{mount_image()}" \
           f"{mount_volume()}" \
           f"{mount_working_dir()}" \
           f"{mount_command(command)}" \
           f"{mount_depends_on(depended)}" \
           f"\n"


def mount_image():
    return "    image: 'pora/bgpstream:v2'\n"


def mount_volume():
    return "    volumes:\n" \
           "      - '/mnt/data-raid/pora-cache/log:/log'\n" \
           "      - '/home/elab/.pora/prefixes-ashege:/app'\n"


def mount_working_dir():
    return "    working_dir: '/app'\n"


def mount_command(command: str):
    return f"    command: {command}\n"


def mount_depends_on(depended_list):
    if depended_list:
        depended = "\n".join(map(lambda x: f"      - {x}", depended_list))
        return f"    depends_on:\n" \
               f"{depended}\n"
    return ""


def file_header():
    return 'version: "3"\n\n' \
           'services:\n'


def core_service():
    return """  zookeeper:
    image: 'bitnami/zookeeper:latest'
    environment:
      - ALLOW_ANONYMOUS_LOGIN=yes

  kafka:
    image:  'bitnami/kafka:latest'
    volumes:
      - '/mnt/data-raid/pora-cache/kafka-data:/bitnami/kafka'
    environment:
      - KAFKA_BROKER_ID=1
      - KAFKA_LISTENERS=PLAINTEXT://:9092
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092
      - KAFKA_ADVERTISED_HOST_NAME=kafka
      - KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181
      - ALLOW_PLAINTEXT_LISTENER=yes
    depends_on:
      - zookeeper\n\n"""


def bgp_message(start_time: str, end_time: str, collectors: str, message_type: str):
    template = ""
    for collector in collectors:
        command = f"python3 " \
                  f"/app/produce_bgpdata.py " \
                  f"-t {message_type} " \
                  f"--collector {collector} " \
                  f"--startTime {start_time} " \
                  f"--endTime {end_time}"
        worker_name = f"bgpstream-{message_type}-{collector}"
        depended = ["zookeeper", "kafka"]
        template += build_template(worker_name, command, depended)
    return template


def bgp_atom_builder(start_time: str, end_time: str, collectors: str):
    template = ""
    for collector in collectors:
        command = f"python3 " \
                  f"/app/produce_bgpatom.py " \
                  f"-c {collector} " \
                  f"-s {start_time} " \
                  f"-e {end_time}"
        worker_name = f"bgpatom-{collector}"
        depended = ["zookeeper", "kafka"]
        template += build_template(worker_name, command, depended)
    return template


def bcscore_builder(start_time: str, end_time: str, collectors: str, for_prefix=False):
    template = ""
    for collector in collectors:
        command = f"python3 " \
                  f"/app/produce_bcscore.py " \
                  f"-c {collector} " \
                  f"-s {start_time} " \
                  f"-e {end_time}"
        if for_prefix:
            worker_name = f"bc-builder-{collector}-prefix"
            command += " -p"
        else:
            worker_name = f"bc-builder-{collector}-asn"
        depended = ["zookeeper", "kafka"]
        template += build_template(worker_name, command, depended)
    return template


def hege_builder(start_time: str, end_time: str, collectors: str, for_prefix=False):
    collectors_str = ",".join(collectors)
    command = f"python3 " \
              f"/app/produce_hege.py " \
              f"-c {collectors_str} " \
              f"-s {start_time} " \
              f"-e {end_time}"
    if for_prefix:
        command += " -p"
        worker_name = f"prefix-hege-builder"
    else:
        worker_name = f"asn-hege-builder"
    depended = ["zookeeper", "kafka"]
    return build_template(worker_name, command, depended)


def debugger():
    depended = ["zookeeper", "kafka"]
    return "  debug:\n" \
           f"{mount_image()}" \
           f"{mount_volume()}" \
           f"{mount_working_dir()}" \
           f"    command: /bin/bash\n" \
           f"    tty: true\n" \
           f"{mount_depends_on(depended)}" \
           f"\n"


if __name__ == "__main__":
    start = "2020-08-01T00:00:00"
    end = "2020-08-01T00:01:00"
    docker_compose_file = ""
    collectors_list = ["rrc00", "rrc10", "route-views2", "route-views.linx"]
    docker_compose_file += file_header()
    docker_compose_file += core_service()

    docker_compose_file += bgp_message(start, end, collectors_list, "ribs")
    docker_compose_file += bgp_message(start, end, collectors_list, "updates")

    docker_compose_file += bgp_atom_builder(start, end, collectors_list)

    docker_compose_file += bcscore_builder(start, end, collectors_list)
    docker_compose_file += bcscore_builder(start, end, collectors_list, True)

    docker_compose_file += hege_builder(start, end, collectors_list)
    docker_compose_file += hege_builder(start, end, collectors_list, True)

    docker_compose_file += debugger()

    f = open("dev-docker-compose.yml", "w")
    f.write(docker_compose_file)
    f.close()
