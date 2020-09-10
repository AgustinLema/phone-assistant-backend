import csv
import json
import re
import logging

import database

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

in_filename = "dataset_load/gsm.csv"
out_filename = "dataset_load/dataset.json"

row_field_mapping = {
    "oem": "Marca",
    "model": "Modelo",
    "memory_internal": "Memoria Interna",
    "platform_os": "Sistema operativo",
    "misc_price": "Precio",
    "launch_status": "Lanzamiento",
    "display_size": "Tamaño de pantalla",
    "tests_battery_life": "Duración de batería",
    "comms_usb": "Tipo de USB",
    "features_sensors": "Sensores",
    "comms_nfc": "NFC",
    "main_camera_video": "Resolucion de video de camara",
    "battery_charging": "Carga de bateria",
    "sound_3.5mm_jack": "Conector para auriculares 3.5mm",
}


def get_columns_from_fields(header, row_field_mapping=row_field_mapping):
    # print("Got headers", header)
    mapping = {}
    for col, field in enumerate(header):
        if field in row_field_mapping:
            mapping[col] = row_field_mapping[field]
    return mapping


def get_obj_from_row_and_mapping(row, mapping):
    return {mapping[col]: row[col] for col, field in mapping.items()}


def has_empty_fields(obj, keys):
    for k in obj.keys():
        if k not in keys:
            continue
        if len(obj[k]) < 1:
            logger.warn("Key has no value! %s", k)
            return True
        # print(f"obj es [{obj[k]}]")
    return False


def dataset_cleanup(dataset):
    cleaned_dataset = []
    memory_pattern = re.compile(r'(\d+ ?[GM]B)')
    os_upgradable_pattern = re.compile(r'upgradable to [^\d]*([\d\.]*)')
    for obj in dataset:
        if has_empty_fields(obj, []):
            continue

        # Filter by release year
        if obj['Lanzamiento'] in ['Discontinued', "Cancelled"]:
            # logger.debug("Skipping release discontinued")
            continue
        try:
            obj['Lanzamiento'] = re.search(
                r'.*(20\d+).*', obj['Lanzamiento']).group(1)
        except AttributeError:
            logger.debug("Found one release date without year, skipping data")
            logger.debug(obj['Lanzamiento'], obj)
            continue

        # Parse screen size
        if "Tamaño de pantalla" in obj:
            obj["Tamaño de pantalla"] = obj["Tamaño de pantalla"].split()[0]

        # Parse OS
        if "Sistema operativo" in obj:
            os_data = obj["Sistema operativo"]
            # print(os_data)
            split_os = os_data.replace(",", "").split()
            if len(split_os) > 0:
                obj["Sistema operativo"] = split_os[0]
                if "upgradable" in os_data.lower():
                    upgradable_found = os_upgradable_pattern.findall(os_data)
                    # print("Upgradable found", upgradable_found, len(upgradable_found))
                    obj["Version del sistema operativo"] = upgradable_found[-1]
                    # raise
                else:
                    if len(split_os) > 1:
                        obj["Version del sistema operativo"] = split_os[1]

        # Split models by internal memory and ram as different devices.
        if ',' in obj['Memoria Interna']:
            memory_variants = obj['Memoria Interna'].split(",")
        else:
            memory_variants = [obj['Memoria Interna']]
        for variant in memory_variants:
            obj['Memoria Interna'] = variant
            memory_data = memory_pattern.findall(variant)
            if len(memory_data) == 0:
                logger.warning(
                    "Skipping phone variant, missing information for memory: [%s] [%s]", variant, memory_data)
                continue
            memory_fields = [
                'RAM', 'Almacenamiento Interno', 'Extra memory data']
            memory_data.reverse()
            obj['Almacenamiento Interno'] = ""  # Quick hack to have it added to make unique name, TODO: fix later
            for i, data in enumerate(memory_data):
                obj[memory_fields[i]] = data.replace(' ', '')
            if i > 1:
                logger.error("Too much memory data?", variant, memory_data)
                raise

            obj['dataset_unique_name'] = f"{obj['Marca']} {obj['Modelo']} {obj['RAM']} {obj['Almacenamiento Interno']}"
            obj['unique_name'] = obj['dataset_unique_name']  # Set initial unique name

            cleaned_dataset.append(obj.copy())

    # Add ids
    for id, obj in enumerate(cleaned_dataset):
        obj["phone_id"] = id

    return cleaned_dataset


def remove_duplicates(objs, key):
    seen = {}
    for obj in objs:
        obj_id = obj[key]
        if obj_id is None:
            logger.error("No key found!")
            logger.error(obj)
            raise Exception("Missing key in object when loading dataset")
        if obj_id in seen and obj != seen[obj_id]:
            # logger.warning("Duplicate object found!")
            # logger.warning(obj)
            # logger.warning(seen[obj_id])
            pass
        else:
            seen[obj_id] = obj
    return list(seen.values())


with open(in_filename, 'r') as csvfile:
    reader = csv.reader(csvfile)
    headers = next(reader)
    field_mapping = get_columns_from_fields(headers)

    logger.debug(field_mapping)
    object_dataset = [get_obj_from_row_and_mapping(
        row, field_mapping) for row in reader]
    logger.debug("Before cleanup: %d", len(object_dataset))
    object_dataset = dataset_cleanup(object_dataset)
    logger.debug("After cleanup: %d", len(object_dataset))
    object_dataset = remove_duplicates(object_dataset, "dataset_unique_name")

    logger.debug("After removing duplicates: %d", len(object_dataset))

    # logger.debug(object_dataset[:20])

    with open(out_filename, 'w') as jsonfile:
        json.dump(object_dataset, jsonfile, indent=4)

    # database.mobile_phone.drop()
    # database.mobile_phone.insert_many(object_dataset)
    database.upsert_many(database.mobile_phone, object_dataset, ["dataset_unique_name"])
