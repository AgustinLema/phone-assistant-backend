import csv
import json
import re
import database

in_filename = "dataset_load/gsm.csv"
out_filename = "dataset_load/dataset.json"

row_field_mapping = {
    "oem": "Marca",
    "model": "Modelo",
    "memory_internal": "Memoria Interna",
    "misc_price": "Precio",
    "launch_status": "Lanzamiento",
}


def get_columns_from_fields(header, row_field_mapping=row_field_mapping):
    #print("Got headers", header)
    mapping = {}
    for col, field in enumerate(header):
        if field in row_field_mapping:
            mapping[col] = row_field_mapping[field]
    return mapping


def get_obj_from_row_and_mapping(row, mapping):
    return {mapping[col]: row[col] for col, field in mapping.items()}


def has_empty_fields(obj):
    for k in obj.keys():
        if len(obj[k]) < 1:
            return True
        # print(f"obj es [{obj[k]}]")
    return False


def dataset_cleanup(dataset):
    cleaned_dataset = []
    memory_pattern = re.compile('(\d+ ?[GM]B)')
    for obj in dataset:
        if has_empty_fields(obj):
            continue

        # Filter by release year
        if obj['Lanzamiento'] in ['Discontinued', "Cancelled"]:
            print("Skipping release discontinued")
            continue
        try:
            obj['Lanzamiento'] = re.search(
                '.*(20\d+).*', obj['Lanzamiento']).group(1)
        except AttributeError:
            print("Found one release date without year, skipping data")
            print(obj['Lanzamiento'], obj)
            continue

        # Split models by internal memory and ram as different devices.
        if ',' in obj['Memoria Interna']:
            memory_variants = obj['Memoria Interna'].split(",")
        else:
            memory_variants = [obj['Memoria Interna']]
        for variant in memory_variants:
            obj['Memoria Interna'] = variant
            memory_data = memory_pattern.findall(variant)
            if len(memory_data) == 0:
                print(
                    "Skipping phone variant, missing information for memory", variant, memory_data)
                continue
            memory_fields = [
                'RAM', 'Almacenamiento Interno', 'Extra memory data']
            memory_data.reverse()
            obj['Almacenamiento Interno'] = ""  # Quick hack to have it added to make unique name, TODO: fix later
            for i, data in enumerate(memory_data):
                obj[memory_fields[i]] = data.replace(' ', '')
            if i > 1:
                print("Too much memory data?", variant, memory_data)
                raise

            obj['dataset_unique_name'] = f"{obj['Marca']} {obj['Modelo']} {obj['RAM']} {obj['Almacenamiento Interno']}"
            obj['unique_name'] = obj['dataset_unique_name']  # Set initial unique name

            cleaned_dataset.append(obj.copy())

    # Add ids
    for id, obj in enumerate(cleaned_dataset):
        obj["id"] = id

    return cleaned_dataset


with open(in_filename, 'r') as csvfile:
    reader = csv.reader(csvfile)
    headers = next(reader)
    field_mapping = get_columns_from_fields(headers)

    print(field_mapping)
    object_dataset = [get_obj_from_row_and_mapping(
        row, field_mapping) for row in reader]
    object_dataset = dataset_cleanup(object_dataset)

    print(len(object_dataset))

    print(object_dataset[:20])

    with open(out_filename, 'w') as jsonfile:
        json.dump(object_dataset, jsonfile, indent=4)

    database.phone_data.drop()
    database.phone_data.insert_many(object_dataset)
