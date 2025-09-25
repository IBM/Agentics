import os
from pathlib import Path
import json
import csv
from collections import Counter


def get_data_from_jsonl(jsonl_file):
    ret_dicts = []
    for each_line in open(jsonl_file):
        if each_line.strip():
            ret_dicts.append(json.loads(each_line.strip()))
    return ret_dicts


def dump_data_to_jsonl(list_of_dicts, jsonl_file, mode='w'):
    with open(jsonl_file, mode) as fp:
        for each_data in list_of_dicts:
            fp.write(json.dumps(each_data) + "\n")


def get_header_csv_file(csv_file):
    with open(csv_file, 'r') as fp:
        csv_reader = csv.reader(fp)
        return next(csv_reader)
    

def preprocess_csv_files(train_csv_file, valid_csv_file, test_csv_file, output_csv_file, num_repeats, imput_col=None):
    def read_csv_file(file_path):
        ret_rows = []
        header = None
        with open(file_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            header = next(csv_reader)
            for row in csv_reader:
                ret_rows.append(row)
        if imput_col is None:
            label_ind = -1
        else:
            label_ind = header.index(imput_col)

        labels = set([row[label_ind] for row in ret_rows])
        return {"header": header, "rows": ret_rows, "labels": labels}
    
    def check_counters_greater_than_n(counter, n):
        for count in counter.values():
            if count <= n:
                return False
        return True
    train_csv = read_csv_file(train_csv_file)
    valid_csv = read_csv_file(valid_csv_file)
    test_csv = read_csv_file(test_csv_file)
    assert valid_csv["header"] == test_csv["header"]
    print(test_csv["labels"] - train_csv["labels"])
    print(test_csv["labels"] - valid_csv["labels"])

    train_rows = list(train_csv["rows"])
    valid_rows = list(valid_csv["rows"])
    fewshot_labels = list(train_csv["labels"] | valid_csv["labels"])
    count_labels = Counter(fewshot_labels) 
    fewshot_rows = train_rows + valid_rows
    selected_rows = []
    for each_row in fewshot_rows:
        current_label = each_row[-1]
        if count_labels[current_label] < num_repeats+1:
            count_labels[current_label] += 1
            selected_rows.append(each_row)
        if check_counters_greater_than_n(count_labels, n=num_repeats+1):
            break
    
    if imput_col is None:
        label_ind = -1
    else:
        label_ind = test_csv["header"].index(imput_col)

    test_rows = []
    for each_row in list(test_csv["rows"]):
        each_row[label_ind] = ""
        test_rows.append(each_row)
    output_csv_file = output_csv_file.replace(".csv", f"__k={num_repeats}.csv")
    with open(output_csv_file, 'w') as output_csv:
        csv_writer = csv.writer(output_csv)
        csv_writer.writerow(test_csv["header"])
        csv_writer.writerows(test_rows)
        csv_writer.writerows(selected_rows)

crew_prompt_params = {
    "role": "Data Imputation.",
    "goal": "Your task is to examine the provided few shot examples of data and fill-in the missing target attribute.",
    "backstory": "The fewshot examples show all labels necessary for filling in the missing target value.",
    "expected_output": "The expected output is described by Pydantic Type.",
}

custom_instruction = """Fill-in the missing value of the target field {target_field}? Conside the fewshot examples to guess the best value."""


if __name__ == "__main__":
    data_path = os.fspath(Path(__file__).resolve().parent.parent.parent / "data" / "data_wrangling")


    preprocess_csv_files(os.path.join(data_path, "Buy/train.csv"),
                         os.path.join(data_path, "Buy/valid.csv"),
                         os.path.join(data_path, "Buy/test.csv"),
                         os.path.join(data_path, "buy_test_with_fewshots.csv"),
                         num_repeats=0)

    preprocess_csv_files(os.path.join(data_path, "Restaurant/train.csv"),
                         os.path.join(data_path, "Restaurant/valid.csv"),
                         os.path.join(data_path, "Restaurant/test.csv"),
                         os.path.join(data_path, "restaurant_test_with_fewshots.csv"),
                         num_repeats=0, imput_col="city")
    
    preprocess_csv_files(os.path.join(data_path, "Buy/train.csv"),
                         os.path.join(data_path, "Buy/valid.csv"),
                         os.path.join(data_path, "Buy/test.csv"),
                         os.path.join(data_path, "buy_test_with_fewshots.csv"),
                         num_repeats=1)

    preprocess_csv_files(os.path.join(data_path, "Restaurant/train.csv"),
                         os.path.join(data_path, "Restaurant/valid.csv"),
                         os.path.join(data_path, "Restaurant/test.csv"),
                         os.path.join(data_path, "restaurant_test_with_fewshots.csv"),
                         num_repeats=1, imput_col="city")

    preprocess_csv_files(os.path.join(data_path, "Buy/train.csv"),
                         os.path.join(data_path, "Buy/valid.csv"),
                         os.path.join(data_path, "Buy/test.csv"),
                         os.path.join(data_path, "buy_test_with_fewshots.csv"),
                         num_repeats=2)

    preprocess_csv_files(os.path.join(data_path, "Restaurant/train.csv"),
                         os.path.join(data_path, "Restaurant/valid.csv"),
                         os.path.join(data_path, "Restaurant/test.csv"),
                         os.path.join(data_path, "restaurant_test_with_fewshots.csv"),
                         num_repeats=2, imput_col="city")
