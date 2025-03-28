def load_txt_to_dict(file_path):
    """
    Load a text file and convert its contents to a dictionary.
    Assumes the file has key-value pairs separated by a delimiter (e.g., ':').

    :param file_path: Path to the text file
    :return: Dictionary with the file's contents
    """
    result_dict = {}
    try:
        with open(file_path, 'r') as file:
            for line in file:
                # Skip empty lines or lines that don't contain a delimiter
                a, b, cor = line.split()
                result_dict[(a,b)] = cor
                # result_dict[(b,a)] = cor
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
    except Exception as e:
        print(f"An error occurred: {e}")
    return result_dict

def check_dict(dict_1: dict, dict_2:dict):
    """
    Check if two dictionaries are equal.

    :param dict_1: First dictionary
    :param dict_2: Second dictionary
    :return: True if the dictionaries are equal, False otherwise
    """
    if len(dict_1.keys()) != len(dict_2.keys()):
        print("Lengths are different")
        return
    for key in dict_1.keys():
        if key not in dict_2.keys() or dict_1[key] != dict_2[key]:
            print(f"Key {key} is different: {dict_1[key]} != {dict_2[key]}")

# Example usage
if __name__ == "__main__":
    solution_path = "cor-demo-output.txt"  # Replace with your file path
    my_path = "my_cor_test.txt"
    solution_dict = load_txt_to_dict(solution_path)
    my_dict = load_txt_to_dict(my_path)
    check_dict(solution_dict, my_dict)