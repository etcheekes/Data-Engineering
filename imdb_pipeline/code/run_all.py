import subprocess
from constants.constants import python_file_paths

# run each script
for py_file in python_file_paths:

    # Use subprocess.run to execute the script and capture output
    result = subprocess.run(["python", py_file], capture_output=True, text=True)

    print(f"running {py_file}")

    # print output of file
    print(f"output\n{result.stdout}")

    # print out any error that may occur
    if result.stderr:
        print(f"Error:\n {result.stderr}")