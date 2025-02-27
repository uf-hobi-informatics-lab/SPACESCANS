# SPACESCANS


## Using the Command Line Interface
### Installation

1. Clone the repo to your local directory
2. In the root of the repo, create a `projects` folder and an `output` folder

        mkdir projects
        mkdir output

### Configuring
In `csv_linkage.py` navigate to line 30 and update the `EXPOSOME_PATH` variable to the path where the exposome file lives. The current version of linkage works only on UCR data. We are working to stand this up for the rest of the datasets.

### Running the Command Line
The command line interface uses projects to store information about a specific run of linkage. In order to get started running linkage, a user must create a project. The project can then be used to run linkage. 

#### 1. create_project
Allows the user to create a new project. In addition to specifying a name, you can add all necessary metadata when calling the command, otherwise the program will prompt you to enter any metadata that was missed

*Accepted Flags*
- `-p`: The name of the project
- `-s`: Start date of the study period in YYYY-MM-DD format
- `-e`: End date of the study period in YYYY-MM-DD format
- `-g`: The geoidentifier used in the patient dataset (9-digit zipcode, 5-digit zipcode, etc)
- `-f`: The location of your patient dataset
- `-h`: Help.

*Syntax*

    $ python3 run_spacescans.py create_project [-h] [-p PROJECT_NAME] [-s START_DATE] [-e END_DATE] [-g GEOID] [-f FILEPATH]

#### 2. link
Allows the user to run linkage using a specified project name

*Accepted Flags*
- `-p`: The name of the project
- `-h`: Help.

*Syntax*

    $ python3 run_spacescans.py link [-h] [-p PROJECT_NAME]
### preprocessing pipeline
The preprocessing pipeline simplifies the task of translating raw exposome data from government sources, available in various geographical formats, into ZIP codes (currently supporting ZIP9). This tool allows you to easily process data by providing a raw exposome data list, output directory, desired exposome type, and buffer file directory.

---

### Getting Started

#### Quick Start with One Command
You can start processing your data using one of the following methods:

---

#### **Method 1: Basic Command Line Usage**
Run this command to process data using the National Walkability Index as an example:

```bash
python run_preprocessing_pipeline.py --data_list /path/to/data_list/ \
                                     --output_dir /path/to/output/ \
                                     --buffer_dir /path/to/buffer_files/ \
                                     --exposome_type wi
```

Replace the paths and parameters with your actual data directories and desired exposome type.

---

#### **Method 2: Configuration File**
The `./example` directory contains sample configurations to help you get started quickly.

1. Modify the `config.yaml` file in the `./example` directory to include:
   - `data_list`: The path to your raw exposome data.
   - `output_dir`: The directory where processed data will be saved.
   - `buffer_dir`: The directory for temporary buffer files.
   - `exposome_type`: The type of exposome data to process.

   **Note**: Use absolute paths for all file directories in `config.yaml`.

2. Run the pipeline with the configuration file:

```bash
python run_preprocessing_pipeline.py --config ./example/config_wi.yaml
```

---

#### **Method 3: Overriding Configuration Parameters**
You can update specific parameters in the `config.yaml` file directly from the command line. For example, to replace the data list directory:

```bash
python run_preprocessing_pipeline.py --config ./example/config_wi.yaml \
                                     --data_list /path/to/your_new_directory/
```

---

### Additional Examples
Explore the `./example` directory for more examples and templates to guide your data preprocessing tasks. Customize the `config.yaml` files for different exposome types and geographical formats.

---
