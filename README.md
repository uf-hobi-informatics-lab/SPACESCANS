# SPACESCANS


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
