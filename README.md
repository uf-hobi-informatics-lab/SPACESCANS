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
