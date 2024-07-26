# Teachable Analytics Engineer Test

## Exercise One

- All the answers to this exercise are in the **e1_answers.txt** file. Additionally, I created a Python job that reads the SQL queries and executes them, creating a log file and a SQLite database. You can check it out at **python\e1_orchestrator.py**.

## Exercise Two

- For this exercise, my notebook unfortunately kept crashing whenever I tried to initialize Spark. Nevertheless, I wrote a PySpark script as required, which can be found at **python\e2_orchestrator.py**. My thought process involved including an "active" flag (`is_active`) to facilitate access to the current GMV situation. This flag is useful for updating the table with the most recent data: whenever a change occurs in one of the tables, the code reads the newest data stored in the table, updates the existing records to "not active" if they have any changes, and inserts new data as it arrives. The SQL query is available at **sql\e2_gmv.sql** and is designed to be understandable even to users who are not SQL experts.

### How to Run

1. **Exercise One**:
   - Navigate to the `python` directory.
   - Run the `e1_orchestrator.py` script.
   - The script will execute the SQL queries, create a log file, store the results in a SQLite database, and print the answers at a text file.

2. **Exercise Two**:
   - Navigate to the `python` directory.
   - Run the `e2_orchestrator.py` script.
   - The script will initialize the Spark session (if the environment allows), process the data, and update the historical table with the most recent information.
   - To get the GMV daily by subsidiary, execute the SQL query provided in the **sql\e2_gmv.sql** file.

### Key Points

- The `is_active` flag in Exercise Two helps maintain an up-to-date view of the GMV by marking the current records as active and updating them as new data arrives.
- The provided SQL query for Exercise Two is user-friendly and does not require advanced SQL knowledge to understand or execute.

If you have any questions or need further assistance, please feel free to reach out.
