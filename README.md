**CSE 138 ASSIGNMENTS TEST SUITE V1.4**
---

# How to run

Extract the archive in the same root folder where your project files are located.<br><br>
Below is an example project directory:<br>
CMakeLists.txt<br>
Dockerfile<br>
/source (contains your code)<br>
/test (the above archived folder extracted which should contain the test suite)<br>

## Steps

1. Open Terminal or PowerShell in the project root directory and run:
    ```sh
    cd tests
    ```

2. Create a virtual environment:
    ```sh
    python -m venv .venv
    ```

3. Activate the virtual environment:

    - **PowerShell:**
      ```powershell
      .venv\Scripts\activate
      ```

    - **Terminal/Linux:**
      ```bash
      source .venv/bin/activate
      ```

4. Install the test suite dependencies:
    ```sh
    pip install -r requirements.txt
    ```

## Running Tests

- To run Assign. 2 tests:
    ```sh
    python -m kvs_test <project_dir> --hw 2
    ```

- To run Assign. 3 tests:
    ```sh
    python -m kvs_test <project_dir> --hw 3
    ```

- To run Assign. 4 tests:
    ```sh
    python -m kvs_test <project_dir> --hw 4
    ```

- To run a specific test with a filter:
    ```sh
    python -m kvs_test <project_dir> --hw %ASSIGNMENT NUMBER% -f causal
    ```
- To run all tests without stopping at first failure:
    ```sh
    python -m kvs_test <project_dir> --hw %ASSIGNMENT NUMBER% --no-fail-fast
    ```
**Note:**  
If your project directory is similar to the example above, replace `<project_dir>` with two dots (`..`).  
For example:
```sh
python -m kvs_test .. --hw 3
```
**Below is what your assignment and test suite directory tree should look like:**
```
Project Root:
│   build_check.sh
│   causal_consistency.pdf
│   CMakeLists.txt
│   Dockerfile
│   run_hw4_tests.sh
│   sharding_demo.py
│   validate_hw4_setup.py
│   ...
│   
├───src # Folder wherein you store your code
│       
└───tests
    │   .gitignore
    │   .python-version
    │   a.txt
    │   Makefile
    │   pyproject.toml
    │   README.md
    │   requirements.txt
    │   uv.lock
    │               
    └───kvs_test
        │   containers.py
        │   hw2_api.py
        │   hw3_api.py
        │   hw4_api.py
        │   testcase.py
        │   util.py
        │   __main__.py
        │   
        ├───hw2_tests
        │       advanced_tests.py
        │       basic.py
        │       __init__.py
        │       
        ├───hw3_tests
        │       availability_tests.py
        │       basic_tests.py
        │       causal_consistency_tests.py
        │       eventual_consistency_tests.py
        │       view_change_tests.py
        │       __init__.py
        │       
        ├───hw4_tests
        │       advanced_sharding_tests.py
        │       basic_sharding_tests.py
        │       performance_tests.py
        │       resharding_tests.py
        │       __init__.py
```

**Changelog:**
* V1.5: Removed `test_no_operation_resharding` thanks to Kat's noticing that it is against Assignment 4's specifications, and added 8 new test cases under critical_edge_case_tests.py (which will also run faster thanks to multi-threading):
    * Key assignment consistency across nodes
    * Deep cross-shard causal chains
    * Partial shard recovery scenarios
    * Cross-shard concurrent write conflicts
    * Shard failure during data migration
    * Many shards performance (8 shards)
    * Proxy request failure handling
    * Large dataset resharding (with parallel insertion)
* v1.4: Corrected the default run so legacy view test (i.e., non-sharded view test) is not run in the default run for Assignment 4 tests.
  * The above bug was caught thanks to Yagnesh and Charles.
* v1.3: Assignment 4's test API and tests were created.
* v1.2: A few status and Docker network bugs were fixed by Alan, Rithwick, and Graham for Assignment 3.
* v1.1: Assignment 3's test API and tests were created.
* v1.0: First version contained an advanced (modified version of the one provided by Professor Alvaro) test for Assignment 2.
