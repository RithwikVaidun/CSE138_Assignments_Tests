# How to run

Extract the archive in the same root folder where your project files are located.  
Below is an example project directory:
CMakeLists.txt
Dockerfile
/source (contains your code)
/test (the above archived folder extracted which should contain the test suite)

Copy

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

- To run a specific test with a filter:
    ```sh
    python -m kvs_test <project_dir> --hw 3 -f causal
    ```

**Note:**  
If your project directory is similar to the example above, replace `<project_dir>` with two dots (`..`).  
For example:
```sh
python -m kvs_test .. --hw 3
