# Databricks intro 10.04.24 for Lerøy Seafood



## Getting started

The easiest way to get started in Databricks is to import individual notebooks. This repo can also be added in its entirety.



### Using Databricks asset bundles
Below is a generic introduction to 
Databricks asset bundles.


1. Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/databricks-cli.html

2. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

3. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```
    (Note that "dev" is the default target, so the `--target` parameter
    is optional here.)

    This deploys everything that's defined for this project.
    For example, the default template would deploy a job called
    `[dev yourname] databricks_intro_job` to your workspace.
    You can find that job by opening your workpace and clicking on **Workflows**.


