Prompt:
Write the python code to read data from Cloud Spanner.
Response:
You can read data from [Cloud Spanner](https://cloud.google.com/spanner) using the `ReadFromSpanner` transform. The following Python code reads a table `example_row` from a Cloud Spanner database `your-database-id` from a Cloud Spanner instance `your-instance-id`. The `your-database-id`, `your-instance-id` and `your-project-id` are provided as a command line arguments. The data is logged to the console:

```python
