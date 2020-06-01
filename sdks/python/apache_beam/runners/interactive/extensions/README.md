# This `extensions` directory holds all plugins for Interactive Beam.

## interactive_beam_side_panel 
  
  - A side panel providing information of current interactive environment, data
    sets associated with PCollections, pipeline state, job status and source
    capture.
  - A Jupyter labextension that only works with JupyterLab version > 2.0.
  - Created through cookiecutter with below configurations:

    ```
    pip install cookiecutter
    cookiecutter https://github.com/jupyterlab/extension-cookiecutter-ts
    
    author_name []: ningk
    extension_name [myextension]: interactive_beam_side_panel
    project_short_description [A JupyterLab extension.]: A side panel providing information of current interactive environment, data sets associated with PCollections, pipeline state, job status and source capture.
    has_server_extension [n]: Y
    has_binder [n]: 
    repository [https://github.com/my_name/myextension]: https://github.com/apache/beam
    ```
