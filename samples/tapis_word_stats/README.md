Tapis Word Stats Pipeline Sample
================================

This directory contains a Tapis Pipelines sample that computes statistics (such as word count) of
text files. The following resources are included:

* ``tapis_exec_system.json`` - The execution system where this sample's pipeline jobs will run.
* ``tapis_*_*_storage_system.json`` - Tapis system definitions for the Local and Remote Inboxes and Outbox.
* ``word_stats_pipeline_config.json`` - The pipeline configuration for this sample.
* ``pipeline_job``  - This directory includes all files constituting the pipeline, including: 
  * the analysis software itself (in this case, the word_stats.py) and build scripts to package it as a container image 
  * the ``app.json`` file, defining the words stats container as a Tapis application.
  * ``core`` - a copy of the core client library to facilitate building Tapis pipelines (the "real" 
    copy is in ``pipelines/core" but this included here so it can be built into the docker image) will ultimately be moved out to PyPI.
* ``examples`` -  directory of examples that can be used to test both individual jobs as well as the full pipeline.
* ``util.py`` - Python functions that can be used for some operations, such as registering the systems and submitting test jobs.

Instructions for Setting Up this Pipeline 
-----------------------------------------
1. Register the required Tapis objects:
 
  * Tapis systems for remote outbox and inobox
  * Tapis app 