Operators
==================================

.. contents::
   :local:
   :depth: 2

require>: Runs another workflow
----------------------------------

**require>:** operator runs another workflow. It's skipped if the workflow is already done successfully.

.. code-block:: yaml

    run: +main
    +main:
      require>: +another
    +another:
      sh>: tasks/another.sh

:command:`require>: +NAME`
  Name of a workflow.

  Example: +another_task

py>: Python scripts
----------------------------------

**py>:** operator runs a Python script using ``python`` command.
TODO: link to `Python API documents <ruby_api.html>`_ for details including variable mappings to keyword arguments.

.. code-block:: yaml

    +step1:
      py>: my_step1_method
    +step2:
      py>: tasks.MyWorkflow.step2

:command:`py>: [PACKAGE.CLASS.]METHOD`
  Name of a method to run.

  * :command:`py>: tasks.MyWorkflow.my_task`


rb>: Ruby scripts
----------------------------------

**rb>:** operator runs a Ruby script using ``ruby`` command.

TODO: add more description here
TODO: link to `Ruby API documents <python_api.html>`_ for details including best practices how to configure the workflow using ``export: require:``.

.. code-block:: yaml

    export:
      ruby:
        require: tasks/my_workflow

    +step1:
      rb>: my_step1_method
    +step2:
      rb>: Task::MyWorkflow.step2

:command:`rb>: [MODULE::CLASS.]METHOD`
  Name of a method to run.

  * :command:`rb>: Task::MyWorkflow.my_task`

:command:`require: FILE`
  Name of a file to require.

  * :command:`require: task/my_workflow`


sh>: Shell scripts
----------------------------------

**sh>:** operator runs a shell script.

TODO: add more description here

.. code-block:: yaml

    +step1:
      sh>: tasks/step1.sh
    +step2:
      sh>: tasks/step2.sh

:command:`sh>: COMMAND [ARGS...]`
  Name of the command to run.

  * :command:`sh>: tasks/workflow.sh --task1`


td>: Treasure Data queries
----------------------------------

**td>:** operator runs a Hive or Presto query on Treasure Data.

TODO: add more description here

.. code-block:: yaml

    export:
      td:
        apikey: YOUR/API_KEY
        database: www_access

    +step1:
      td>: queries/step1.sql
    +step2:
      td>: queries/step2.sql
      create_table: mytable_${session_date_compact}
    +step3:
      td>: queries/step2.sql
      insert_into: mytable

Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:command:`td>: FILE.sql`
  Path to a query template file. This file can contain ``${...}`` syntax to embed variables.

  * :command:`td>: queries/step1.sql`

:command:`create_table: NAME`
  Name of a table to create from the results. This option deletes the table if it already exists.

  * :command:`create_table: my_table`

:command:`insert_into: NAME`
  Name of a table to append results into.

  * :command:`insert_into: my_table`

:command:`download_file: NAME`
  Saves query result as a local CSV file.

  * :command:`download_file: output.csv`

:command:`result_url: NAME`
  Output the query results to the URL:

  * :command:`result_url: tableau://username:password@my.tableauserver.com/?mode=replace`

:command:`database: NAME`
  Name of a database.

  * :command:`database: my_db`

:command:`apikey: APIKEY`
  API key.

  * :command:`apikey: 992314/abcdef0123456789abcdef0123456789`

:command:`engine: presto`
  Query engine (``presto`` or ``hive``).

  * :command:`engine: hive`
  * :command:`engine: presto`


Output parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:command:`td.last_job_id`
  The job id this task executed.

  * :command:`52036074`


td_load>: Treasure Data bulk loading
----------------------------------

**td_load>:** operator loads data from storages, databases, or services.

TODO: add more description here

.. code-block:: yaml

    export:
      td:
        apikey: YOUR/API_KEY

    +step1:
      td_load>: config/connector1.yml
      database: prod
      table: raw

:command:`td>: FILE.yml`
  Path to a YAML template file. This configuration needs to be guessed using td command.

  * :command:`td>: config/from_s3.sql`

:command:`database: NAME`
  Name of the database load data to.

  * :command:`database: my_database`

:command:`table: NAME`
  Name of the table load data to.

  * :command:`table: my_table`

:command:`apikey: APIKEY`
  API key.

  * :command:`apikey: 992314/abcdef0123456789abcdef0123456789`


td_ddl>: Treasure Data operations
----------------------------------

**_type: td_ddl** operator runs an operational task on Treasure Data.

TODO: add more description here

.. code-block:: yaml

    export:
      td:
        apikey: YOUR/API_KEY
        database: www_access

    +step1:
      _type: td_ddl
      create_table: my_table_${session_date_compact}
    +step2:
      _type: td_ddl
      drop_table: my_table_${session_date_compact}
    +step2:
      _type: td_ddl
      empty_table: my_table_${session_date_compact}

:command:`create_table: NAME`
  Create a new table if not exists.

  * :command:`create_table: my_table`

:command:`empty_table: NAME`
  Create a new table (drop it first if it exists).

  * :command:`empty_table: my_table`

:command:`drop_table: NAME`
  Drop a table if exists.

  * :command:`drop_table: my_table`

:command:`apikey: APIKEY`
  API key.

  * :command:`apikey: 992314/abcdef0123456789abcdef0123456789`


mail>: Sending email
----------------------------------

**mail>:** operator sends an email.

To use Gmail SMTP server, you need to do either of:

  a) Generate a new app password at `App passwords <https://security.google.com/settings/security/apppasswords>`_. This needs to enable 2-Step Verification first.

  b) Enable access for less secure apps at `Less secure apps <https://www.google.com/settings/security/lesssecureapps>`_. This works even if 2-Step Verification is not enabled.

.. code-block:: yaml

    export:
      mail:
        host: smtp.gmail.com
        port: 587
        from: "you@gmail.com"
        username: "you@gmail.com"
        password: "...password..."
        debug: true

    +step1:
      mail>: body.txt
      subject: workflow started
      to: [me@example.com]

    +step2:
      _type: mail
      body: this is email body in string
      subject: workflow started
      to: [me@example.com]

    +step3:
      sh>: this_task_might_fail.sh
      error:
        mail>: body.txt
        subject: this workflow failed
        to: [me@example.com]

:command:`mail>: FILE`
  Path to a mail body template file. This file can contain ``${...}`` syntax to embed variables.

  * :command:`mail>: mail_body.txt`

:command:`subject: SUBJECT`
  Subject of the email.

  * :command:`subject: Mail From Digdag`

:command:`body: TEXT`
  Email body if tempalte file path is not set.

  * :command:`body: Hello, this is from Digdag`

:command:`to: [ADDR1, ADDR2, ...]`
  To addresses.

  * :command:`to: [analyst@examile.com]`

:command:`from: ADDR`
  From address.

  * :command:`from: admin@example.com`

:command:`host: NAME`
  SMTP host name.

  * :command:`host: smtp.gmail.com`

:command:`port: NAME`
  SMTP port number.

  * :command:`port: 587`

:command:`username: NAME`
  SMTP login username if authentication is required me.

  * :command:`username: me`

:command:`password: APIKEY`
  SMTP login password.

  * :command:`password: MyPaSsWoRd`

:command:`tls: BOOLEAN`
  Enables TLS handshake.

  * :command:`tls: true`

:command:`ssl: BOOLEAN`
  Enables legacy SSL encryption.

  * :command:`ssl: false`

:command:`html: BOOLEAN`
  Uses HTML mail (default: false).

  * :command:`html: true`

:command:`debug: BOOLEAN`
  Shows debug logs (default: false).

  * :command:`debug: false`

:command:`attach_files: ARRAY`
  Attach files. Each element is an object of:

  * :command:`path: FILE`: Path to a file to attach.

  * :command:`content_type`: Content-Type of this file. Default is application/octet-stream.

  * :command:`filename`: Name of this file. Default is base name of the path.

  Example:

  .. code-block:: yaml

      attach_files:
        - path: data.csv
        - path: output.dat
          filename: workflow_result_data.csv
        - path: images/image1.png
          content_type: image/png

embulk>: Embulk data transfer
----------------------------------

**embulk>:** operator runs `Embulk <http://www.embulk.org>`_ to transfer data across storages including local files.

.. code-block:: yaml

    +load:
      embulk>: data/load.yml

:command:`embulk>: FILE.yml`
  Path to a configuration template file.

  * :command:`embulk>: embulk/mysql_to_csv.yml`
