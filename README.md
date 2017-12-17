# MapReduce

`python setup.py install --record files.txt` - to install the lib (files.txt for removing it then)

* `mr --start-master host port` - start master
* `mr --start-slave port master-host master-port size-limit` - start slave
* `mr --write cluster_path` - write data
* `mr --read cluster_path` - read table
* `mr --delete cluster_path` - remove table
* `mr --list-dir cluster_path` - get everything inside cluster (just checking common prefix)
* `mr --map path_in path_out run_command file1 … filen` - run mapper -> results in operation id
* `mr --list-operations` - empty
* `mr --table-info cluster_path` - table info: total size (in chars) + how many nodes it is distributed among

# Things to pay attention

- `/free_space/` - this folder is for saving files sent to perform map, can be changed :(in the code): in `mr/slave/file_manager.py`
