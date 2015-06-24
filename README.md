# es_dump_restore

A utility for safely dumping the contents of an ElasticSearch index to a compressed file and restoring it
later on.  This can be used for backups or for cloning an ElasticSearch index without needing to take down
the server.

The file format is a ZIP file containing the index metadata, the number of objects in the index, and a
series of commands to be sent to the ElasticSearch bulk API.

## Installation

    gem install es_dump_restore

## Usage

To dump an ElasticSearch index to a file:

    es_dump_restore dump ELASTIC_SEARCH_SERVER_URL INDEX_NAME DESTINATION_FILE_ZIP

To dump an ElasticSearch index by type to a file:

    es_dump_restore dump ELASTIC_SEARCH_SERVER_URL INDEX_NAME TYPE DESTINATION_FILE_ZIP

To restore an index to an ElasticSearch server:

    es_dump_restore restore ELASTIC_SEARCH_SERVER_URL DESTINATION_INDEX FILENAME_ZIP [SETTING_OVERRIDES] [BATCH_SIZE]

To restore an index and set an alias to point to it:

    es_dump_restore restore_alias ELASTIC_SEARCH_SERVER_URL DESTINATION_ALIAS DESTINATION_INDEX FILENAME_ZIP [SETTING_OVERRIDES] [BATCH_SIZE]

This loads the dump into an index named `DESTINATION_INDEX`, and once the load
is complete sets the alias `DESTINATION_ALIAS` to point to it.  If
`DESTINATION_ALIAS` already exists, it will be atomically changed to point to
the new location.  This allows a dump file to be loaded on a running server
without disrupting searches going on on that server (as long as those searches
are accessing the index via the alias).

If `SETTING_OVERRIDES` is set for a restore command, it must be a valid
serialised JSON object.  This will be merged with the settings in the dump
file, allowing selected settings to be altered, but keeping any unspecified
settings as they were in the dump file.  For example:

    es_dump_restore restore_alias http://localhost:9200 test test-1276512 test_dump.zip '{"settings":{"index":{"number_of_replicas":"0","number_of_shards":"1"}}'

would read the dump file `test_dump.zip`, load it into an index called
`test-1276512`, then set the alias `test` to point to this index.  The index
would be set to have no replicas, and only 1 shard, but have all other settings
from the dump file.

If `BATCH_SIZE` is set for a restore command, it controls the number of
documents which will be sent to elasticsearch at once.  This defaults to 1000,
which is normally fine, but if you have particularly complex documents or
mappings this might need reducing to avoid timeouts.

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
