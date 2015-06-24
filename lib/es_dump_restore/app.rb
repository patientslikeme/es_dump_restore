require "es_dump_restore/es_client"
require "es_dump_restore/dumpfile"
require "thor"
require "progress_bar"
require "multi_json"

module EsDumpRestore
  class App < Thor
    
    option :noprogressbar, :type => :boolean # switch for showing progress bar
    option :verbose, :type => :boolean # add some additional output

    desc "dump URL INDEX_NAME FILENAME", "Creates a dumpfile based on the given ElasticSearch index"
    def dump(url, index_name, filename)
      client = EsClient.new(url, index_name, nil)

      Dumpfile.write(filename) do |dumpfile|
        dumpfile.index = {
          settings: client.settings,
          mappings: client.mappings
        }

        client.start_scan do |scroll_id, total|
          dumpfile.num_objects = total
          bar = ProgressBar.new(total) unless options[:noprogressbar]

          dumpfile.get_objects_output_stream do |out|
            client.each_scroll_hit(scroll_id) do |hit|
              metadata = { index: { _type: hit["_type"], _id: hit["_id"] } }
              out.write("#{MultiJson.dump(metadata)}\n#{MultiJson.dump(hit["_source"])}\n")
              bar.increment! unless options[:noprogressbar]
            end
          end
        end
      end
    end

    desc "dump_type URL INDEX_NAME TYPE FILENAME", "Creates a dumpfile based on the given ElasticSearch index"
    def dump_type(url, index_name, type, filename)
      client = EsClient.new(url, index_name, type)

      Dumpfile.write(filename) do |dumpfile|
        dumpfile.index = {
          settings: client.settings,
          mappings: client.mappings
        }

        client.start_scan do |scroll_id, total|
          dumpfile.num_objects = total
          bar = ProgressBar.new(total) unless options[:noprogressbar]

          dumpfile.get_objects_output_stream do |out|
            client.each_scroll_hit(scroll_id) do |hit|
              metadata = { index: { _type: hit["_type"], _id: hit["_id"] } }
              out.write("#{MultiJson.dump(metadata)}\n#{MultiJson.dump(hit["_source"])}\n")
              bar.increment! unless options[:noprogressbar]
            end
          end
        end
      end
    end

    desc "restore URL INDEX_NAME FILENAME", "Restores a dumpfile into the given ElasticSearch index"
    def restore(url, index_name, filename, overrides = nil, batch_size = 1000)
      client = EsClient.new(url, index_name, nil)

      Dumpfile.read(filename) do |dumpfile|
        client.create_index(dumpfile.index, overrides)

        bar = ProgressBar.new(dumpfile.num_objects) unless options[:noprogressbar]
        dumpfile.scan_objects(batch_size.to_i) do |batch, size|
          client.bulk_index batch
          bar.increment!(size) unless options[:noprogressbar]
        end
      end
    end

    desc "restore_alias URL ALIAS_NAME INDEX_NAME FILENAME", "Restores a dumpfile into the given ElasticSearch index, and then sets the alias to point at that index, removing any existing indexes pointed at by the alias"
    def restore_alias(url, alias_name, index_name, filename, overrides = nil,
                      batch_size = 1000)
      client = EsClient.new(url, index_name, nil)
      client.check_alias alias_name

      Dumpfile.read(filename) do |dumpfile|
        client.create_index(dumpfile.index, overrides)

        bar = ProgressBar.new(dumpfile.num_objects) unless options[:noprogressbar]
        dumpfile.scan_objects(batch_size.to_i) do |batch, size|
          client.bulk_index batch
          bar.increment!(size) unless options[:noprogressbar]
        end
      end

      client.replace_alias_and_close_old_index alias_name
    end

  end
end
