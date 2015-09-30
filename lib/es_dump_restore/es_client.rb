require 'uri'
require 'httpclient'
require 'multi_json'

module EsDumpRestore
  class EsClient
    attr_accessor :base_uri
    attr_accessor :index_name

    def initialize(base_uri, index_name, type)
      @httpclient = HTTPClient.new
      @index_name = index_name

      @es_uri = base_uri
      @path_prefix = type.nil? ? index_name : index_name + "/" + type
    end

    def mappings
      data = request(:get, "#{@path_prefix}/_mapping")
      if data.values.size != 1
        raise "Unexpected response: #{data}"
      end
      mappings = data.values.first
      if mappings["mappings"].nil?
        mappings
      else
        mappings["mappings"]
      end
    end

    def settings
      data = request(:get, "#{@index_name}/_settings")
      if data.values.size != 1
        raise "Unexpected response: #{data}"
      end
      settings = data.values.first
      if settings["settings"].nil?
        settings
      else
        settings["settings"]
      end
    end

    def start_scan(&block)
      scroll = request(:get, "#{@path_prefix}/_search",
        query: { search_type: 'scan', scroll: '10m', size: 500 },
        body: MultiJson.dump({ query: { match_all: {} } }) )
      total = scroll["hits"]["total"]
      scroll_id = scroll["_scroll_id"]

      yield scroll_id, total
    end

    def each_scroll_hit(scroll_id, &block)
      done = 0
      loop do
        batch = request(:get, '_search/scroll', {query: {
          scroll: '10m', scroll_id: scroll_id
        }}, [404])

        batch_hits = batch["hits"]
        break if batch_hits.nil?
        hits = batch_hits["hits"]
        break if hits.empty?

        hits.each do |hit|
          yield hit
        end

        total = batch_hits["total"]
        done += hits.size
        break if done >= total
      end
    end

    def create_index(metadata, overrides)
      if overrides
        overrides = MultiJson.load(overrides)
        metadata = deep_merge(metadata, overrides)
      end
      request(:post, "#{@path_prefix}", :body => MultiJson.dump(metadata))
    end

    def check_alias(alias_name)
      # Checks that it's possible to do an atomic restore using the given alias
      # name.  This requires that:
      #  - `alias_name` doesn't point to an existing index
      #  - `index_name` doesn't point to an existing index
      existing = request(:get, "_aliases")
      if existing.include? index_name
        raise "There is already an index called #{index_name}"
      end
      if existing.include? alias_name
        raise "There is already an index called #{alias_name}"
      end
    end

    def replace_alias_and_close_old_index(alias_name)
      existing = request(:get, "_aliases")

      # Response of the form:
      #   { "index_name" => { "aliases" => { "a1" => {}, "a2" => {} } } }
      old_aliased_indices = existing.select { |name, details|
        details.fetch("aliases", {}).keys.include? alias_name
      }
      old_aliased_indices = old_aliased_indices.keys

      # For any existing indices with this alias, remove the alias
      # We would normally expect 0 or 1 such index, but several is
      # valid too
      actions = old_aliased_indices.map { |old_index_name|
        { "remove" => { "index" => old_index_name, "alias" => alias_name } }
      }

      actions << { "add" => { "index" => index_name, "alias" => alias_name } }

      request(:post, "_aliases", :body => MultiJson.dump({ "actions" => actions }))
      old_aliased_indices.each do |old_index_name|
        request(:post, "#{old_index_name}/_close")
      end
    end

    def bulk_index(data)
      request(:post, "#{@path_prefix}/_bulk", :body => data)
    end

    private

    def request(method, path, options={}, extra_allowed_exitcodes=[])
      request_uri = @es_uri + "/" + path
      begin
        response = @httpclient.request(method, request_uri, options)
        unless response.ok? or extra_allowed_exitcodes.include? response.status
          raise "Request failed with status #{response.status}: #{response.reason} #{response.content}"
        end
        MultiJson.load(response.content)
      rescue Exception => e
        puts "Exception caught issuing HTTP request to #{request_uri}"
        raise e
      end
    end

    def deep_merge(hash1, hash2)
      merger = proc { |key, v1, v2|
        if Hash === v1 && Hash === v2
          v1.merge(v2, &merger)
        else
          v2
        end
      }
      hash1.merge(hash2, &merger)
    end
  end
end
