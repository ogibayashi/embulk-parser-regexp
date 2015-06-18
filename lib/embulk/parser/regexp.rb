require "time"

module Embulk
  module Parser

    class RegexpParserPlugin < ParserPlugin
      Plugin.register_parser("regexp", self)
      BOOLEAN_TYPES = ["yes", "true", "1"]

      class UnmatchedLineException < RuntimeError
      end
      
      def self.transaction(config, &control)
        # configuration code:
        task = {
          "format" => config.param("format", :string),
          "field_types" => config.param("field_types", :array),
          "ignore_unmatched_line" => config.param("ignore_unmatched_line",:bool, default:false)
        }

        # set default value
        task["field_types"].each do |v|
          case v["type"]
          when "timestamp"
            v["opts"]["time_format"] ||= '%d/%b/%Y:%T %z'
          end
        end
        
        columns = task["field_types"].each_with_index.map do |c,i|
          Column.new(i, c["name"], c["type"].to_sym)
        end

        yield(task, columns)
      end

      def init
        # initialization code:
        @regexp = Regexp.new task["format"]
        @field_types = task["field_types"]
        @ignore_unmatched_line = task["ignore_unmatched_line"]
      end

      def run(file_input)
        decoder_task = @task.load_config(Java::LineDecoder::DecoderTask)
        decoder = Java::LineDecoder.new(file_input.instance_eval { @java_file_input }, decoder_task)
        while decoder.nextFile
          while line = decoder.poll
            record = []
            if (m = @regexp.match line)
              task["field_types"].each do |v|
                record << type_convert(m[v["name"]], v["type"],v["opts"])
              end
              page_builder.add record
            else
              raise UnmatchedLineException, "Unmatched line: #{line}" unless @ignore_unmatched_line
            end

          end
        end
        page_builder.finish
      end

      private

      def type_convert(v, field_type,opts={  })
        case field_type
          when "string"
            v
          when "long"
            v.to_i
          when "double"
            v.to_f
          when "timestamp"
            Time.strptime(v, opts["time_format"])
          when "boolean"
            BOOLEAN_TYPES.include? v.downcase
          else
            raise "unsupported type #{field_type}"
        end
      end
    end

  end
end
