module Dataframe

  def self.Row(obj)
    if obj.is_a?(Dataframe::RowType)
      obj
    elsif obj.respond_to?(:as_dataframe_row)
      obj.as_dataframe_row
    else
      obj.to_hash.extend(Dataframe::RowType).rowify
    end
  end

  module RowType

    def rowify
      self.default_proc = proc do |h, k|
        case k
          when String then sym = k.to_sym; h[sym] if h.key?(sym)
          when Symbol then str = k.to_s; h[str] if h.key?(str)
        end
      end
      self
    end

    def method_missing(method_sym, *arguments)
      if method_sym.to_s[-1] == "="
        self[method_sym[0..-2].to_sym] = arguments.first
      else
        self[method_sym]
      end
    end

    def pick(*keys)
      keys.map {|k| self[k]}
    end

  end

end
