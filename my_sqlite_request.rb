require 'colorize'
require 'csv'
class String
    def numeric?
        match(/\A[+-]?\d+?(_?\d+)*(\.\d+e?\d*)?\Z/) == nil ? false : true
    end
end

class MySqliteRequest
    def initialize
        @the_update_and_the_delete_indexes = []
        @the_updated_rows = []
        @where_conditions = []

        @the_insert_or_the_update = nil
        @the_selected = nil
        @the_table = nil
        @to_the_delete = nil
        @to_the_insert = nil
        @to_the_select = nil

        @the_delete = false
        @the_join = false
        @the_insert = false
        @the_select = false
        @the_update = false
        @is_where = false
    end

    def from(table_name)
        if File.exist?(table_name)
            @the_table = CSV.parse(File.read(table_name), headers: true)
            @to_the_delete = table_name
        elsif File.exist?(table_name += '.csv')
            @the_table = CSV.parse(File.read(table_name), headers: true)
            @to_the_delete = table_name
        else
            puts "CSV table not found: '#{table_name}'".red
            exit(false)
        end
        return self
    end

    def select(column_name)
        # if @the_table == nil
        #     puts 'Table not found'.red
        #     return self
        # end
        @the_select = true
        @to_the_select = column_name
        self
    end

    def doSelecting
        @the_selected = Marshal.load(Marshal.dump(@the_table))
        if @to_the_select == '*'
            return self
        end
        if @to_the_select.class == Array
            the_names = @the_selected.headers
            the_names.each do |name|
                if !@to_the_select.include?(name) && name != nil
                   @the_selected.delete(name)
                end
            end
            @the_selected.to_csv
            return self
        elsif @to_the_select.class == String
            the_names = @the_selected.headers
            the_names.each do |name|
                if @to_the_select != name && name != nil
                   @the_selected.delete(name)
                end
            end
            @the_selected.to_csv
            return self
        else
            puts "Selecting error: '#{@to_the_select.class}' does not support".red
            return self
        end
    end

    def isExisting(param_5, param_6) 
        return (param_5.headers.include?(param_6)) ? true : false
    end

    def where(column_name, criteria)
        @is_where = true
        @where_conditions << column_name
        @where_conditions << criteria
        return self
    end

    def isWhere
        if @the_table == nil
            puts 'Table not found'.red
            exit(false)
        end
        if @the_update || @the_delete
            for i in 0..@the_table.length - 1 do
                if @the_table.by_row[i][@where_conditions[0]] == @where_conditions[1]
                    @the_update_and_the_delete_indexes << i
                end
            end
            return self
        end
        the_result = CSV::Table.new([], headers:@the_table.headers.dup)
        @the_table.filter do |the_raw|
            if the_raw[@where_conditions[0]] == @where_conditions[1].to_s
                the_result << the_raw.dup
            end
        end
        if @the_select && the_result != nil
            the_temp = the_result.headers.dup
            the_temp.each do |the_col|
                if !isExisting(@the_selected, the_col)
                    the_result.delete(the_col)
                end
            end
            @the_selected = Marshal.load(Marshal.dump(the_result))
        else
            @the_table = the_result.clone
        end  
    end

    def theConcat(param_7, param_8, indexes)
        the_hed = param_8.headers
        indexes.each { |i, j|
            the_hed.each do |k|
                if !isExisting(param_7, k)
                    # puts "#{k} - #{param_7.by_row[i]}"
                    param_7.by_row[i][k] = param_8.by_row[j][k]
                    # puts "#{j} - #{param_7.by_row[i]}"
                end
            end
        }
        return param_7
    end

    def getTheMatchCol(param_9, param_10, param_11)
        for i in 0..param_9.length - 1 do
            if param_9.by_row[i][param_10] == param_11
                # puts "#{i} << #{param_9.by_row[i]} === #{param_11}"
                return i
            end
        end
        nil
    end

    def join(column_on_db_a, filename_db_b, column_on_db_b)
        @the_join = true
        new_people_data = CSV.parse(File.read(filename_db_b), headers: true)
        the_matched = CSV::Table.new([], headers:true)
        the_indexes = Hash.new
        for row_in in 0..@the_table.length - 1 do
            i = getTheMatchCol(new_people_data, column_on_db_b, @the_table.by_row[row_in][column_on_db_a])
            if i != nil
                the_matched << new_people_data.by_row[i]
                the_indexes[row_in] = i
            end
        end
        @the_table = theConcat(@the_table, the_matched, the_indexes)
        # @the_table.each do |j|
        #     print j.to_hash
        # end
        # @the_table << new_people_data[column_on_db_b]
        return self
    end

    def theReverse(param_12, param_13)
        i = 0
        while i < param_13
            the_temp = param_12[i]
            param_12[i] = param_12[param_13]
            param_12[param_13] = the_temp
            i += 1
            param_13 -= 1
        end
        return param_12
    end

    def order(order, column_name)
        if order != 'asc' && order != 'desc'
            puts "Ordering failure: '#{order}' type is not avaiable".red
            return self
        end
        the_sorted = nil
        for i in 0..@the_table.headers.length() do
            if @the_table.headers[i] == column_name
                if column_name.numeric? 
                    the_sorted = @the_table.sort_by{|the_line| the_line[i].to_i}
                else
                    the_sorted = @the_table.sort_by{|the_line| the_line[i].to_s}
                end
                if order == 'desc'
                    the_sorted = theReverse(the_sorted, the_sorted.length() - 1)
                end
                @the_table.delete_if do |the_row|
                    true
                end
                i = 0
                the_sorted.each do |the_row|
                    @the_table << Marshal.load(Marshal.dump(the_row))
                    i += 1
                end
                return self
            end
        end
        return self
    end

    def insert(table_name)
        @the_insert = true
        if File.exist?(table_name)
            @to_the_insert = CSV.parse(File.read(table_name), headers: true)
        elsif File.exist?(table_name += '.csv')
            @to_the_insert = CSV.parse(File.read(table_name), headers: true)
        else
            puts "MySqlite; table not found: '#{table_name}'".red
            exit(false)
        end
        @the_insert_or_the_update = table_name
        return self
    end

    def toWriteCSV(param_14, param_15)
        CSV.open(param_14, 'w') do |the_csv|
            the_csv << param_15.headers
            param_15.each do |the_row|
              the_csv << the_row
            end
        end
    end

    def values(data)
        if @to_the_insert == nil
            puts 'Cannot insert. Table not found'.red
            return self
        end
        if data.class != Hash && data.class != Array
            puts "Type not support: '#{data.class.to_s.red}'"
            return self
        end
        the_row = nil
        if data.class == Hash
            the_temp = Hash.new('undefined')
            @to_the_insert.headers.each do |the_key|
                the_temp[the_key.to_sym.to_s] = data[the_key.to_sym]
            end
            the_row = CSV::Row.new(the_temp.keys, the_temp.values, headers: true)
        elsif data.class == Array
            the_row = CSV::Row.new(@to_the_insert.headers, data, headers: true)
        end
        @to_the_insert << the_row
        return self
    end

    def update(table_name)
        @the_update = true
        @the_insert_or_the_update = table_name
        if File.exist?(table_name)
            @the_table = CSV.parse(File.read(table_name), headers: true)
            @to_the_delete = table_name
        elsif File.exist?(table_name += '.csv')
            @the_table = CSV.parse(File.read(table_name), headers: true)
            @to_the_delete = table_name
        else
            puts "MySqlite; table not found: '#{table_name}'".red
            exit(false)
        end
        return self
    end

    def changeTheHash(param_16, param_17)
        param_17.each { |key, value|
            if param_16.has_key?(key.to_s)
                param_16[key.to_s] = value
            end
        }
        return param_16
    end

    def set(data)
        for i in 0..@the_table.length - 1 do
                the_en = @the_table.by_row[i].to_hash
                the_en = changeTheHash(the_en, data)
                the_en = CSV::Row.new(the_en.keys, the_en.values, headers: true)
                @the_updated_rows << the_en
        end
        return self
    end

    def delete
        @the_delete = true
        return self
    end

    def run
        if @the_select
            doSelecting
            if @is_where
                isWhere
            end
            @the_table = @the_selected.dup
            @the_table.each do |the_row|
                print "#{the_row.to_hash}\n"
            end
        end
        if @the_insert
            toWriteCSV(@the_insert_or_the_update, @to_the_insert)
            # @to_the_insert.each do |the_row|
            #     p the_row.to_hash
            # end
            puts 'Success'.green
        elsif @the_update
            if @is_where
                isWhere
            end
            if @the_update_and_the_delete_indexes.length < 1
                puts 'Table not updated'.red
                return self
            end
            the_temp = CSV::Table.new([], headers:@the_table.headers.dup)
            index = 0
            for i in 0..@the_table.length - 1 do 
                if @the_update_and_the_delete_indexes.include?(i)
                    the_temp << @the_updated_rows[@the_update_and_the_delete_indexes[index]]
                    index += 1
                else
                    the_temp << @the_table.by_row[i]        
                end
            end
            toWriteCSV(@the_insert_or_the_update, the_temp)
            puts 'Success'.green
        elsif @the_delete
            if @is_where
                isWhere
            end
            if @the_update_and_the_delete_indexes.length < 1
                puts 'Cannot delete. Criteria not found'.red
                return self
            end
            @the_update_and_the_delete_indexes.each do |i|
                @the_table.delete(i)
            end
            if @to_the_delete != nil
                toWriteCSV(@to_the_delete, @the_table)
            end
            puts 'Success'.green
        end
    end
end

# This are for create.

# request = MySqliteRequest.new
# request.select(['name', 'position', 'year_start']).from('people_data.csv').order('asc', 'year_start').run
# request = request.select(['name', 'position', 'college', 'year_start'])
# request = request.from('people_data.csv')
# request = request.order('asc', 'year_start')
# request = request.where('position', 'A')
# request.run

# instance = MySqliteRequest.new
# instance = instance.insert('new_people_data.csv')
# instance = instance.values(['Muhammadqodir', '2020', '2022', 'T', '120', '65', 'Aug 28, 2004', 'Insitut'])
# instance.run

# to_update = MySqliteRequest.new
# to_update = to_update.update('new_people_data.csv')
# to_update = to_update.set({'college': 'Tatu'})
# to_update = to_update.where('year_start', '2018')
# to_update.run

# delet = MySqliteRequest.new
# delet = delet.from('new_people_data.csv')
# delet = delet.delete
# delet = delet.where('name', 'Muhammadqodir')
# delet.run

# to_join = MySqliteRequest.new
# to_join = to_join.select(['name', 'type', 'team', 'year_start', 'college'])
# to_join = to_join.from('new_people_data.csv')
# to_join = to_join.join('year_end', 'people_data.csv', 'year_start')
# to_join = to_join.where('college', 'Moliya')
# to_join.run