require 'readline'
require './my_sqlite_request'

class Sqlite_cli
    def initialize
        @the_command = nil

        @the_delete = false
        @the_insert = false
        @the_select = false
        @the_update = false

        @the_runner = MySqliteRequest.new
    end

    def isCleaner(param_1)
        param_1 = param_1.gsub(/,/, ' ')
        param_1 = param_1.gsub(/\(/, ' ')
        param_1 = param_1.gsub(/\)/, ' ')
        param_1 = param_1.gsub(/;/, '')
        @the_command = param_1.split
    end

    def choice(param_2, param_3)
        return (param_2.downcase == param_3.downcase) ? true : false
    end

    def setTheCommand()
        a_command = @the_command[0]
        if choice(a_command, 'select')
            @the_select = true
        elsif choice(a_command, 'delete')
            @the_runner.delete
            @the_delete = true
        elsif choice(a_command, 'update')
            @the_update = true
        elsif choice(a_command, 'insert')
            @the_insert = true
        else
            puts "Command not found #{a_command}".red
            exit(false)
        end
    end

    def selecting()
        selects = []
        for i in 1..@the_command.length do
            if !choice(@the_command[i], 'from')
                selects << @the_command[i]
            else
                break
            end
        end
        if selects.length == 1
            @the_runner =  @the_runner.select(selects.join(''))
        else
            @the_runner = @the_runner.select(selects)
        end
    end

    def deleting()
        for i in 2..@the_command.length do
            cmd = @the_command[i]
            if choice(cmd, 'where') && i + 3 <= @the_command.length
                @the_runner = @the_runner.where(@the_command[i + 1], @the_command[i + 3])
                break
            end
        end
    end

    def inserting
        for i in 1..@the_command.length - 1 do
            if choice(@the_command[i], 'into') && i + 1 < @the_command.length
                @the_runner = @the_runner.insert(@the_command[i + 1])
                break
            end
        end
        k = @the_command.find_index('values')
        k += 1
        values = []
        for j in k..@the_command.length - 1
            values << @the_command[j]
        end
        @the_runner = @the_runner.values(values)
    end

    def getTheEnd
        for i in 3..@the_command.length - 1 do
            if choice(@the_command[i], 'where')
                return i - 3
            end
        end
    end

    def theArrayToTheHash(param_4)
        param_4.delete('=')
        hashing = Hash.new
        i = 1
        while i < param_4.length
            hashing[param_4[i - 1]] = param_4[i]
            i += 2
        end
        return hashing
    end

    def updating
        if @the_command.length < 5
            puts 'There is little argument for update'.red
            exit(false)
        end
        @the_runner = @the_runner.update(@the_command[1])
        if choice(@the_command[2], 'set')
            the_temp = @the_command.slice(3, getTheEnd)
            @the_runner = @the_runner.set(theArrayToTheHash(the_temp))
        end
        if choice(@the_command[getTheEnd + 3], 'where') && getTheEnd + 6 <= @the_command.length
            @the_runner = @the_runner.where(@the_command[getTheEnd + 4], @the_command[getTheEnd + 6])
        end
    end

    def doFrom()
        if @the_select || @the_delete
            for i in 1..@the_command.length - 1 do
                if choice(@the_command[i], 'from') && i + 1 < @the_command.length
                    @the_runner = @the_runner.from(@the_command[i + 1])
                    break
                end
            end
        end
    end

    def going
        if @the_select
            selecting
            doFrom
        elsif @the_delete
            deleting
            doFrom
        elsif @the_insert
            inserting
        elsif @the_update
            updating
        else
            puts 'Command not found'
        end
        @the_runner.run
    end

end

puts 'MySQLite version 0.1 2022-02-04'
while quit_button = Readline.readline('my_sqlite_cli> ', true)
    if quit_button == 'quit'
        exit(true)
    end
    click = Sqlite_cli.new
    quit_button = click.isCleaner(quit_button)
    click.setTheCommand
    click.going
end