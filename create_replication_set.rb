#!/usr/bin/ruby

##
## create_replication_set.rb
## dynamically create replication_set for pglogical
##
require 'pg'
require 'optparse'

## methods
def db_connect(hostname, database)
  begin
    connection = PG::Connection.open(
      :host   => hostname,
      :port   => '5432',
      :dbname => database,
      :user   => 'postgres'
    )
  rescue PG::ConnectionBad => e
    raise e
    exit
  end
end  

def create_replication_set(connection, replication_set)
  begin
    result = connection.exec_params("SELECT pglogical.create_replication_set('#{replication_set}')")
  rescue => e
    puts e
  end
end

def drop_replication_set(connection, replication_set)
  begin
    result = connection.exec_params("SELECT pglogical.drop_replication_set('#{replication_set}')")
  rescue => e
    puts e
  end
end

def add_sequences(connection, replication_set, schema)
  begin
    result = connection.exec_params("SELECT pglogical.replication_set_add_all_sequences( set_name := '', schema_names := ARRAY['#{schema}'], synchronize_data := true )")
  rescue => e
    puts e
  end
end
  
def fetch_tables(connection, schema) 
  tables = connection.exec_params("SELECT table_name FROM information_schema.tables WHERE table_schema = '#{schema}' AND table_type = 'BASE TABLE';")
end

def add_table(connection, replication_set, table)
  begin 
    result = connection.exec_params("SELECT pglogical.replication_set_add_table( set_name := '#{replication_set}', relation := '#{table}', synchronize_data := true );")
    puts "#{table} added to #{replication_set}"
  rescue => e
    #puts e
  end
end

## end methods
## main

## options
options = Hash.new
opt_parser = OptionParser.new do |opts|
  opts.banner = "Usage: #{$0} [OPTIONS]"
  opts.on('-d DBNAME', '--dbname=DBNAME', String, 'database name') do |v|
    options[:dbname] = v
  end
  opts.on('-s SCHEMA', '--schema=SCHEMA', String, 'database schema') do |v|
    options[:schema] = v
  end
  opts.on('-r REPSET', '--replication-set=REPSET', String, 'name of plogical replication set') do |v|
    options[:repset] = v
  end
  opts.on('-h', '--help', 'help') do
    puts opts
    exit
  end
end
opt_parser.parse!

## require database name
if options[:dbname].nil?
  puts "please specify dbname"
  puts opt_parser
  exit
end

if options[:schema].nil?
  options[:schema] = 'public'
end

if options[:repset].nil?
  options[:repset] = 'tevo'
end

connection = db_connect('localhost', options[:dbname])
create_replication_set(connection, options[:repset])
add_sequences(connection, options[:repset], options[:schema])

tables = fetch_tables(connection, options[:schema])
tables.each do |table|
  add_table(connection, options[:repset], table['table_name'])
end
#drop_replication_set(connection, options[:repset])

## end main
