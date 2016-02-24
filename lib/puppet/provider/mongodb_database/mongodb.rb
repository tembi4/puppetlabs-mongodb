require File.expand_path(File.join(File.dirname(__FILE__), '..', 'mongodb'))
Puppet::Type.type(:mongodb_database).provide(:mongodb, :parent => Puppet::Provider::Mongodb) do

  desc "Manages MongoDB database."

  defaultfor :kernel => 'Linux'

  def self.instances(admin_username = nil, admin_password = nil)
    require 'json'

    if auth_enabled
      Puppet.debug 'Getting list of databases with auth ON'
      dbs = JSON.parse mongo_eval('printjson(db.getMongo().getDBs())', 
                                  'admin', 10, nil, admin_username, admin_password)
    else
      dbs = JSON.parse mongo_eval('printjson(db.getMongo().getDBs())')
    end

    dbs['databases'].collect do |db|
      new(:name   => db['name'],
          :ensure => :present)
    end

  end

  # Assign prefetched dbs based on name.
  def self.prefetch(resources)
    if resources.size > 0
      Puppet.debug "Using #{resources.values[0][:admin_username]} for admin"
      firstResource = resources.values[0]
      dbs = instances(firstResource[:admin_username], firstResource[:admin_password])
      resources.keys.each do |name|
        if provider = dbs.find { |db| db.name == name }
          resources[name].provider = provider
        end
      end
    end
  end

  def create
    if db_ismaster
      mongo_eval('db.dummyData.insert({"created_by_puppet": 1})', @resource[:name])
    else
      Puppet.warning 'Database creation is available only from master host'
    end
  end

  def destroy
    if db_ismaster
      mongo_eval('db.dropDatabase()', @resource[:name])
    else
      Puppet.warning 'Database removal is available only from master host'
    end
  end

  def exists?
    !(@property_hash[:ensure] == :absent or @property_hash[:ensure].nil?)
  end

end
