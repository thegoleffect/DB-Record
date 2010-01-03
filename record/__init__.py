#!/usr/bin/python

class ImproperUsageError(Exception): pass

def Record_Instance(db_handler):
  class Record(object):
    def __init__(self, table_name):
      self.db = db_handler
      self.table_name = table_name
      self.result = None
      self.operators = {
          'eq':'=',
          'neq':'!=',
          'lt':'<',
          'gt':'>',
          'lte':'<=',
          'gte':'>=',
          'like':'LIKE',
          'nlike':'NOT LIKE',
          'regexp':'REGEXP',
          # 'nt':'NOT'
          # still need:  not in, not null, between, exists @@@3
      }
    
    def id(self, _id):
      """Returns sql result for the given id."""
      sql = """SELECT * FROM %s where id = '%s'""" % (self.table_name, _id)
      self.result = self.db.query(sql)
      return self
      
    def where(self, key, value):
      sql = """SELECT * FROM %s where `%s` = '%s'""" %(self.table_name, key, value)
      self.result = self.db.query(sql)
      return self
      
    def new(self, _data):
      """Creates a table row based on the given data and returns id on success; False on fail."""
      keys, values = zip(*_data.items()) # transpose to get keys, values
      sql = "INSERT INTO %s (%s) VALUES (%s)" % (self.table_name, ", ".join(keys), ", ".join(["%s" for x in range(len(keys))]))
      
      print sql
      print values 
        
      try:
        self.result = {'id':self.db.execute(sql, *values)}
      except Exception, e:
        print "Error:", e
      # self.last_insert_id()
      return self
      
    def all(self):
      sql = "SELECT * FROM %s" %(self.table_name)
      self.result = self.db.query(sql)
      return self
      
    def last_insert_id(self):
      sql = "SELECT last_insert_rowid()"
      self.result = self.db.query(sql)[0]['last_insert_rowid()']
      return self
      
    def update(self, _data):
      "Complete or partial update of an SQL row.  Saves list of updated ids in self.result."
      # print self.result
      results = self.result
      self.result = []
      try:
        for result in results:
          if result.has_key('id'):
            _id = result['id']
            sql = """UPDATE %s SET %s WHERE id = %s""" %(self.table_name, ", ".join(["%s='%s'"%(k,v) for k,v in _data.items()]), _id)
            # print sql
            self.db.execute(sql)
            self.result.append({'id':_id})
      except TypeError:
        raise ImproperUsageError    
      return self
        
    def _update(self, _id, _data, table_name=None):
      table_name = table_name or self.table_name
      sql = """UPDATE %s SET %s WHERE id = %s""" %(table_name, ", ".join(["%s='%s'"%(k,v) for k,v in _data.items()]), _id)
      self.db.execute(sql)
        
    def delete(self, disable=False):
      if self.result and len(self.result) > 0:
        new_result = []
        for result in self.result:
          if result.has_key('id'):
            _id = result['id']
            if disable:
              del result['id']
              keys, values = zip(*result.items())              
              sql = "INSERT INTO %s (%s) VALUES (%s)" % (self.table_name + "_disabled", ", ".join(keys), ", ".join(["%s" for x in range(len(keys))]))
              self.db.execute(sql, *values)
            self._delete(_id)
            new_result.append(_id)
        self.result = new_result
        return self
      else:
        raise ImproperUsageError
        
    def _delete(self, _id, table_name=None):
      table_name = table_name or self.table_name
      sql = """DELETE FROM %s WHERE id = '%s'""" %(self.table_name, _id)
      self.db.execute(sql)
        
    def disable(self):
      self.delete(disable=True)
      return self
      
    # The following functions replicate conditional operators 
    def _op(self, key, value, operator):
      sql = "SELECT * FROM %s WHERE `%s` %s '%s'" %(self.table_name, key, operator, value)
      self.result = self.db.query(sql)
      # print sql, "=", self.result
      return self
    
    def __getattr__(self, name):
      if self.operators.has_key(name.lower()):
        return lambda k,v,: self._op(k, v, self.operators[name.lower()])
  return Record