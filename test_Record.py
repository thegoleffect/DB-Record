#!/usr/bin/python
import sys
sys.dont_write_bytecode = True
import unittest
import application.libraries.sqlite as sqlite
import datetime
import os
from application.models import Record_Instance, ImproperUsageError
import tornado.database

class TestRecord(unittest.TestCase):
  def setUp(self):
    # self.path = '/tmp/test_db.db'
    self.path = ":memory:"
    # self.db = sqlite.Connection(self.path)
    self.db = tornado.database.Connection(
        host="localhost", database="higut",
        user="higut", password="bevyofbeavers"
    )
    self.initialize_db()
    self.Record = Record_Instance(self.db)
    
  def tearDown(self):
    self.db.close()

  def initialize_db(self):
    self.db.execute("""DROP TABLE IF EXISTS stocks""")
    self.db.execute("""DROP TABLE IF EXISTS stocks_disabled""")
    self.db.execute("""CREATE TABLE stocks (id integer auto_increment primary key, date text, trans text, symbol text, qty real, price real)""") 
    self.db.execute("""insert into stocks (date, trans, symbol, qty, price) values (%s, %s, %s, %s, %s)""", '2006-01-05','BUY','RHAT',100,35.14)
    self.db.execute("""CREATE TABLE stocks_disabled (id integer auto_increment primary key, date text, trans text, symbol text, qty real, price real)""")

  def testRecordID(self):
    self.assertNotEqual(self.Record("stocks").id(1).result,[])
    
  def testRecordIDNone(self):
    self.assertEqual(self.Record("stocks").id(2).result,[])
    
  def testRecordWhere(self):
    result1 = self.Record("stocks").where("id", 1).result
    result2 = self.Record("stocks").id(1).result
    self.assertEqual(result1, result2)
    
  def testRecordNew(self):
    data = {'date':u'2010-01-01', 'trans':u'BUY', 'symbol':u'MSFT', 'qty':20, 'price':12.1}
    self.Record("stocks").new(data)
    sample = self.Record("stocks").all().result[1]
    del sample['id']
    self.assertEqual(data, sample)
    
  def testRecordAll(self):  
    self.Record("stocks").new({'date':'2010-01-01', 'trans':'BUY', 'symbol':'MSFT', 'qty':20, 'price':12.12})
    self.Record("stocks").new({'date':'2010-01-01', 'trans':'BUY', 'symbol':'MSFT', 'qty':20, 'price':12.12})
    self.assertNotEqual(len(self.Record("stocks").all().result), 1)
    
  def testNewLastInsertID(self):  
    _id = self.Record("stocks").new({'date':'2010-01-01', 'trans':'BUY', 'symbol':'MSFT', 'qty':20, 'price':12.12}).result['id']
    self.assertEqual(_id, 2)
    
  def testRecordEqID(self):
    result1 = self.Record("stocks").eq("id", "1").result
    result2 = self.Record("stocks").id(1).result
    self.assertEqual(result1, result2)
    
  def testRecordNeqID(self):
    result1 = self.Record("stocks").neq('id', 2).result
    result2 = self.Record("stocks").id(1).result
    self.assertEqual(result1, result2)
    
  def testRecordLtID(self):
    result1 = self.Record("stocks").lt('id', 2).result
    result2 = self.Record("stocks").id(1).result
    self.assertEqual(result1, result2)
    
  def testRecordGtID(self):
    result1 = self.Record("stocks").gt('id', 0).result
    result2 = self.Record("stocks").id(1).result
    self.assertEqual(result1, result2)
    
  def testRecordLteID(self):
    result1 = self.Record("stocks").lte('id', 1).result
    result2 = self.Record("stocks").id(1).result
    self.assertEqual(result1, result2)
    
  def testRecordGteID(self):
    result1 = self.Record("stocks").gte('id', 1).result
    result2 = self.Record("stocks").id(1).result
    self.assertEqual(result1, result2)
    
  def testRecordLikeID(self):
    result1 = self.Record("stocks").like('symbol', 'RH%%').result
    result2 = self.Record("stocks").id(1).result
    self.assertEqual(result1, result2)
    
  def testRecordNlikeID(self):
    result1 = self.Record("stocks").nlike('symbol', 'MS%%').result
    result2 = self.Record("stocks").id(1).result
    self.assertEqual(result1, result2)
    
  # def testRecordRegexpID(self):
  #   """SQLite3 does not have a REGEXP function"""
  #   return
  #   result1 = self.Record("stocks").regexp('symbol', 'RH').result
  #   result2 = self.Record("stocks").id(1).result
  #   self.assertEqual(result1, result2)
    
  def testRecordUpdate(self):
    _data = {'date':'2010-01-01', 'trans':'BUY', 'symbol':'MSFT', 'qty':20, 'price':12.12}
    self.Record("stocks").id(1).update(_data)
    result1 = self.Record("stocks").id(1).result[0]
    del result1['id']
    result2 = _data
    self.assertEqual(result1, result2)
    
  def testRecord_Update(self):
    _data = {'date':'2010-01-01', 'trans':'BUY', 'symbol':'MSFT', 'qty':20, 'price':12.12}
    self.Record("stocks")._update(1, _data)
    result1 = self.Record("stocks").id(1).result[0]
    del result1['id']
    result2 = _data
    self.assertEqual(result1, result2)
    
  def testRecordUpdatePartial(self):
    # do an update w/ only partial data
    # self.fail("Incomplete")
    _data = {'trans':'SELL'}
    self.Record("stocks").id(1).update(_data)
    result1 = self.Record("stocks").id(1).result[0]['trans']
    result2 = _data['trans']
    self.assertEqual(result1, result2)
  
  def testRecordUpdateMultiplePartial(self):
    # do an update w/ multiple rows in self.result
    _data = {'trans':'SELL'}
    self.Record("stocks").new({'date':'2010-01-01', 'trans':'BUY', 'symbol':'MSFT', 'qty':20, 'price':12.12})
    self.Record("stocks").all().update(_data)
    result = self.Record("stocks").all().result
    for each in result:
      self.assertEqual(each['trans'], "SELL")
      
  def testRecordUpdateException(self):
    try:
      self.Record("stocks").update({'date':'2010-01-01', 'trans':'BUY', 'symbol':'MSFT', 'qty':20, 'price':12.12})
    except ImproperUsageError:
      pass
    
  def testRecordDelete(self):
    self.Record("stocks").id(1).delete()
    result = self.Record("stocks").all().result
    self.assertEqual(result, [])
    
  def testRecordDeleteException(self):
    try:
      self.assertRaises(ImproperUsageError, self.Record("stocks").delete())
    except ImproperUsageError:
      pass
    
    
  def testRecordDisable(self):  
    self.Record("stocks").id(1).disable()
    result1 =  self.Record("stocks_disabled").all().result
    result2 = self.Record("stocks").all().result
    self.assertNotEqual(result1, [])
    self.assertEqual(result2, [])
    