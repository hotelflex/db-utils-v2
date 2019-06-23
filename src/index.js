const pg = require('pg')
const knex = require('knex')
const moment = require('moment')
const Id = require('@hotelflex/id')
const { transaction } = require('objection')
const Errors = require('./Errors')

pg.types.setTypeParser(20, 'text', parseInt)
pg.types.setTypeParser(1700, 'text', parseFloat)

module.exports.Errors = Errors

module.exports.createOp = (opts={}) => {
  const doc = {}
  const now = moment.utc().format('YYYY-MM-DDTHH:mm:ss')
  const operationId = opts.operationId || Id.create()

  doc.id = operationId
  doc.timestamp = now
  doc.committed = !opts.messages
  doc.retrySafe = true

  if(opts.messages) {
    const transactionId = opts.transactionId || Id.create()
    doc.messages = JSON.stringify(
      opts.messages.map((m,i) => ({
        id: Id.create(),
        topic: m.topic,
        body: m.body,
        timestamp: now,
        transactionId,
        operationId: Id.create(operationId + i),
      }))
    )
  }
  return doc
}

module.exports.safeUpdate = (knex, table, doc, data) => {
  return transaction(knex, async (trx) => {
    const _doc = await trx(table)
      .query()
      .where('id', doc.id)
      .first('updatedAt')

    if(_doc.updatedAt > doc.updatedAt) 
      throw new Errors.WriteFailure('Update conflict.')
    
    data.updatedAt = moment
      .utc()
      .format('YYYY-MM-DDTHH:mm:ss')
    
    return trx(table)
      .query()
      .where('id', doc.id)
      .update(data)
  })
}

module.exports.createDbPool = (postgres, { min, max } = {}, connOpts = {}) => {
  return knex({
    client: 'pg',
    connection: Object.assign({}, postgres, connOpts),
    pool: { min, max },
  })
}

module.exports.createDbConn = (postgres) => {
  const client = new pg.Client(postgres)
  client.connect()
  return client
  return knex({
    client: 'pg',
    connection: postgres,
  })
}

