//jshint -W033
//jshint -W018
//jshint  esversion: 11

const {join} = require('path')
const pull = require('pull-stream')
const {DateTime} = require('luxon')

const OffsetLog = require('flumelog-offset')
const offsetCodecs = require('flumelog-offset/frame/offset-codecs')
const codec = require('flumecodec')
const Flume = require('flumedb')

const update = require('.')

const test = require('tape')

test('update', t=>{
  const db = Flume(OffsetLog(join(__dirname, 'flume.log'), {
    codec: codec.json,
    offsetCodec: offsetCodecs[48]
  }))

  function now() {
    return DateTime.fromISO('2014-06-07T09:00:00', {zone: 'Europe/Berlin'})
  }

  const conf = {
    now,
    update_interval: {seconds: 2},
    startday: now().toISODate(),
    minage: {seconds: 10},
    maxSpanPerReq: {months: 1},
    minSpanPerReq: {minutes: 1},
  }

  update(conf, db, stream, err=>{
    if (err && err.message == 'end') return t.end()
    t.fail(err)
  })

  function stream(start, end) {
    t.equal(start.toISO(), '2014-06-07T00:00:00.000+02:00')
    t.equal(end.toISO(), '2014-06-07T08:59:50.000+02:00')
    return pull.error(new Error('end'))
  }

})
