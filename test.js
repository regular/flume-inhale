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

  const conf = {
    update_interval: {seconds: 2},
    startday: DateTime.now().toISODate(),
    minage: {seconds: 10},
    maxSpanPerReq: {months: 1},
    minSpanPerReq: {days: 1},
  }

  update(conf, db, stream, err=>{
    if (err && err.message == 'end') return t.end()
    t.fail(err)
  })

  function stream(start, end) {
    console.log(start.toISODate(), end.toISODate())
    return pull.error(new Error('end'))
    t.end()
  }

})
