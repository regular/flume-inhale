//jshint -W033
//jshint -W018
//jshint  esversion: 11

const pull = require('pull-stream')
const pullLooper = require('pull-looper')
const bufferUntil = require('pull-buffer-until')

const { DateTime } = require('luxon')
const { inspect } = require('util')
const { join } = require('path')

const Reduce = require('flumeview-reduce')

const defaults = {
  //stream_delay: {seconds: 1},
  update_interval: {minutes: 15},
  startday: '2022-01-01',
  minage: {days: 1},
  maxSpanPerReq: {months: 1},
  minSpanPerReq: {days: 1},
  tz: 'Europe/Berlin'
}

module.exports = function(conf, db, stream, cb) {
  conf = Object.assign({}, defaults, conf)
  const debug = require('debug')(conf.debug_id || 'flume-inhale')
  const now = conf.now || DateTime.now
  //if (conf.data_dir) return cb(new Error('Need data_dir'))

  db.use('continuation', Reduce(2, {
    reduce: (acc, item) => {
      if (item.type !== '__since') return acc
      return item.data.timestamp >= acc.timestamp ? item.data : acc
    },
    initial: {timestamp: 0}
  }))
   
  db.continuation.get((err, continuation) => {
    if (err) {
      console.error('Unable to read continuation value:', err.message)
      return cb(err)
    }
    if (continuation && continuation.timestamp) {
      debug('continuation timestamp is', formatTimestamp(continuation.timestamp))
    } else {
      debug('Starting from scratch')
    }
    conf.continuation = continuation
    
    function periodic() {
      update(db, conf, (err, continuation)=>{
        conf.continuation = continuation
        if (err) {
          console.error('-- STREAM ABORT')
          console.error(inspect(err, {depth: 6, colors: true}))
          return cb(err)
        } else {
          //console.error('flumedb is in sync with upstream source.')
        }
        const ms = DateTime.fromSeconds(0).plus(conf.update_interval).toMillis()
        setTimeout(periodic, ms)
      })
    }

    periodic()
  })

  function update(db, conf, cb) {
    let {continuation} = conf
    continuation = continuation || {}

    // timestamps of items requested must be greater or equal request_from
    debug('conf.continuation.timestamp: %d (%s)', continuation.timestamp, formatTimestamp(continuation.timestamp))
    const dt_request_from = continuation.timestamp ?
      DateTime.fromSeconds(continuation.timestamp / 1000).setZone(conf.tz) :    
      DateTime.fromISO(conf.startday, {zone: conf.tz}).startOf('day')
    debug('dt_request_from=%s', dt_request_from.toISO())

    // the highest timestamp we might request without vialating minimum age requirement
    // if minage is zero, we can safely request items up to the current time
    const dt_max_safe = now().setZone(conf.tz).minus(conf.minage)
    debug('minage %O limits us to before %s', conf.minage, dt_max_safe.toISO())
    
    const dt_max_span_request_end = dt_request_from.plus(conf.maxSpanPerReq)
    debug('maxSpanPerReq %O limits us to before %s', conf.maxSpanPerReq, dt_max_span_request_end.toISO())
  
    // timestamps of items requested must be lesser than request_to
    const dt_request_to = DateTime.min(dt_max_safe, dt_max_span_request_end)
    debug('dt_request_to=%s', dt_request_to.toISO())

    const dt_min_span_request_end = dt_request_from.plus(conf.minSpanPerReq)
    debug('minSpanPerReq %O limits us to before %s', conf.minSpanPerReq, dt_min_span_request_end.toISO())

    const wait = dt_min_span_request_end.toSeconds() - dt_request_to.toSeconds()
    if (wait>0) {
      debug('we need to wait %d seconds in order to make the smallest allowed request of %O', wait, conf.minSpanPerReq)
      console.error(`Will continue ${now().plus({seconds: wait}).toRelative()}`)
      setTimeout( ()=>{
        cb(null, continuation)
      }, wait * 1000)
      return 
    }   

    pull(
      stream(dt_request_from, dt_request_to, continuation),
      pullLooper,
      bufferUntil( buff=>{
        return buff[buff.length-1].type == '__since'
      }),
      pull.asyncMap( (items, cb) => {
        //console.log(items)
        db.append(items, (err, seq) => {
          if (err) return cb(err)
          db.continuation.get((err, value) => {
            if (err) return cb(err)
            debug('written %d records, new continuation timepstamp: %s', items.length, formatTimestamp(value.timestamp))
            continuation = value
            cb(null, seq)
          })
        })
      }),
      pull.onEnd(err=>{
        cb(err, err ? undefined : continuation)
      })
    )
  }

  function formatTimestamp(ts) {
    const t = DateTime.fromSeconds(ts/1000).setZone(conf.tz)
    return t.setLocale('de').toLocaleString(DateTime.DATETIME_MED)
  }
}

